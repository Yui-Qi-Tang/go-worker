// Package worker runs tasks with an Init, Run, Done lifecycle on a pool of
// workers in one process. A Worker executes one task at a time; a Master
// schedules synchronous calls and a bounded asynchronous queue onto its
// workers, reports structured results, shuts down gracefully, and can replace
// workers that panic. The durable subpackage persists jobs before dispatch.
package worker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	guuid "github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// normal events
	workerEventStart    string = "starting"
	workerEventDone     string = "done"
	workerEventReceived string = "received-task"
	workerEventQuit     string = "quit"
	// error
	workerErrInit string = "error-init"
	workerErrRun  string = "error-run"
	workerErrDone string = "error-done"
	// panic
	workerPanic string = "panic"
)

var (
	// ErrInvalidLogger indicates an explicitly configured nil logger.
	ErrInvalidLogger = errors.New("logger must not be nil")
	// ErrWorkerTaskInit means the task's Init returned an error; Result.Cause holds it.
	ErrWorkerTaskInit error = errors.New("worker got error from executing job in Init phase")
	// ErrWorkerTaskRun means the task's Run returned an error; Result.Cause holds it.
	ErrWorkerTaskRun error = errors.New("worker got error from executing job in Run phase")
	// ErrWorkerTaskDone means the task's Done returned an error; Result.Cause holds it.
	ErrWorkerTaskDone error = errors.New("worker got error from executing job in Done phase")
	// ErrWorkerPanic means a task method panicked or called runtime.Goexit,
	// which stops the worker.
	ErrWorkerPanic error = errors.New("worker got panic")
	// ErrWorkerNotStarted means the worker has not been started.
	ErrWorkerNotStarted error = errors.New("worker is not started")
	// ErrWorkerAlreadyStarted means Start was called on a started worker.
	ErrWorkerAlreadyStarted error = errors.New("worker is already started")
	// ErrWorkerStopped means the worker has stopped accepting tasks.
	ErrWorkerStopped error = errors.New("worker is stopped")
	// ErrWorkerNilTask denotes the worker was asked to process a nil task.
	ErrWorkerNilTask error = errors.New("worker task is nil")
	// ErrWorkerInvalidName denotes a worker was configured with an empty name.
	ErrWorkerInvalidName error = errors.New("worker name can not be empty")
	// ErrWorkerNotInitialized denotes a worker was not created with package invariants.
	ErrWorkerNotInitialized error = errors.New("worker is not initialized")
)

// Worker executes one task at a time. A Worker belongs to at most one Master.
type Worker struct {
	mu   sync.Mutex
	task chan *taskRequest
	name string
	id   string

	recoveryID string
	// recoveryBudget is shared across replacements and guarded by the master.
	recoveryBudget *restartBudget

	logger           *zap.Logger
	loggerConfigured bool
	metrics          taskMetrics
	ownerMetrics     *taskMetrics
	done             chan struct{}
	finishOnce       sync.Once
	// ctx is passed to task phases; cancel aborts running work.
	ctx    context.Context
	cancel context.CancelFunc

	// recovery carries this worker's recovery identity to its master after a panic.
	recovery chan string
	quit     chan any

	started  bool
	stopped  bool
	panicked bool
	stopOnce sync.Once
}

// Option configures a Worker in NewWorker.
type Option func(w *Worker)

// WithName sets the worker name, which is also its identity in a Master.
// The name must not be empty. By default NewWorker generates a UUID.
func WithName(name string) Option {
	return func(w *Worker) {
		w.name = name
	}
}

// WithLogger supplies the worker logger. The caller owns the logger and its
// final Sync; the worker never closes it. A nil logger is rejected.
func WithLogger(logger *zap.Logger) Option {
	return func(w *Worker) {
		w.logger = logger
		w.loggerConfigured = true
	}
}

// withRecovery gives a worker created by a master its recovery channel.
func withRecovery(ok bool) Option {
	return func(w *Worker) {
		if !ok {
			w.recovery = nil
			return
		}

		if w.recovery == nil {
			w.recovery = make(chan string, 1)
		}
	}
}

// NewWorker returns an initialized, unstarted worker.
func NewWorker(opts ...Option) (*Worker, error) {
	name := guuid.New().String()
	w := &Worker{
		name: name, recoveryID: name,
		quit: make(chan any), task: make(chan *taskRequest),
		done: make(chan struct{}),
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(w)
	}
	if w.name == "" {
		return nil, ErrWorkerInvalidName
	}
	if w.loggerConfigured && w.logger == nil {
		return nil, ErrInvalidLogger
	}
	if w.logger == nil {
		config := zap.NewProductionConfig()
		config.Encoding = "console"
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		logger, err := config.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to create logger for worker: %w", err)
		}
		w.logger = logger
	}
	w.id = w.name
	return w, nil
}

// Start launches the worker goroutine and returns immediately. It returns
// ErrWorkerAlreadyStarted if the worker is running and ErrWorkerStopped once
// it has been stopped; a stopped worker cannot be restarted.
func (w *Worker) Start() error {
	return w.start()
}

func (w *Worker) start() error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.mu.Lock()
	if !w.isInitialized() {
		w.mu.Unlock()
		return ErrWorkerNotInitialized
	}
	if w.stopped {
		w.mu.Unlock()
		return ErrWorkerStopped
	}
	if w.started {
		w.mu.Unlock()
		return ErrWorkerAlreadyStarted
	}
	w.started = true
	w.mu.Unlock()

	go w.run()
	return nil
}

func (w *Worker) run() {
	var activeTask *taskRequest
	var result Result
	var owner *taskMetrics
	active := false
	defer w.finishOnce.Do(func() { close(w.done) })
	defer w.cancel()
	defer func() {
		reason := recover()
		// active also handles runtime.Goexit and legacy panic(nil), which would
		// otherwise abandon a caller that is waiting for its task result.
		if reason != nil || active {
			result.Err = ErrWorkerPanic
			result.PanicValue = reason
			result.PanicStack = string(debug.Stack())
			if cause, ok := reason.(error); ok {
				result.Cause = cause
			}
			w.mu.Lock()
			w.panicked = true
			w.mu.Unlock()
			w.stop()
			if active {
				w.finishTask(owner, result)
				w.reportTaskResult(activeTask, result)
			}
			w.notifyRecovery()
			w.logPanic(reason)
		}
		w.markStopped()
	}()

	w.logger.Info(workerEventStart, zap.String("worker", w.name))
	for {
		select {
		case <-w.quit:
			w.logger.Info(workerEventQuit, zap.String("worker", w.name))
			if !w.loggerConfigured {
				_ = w.logger.Sync()
			}
			return
		case request := <-w.task:
			w.mu.Lock()
			if w.stopped {
				w.mu.Unlock()
				w.reportTaskResult(request, Result{Err: ErrWorkerStopped})
				return
			}
			owner = w.ownerMetrics
			w.metrics.begin()
			if owner != nil {
				owner.begin()
			}
			w.mu.Unlock()
			activeTask, active, result = request, true, Result{}
			w.executeTask(request.Task, &result)
			w.finishTask(owner, result)
			active = false
			w.reportTaskResult(request, result)
			activeTask = nil
		}
	}
}

// Diagnostics must not raise a second panic while the worker is unwinding.
// The original failure is already available through the task result.
func (w *Worker) logPanic(reason any) {
	defer func() { _ = recover() }()
	w.logger.Error(workerPanic, zap.String("worker", w.name), zap.Any("reason", reason))
	if !w.loggerConfigured {
		_ = w.logger.Sync()
	}
}

func (w *Worker) finishTask(owner *taskMetrics, result Result) {
	w.metrics.finish(result)
	if owner != nil {
		owner.finish(result)
	}
}

// Stop stops accepting tasks and cancels the context of a running task,
// without waiting. It is safe to call more than once.
func (w *Worker) Stop() {
	if !w.isInitialized() {
		return
	}
	w.stop()
	w.cancel()
}

// stop stops accepting tasks and lets a running task finish.
func (w *Worker) stop() {
	if !w.isInitialized() {
		return
	}

	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.stopped = true
		started := w.started
		close(w.quit)
		w.mu.Unlock()
		if !started {
			w.cancel()
			w.finishOnce.Do(func() { close(w.done) })
		}
	})
}

// Name returns the worker name set by WithName, or a generated UUID.
func (w *Worker) Name() string {
	if w == nil {
		return ""
	}
	return w.name
}

func (w *Worker) identity() string {
	if w.id == "" {
		return w.name
	}
	return w.id
}

func (w *Worker) recoveryIdentity() string {
	if w.recoveryID == "" {
		return w.identity()
	}
	return w.recoveryID
}

func (w *Worker) notifyRecovery() {
	w.mu.Lock()
	recovery := w.recovery
	w.mu.Unlock()
	if recovery == nil {
		return
	}

	// Never block a dying worker. A pending signal is enough: the master also
	// observes each worker's exit.
	select {
	case recovery <- w.recoveryIdentity():
	default:
	}
}

func (w *Worker) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

func (w *Worker) didPanic() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.panicked
}

func (w *Worker) isStarted() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.started && !w.stopped
}

func (w *Worker) isInitialized() bool {
	return w != nil && w.task != nil && w.quit != nil && w.logger != nil && w.done != nil && w.cancel != nil
}

func (w *Worker) readyError() error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	if w.stopped {
		return ErrWorkerStopped
	}
	if !w.started {
		return ErrWorkerNotStarted
	}
	return nil
}

func (w *Worker) markStopped() {
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
}

// Wait waits for this worker to exit. It does not initiate shutdown.
func (w *Worker) Wait(ctx context.Context) error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	return waitDone(ctx, w.done)
}

// Shutdown stops accepting tasks and waits for an active task to finish
// without canceling its context. A deadline limits the wait; call Stop to
// cancel the running task.
func (w *Worker) Shutdown(ctx context.Context) error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.stop()
	return w.Wait(ctx)
}
