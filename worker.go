// Package worker provides creating worker with consistency task interface for your job.
// And we also provide a master to manage the workers of this package and support worker failed recovery.
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
	workerErrNil  string = "error-nil-task"
	// panic
	workerPanic string = "panic"
)

var (
	// ErrInvalidLogger indicates an explicitly configured nil logger.
	ErrInvalidLogger = errors.New("logger must not be nil")
	// ErrWorkerTaskInit is denoted the worker processes job but get error in Init phase
	ErrWorkerTaskInit error = errors.New("worker got error from executing job in Init phase")
	// ErrWorkerTaskRun is denoted the worker processes job but get error in Run phase
	ErrWorkerTaskRun error = errors.New("worker got error from executing job in Run phase")
	// ErrWorkerTaskDone is denoted the worker process job but get error in Done phase
	ErrWorkerTaskDone error = errors.New("worker got error from executing job in Done phase")
	// ErrWorkerPanic is denoted the worker got panic error from executing job or itself.
	ErrWorkerPanic error = errors.New("worker got panic")
	// ErrWorkerNotStarted is denoted the worker has not started accepting tasks.
	ErrWorkerNotStarted error = errors.New("worker is not started")
	// ErrWorkerAlreadyStarted is denoted the worker has already started accepting tasks.
	ErrWorkerAlreadyStarted error = errors.New("worker is already started")
	// ErrWorkerStopped is denoted the worker has already stopped accepting tasks.
	ErrWorkerStopped error = errors.New("worker is stopped")
	// ErrWorkerNilTask denotes the worker was asked to process a nil task.
	ErrWorkerNilTask error = errors.New("worker task is nil")
	// ErrWorkerInvalidName denotes a worker was configured with an empty name.
	ErrWorkerInvalidName error = errors.New("worker name can not be empty")
	// ErrWorkerNotInitialized denotes a worker was not created with package invariants.
	ErrWorkerNotInitialized error = errors.New("worker is not initialized")
)

// Worker is the structure for worker
type Worker struct {
	sync.Mutex
	Task chan Task
	Name string
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

	// Recovery TODO: use a special type for this channel, it's between master and worker
	Recovery chan string
	Quit     chan any

	status chan string

	started  bool
	stopped  bool
	panicked bool
	stopOnce sync.Once
}

// Option is a functional option for worker setup
type Option func(w *Worker)

// WithName is setup worker name
func WithName(name string) Option {
	return func(w *Worker) {
		w.Name = name
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

// WithRecovery sets recovery chan for upstream
// if no upstream exists, don't create worker with this option
func WithRecovery(ok bool) Option {
	return func(w *Worker) {
		if !ok {
			w.Recovery = nil
			return
		}

		if w.Recovery == nil {
			w.Recovery = make(chan string, 1)
		}
	}
}

// NewWorker returns an initialized, unstarted worker.
func NewWorker(opts ...Option) (*Worker, error) {
	name := guuid.New().String()
	w := &Worker{
		Name: name, recoveryID: name,
		Quit: make(chan any), Task: make(chan Task),
		status: make(chan string, 1), done: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.Name == "" {
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
	w.id = w.Name
	return w, nil
}

// Start waits the work...
// HINT: it's goroutine!
func (w *Worker) Start() error {
	return w.start()
}

func (w *Worker) start() error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.Lock()
	if !w.isInitialized() {
		w.Unlock()
		return ErrWorkerNotInitialized
	}
	if w.stopped {
		w.Unlock()
		return ErrWorkerStopped
	}
	if w.started {
		w.Unlock()
		return ErrWorkerAlreadyStarted
	}
	w.started = true
	w.Unlock()

	go w.run()
	return nil
}

func (w *Worker) run() {
	var activeTask Task
	var result Result
	var owner *taskMetrics
	active := false
	defer w.finishOnce.Do(func() { close(w.done) })
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
			w.Lock()
			w.panicked = true
			w.Unlock()
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

	w.logger.Info(workerEventStart, zap.String("worker", w.Name))
	for {
		select {
		case <-w.Quit:
			w.logger.Info(workerEventQuit, zap.String("worker", w.Name))
			if !w.loggerConfigured {
				_ = w.logger.Sync()
			}
			return
		case task, ok := <-w.Task:
			if !ok {
				w.stop()
				return
			}
			w.Lock()
			if w.stopped {
				w.Unlock()
				w.reportTaskResult(task, Result{Err: ErrWorkerStopped})
				return
			}
			owner = w.ownerMetrics
			w.metrics.begin()
			if owner != nil {
				owner.begin()
			}
			w.Unlock()
			activeTask, active, result = task, true, Result{}
			w.executeTask(task, &result)
			w.finishTask(owner, result)
			active = false
			w.reportTaskResult(task, result)
			activeTask = nil
		}
	}
}

// Diagnostics must not raise a second panic while the worker is unwinding.
// The original failure is already available through the task result.
func (w *Worker) logPanic(reason any) {
	defer func() { _ = recover() }()
	w.logger.Error(workerPanic, zap.String("worker", w.Name), zap.Any("reason", reason))
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

// Stop terminates worker. It is safe to call more than once.
func (w *Worker) Stop() {
	w.stop()
}

func (w *Worker) stop() {
	if !w.isInitialized() {
		return
	}

	w.stopOnce.Do(func() {
		w.Lock()
		w.stopped = true
		started := w.started
		close(w.Quit)
		w.Unlock()
		if !started {
			w.finishOnce.Do(func() { close(w.done) })
		}
	})
}

func (w *Worker) identity() string {
	if w.id == "" {
		return w.Name
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
	w.Lock()
	recovery := w.Recovery
	w.Unlock()
	if recovery == nil {
		return
	}

	defer func() {
		// Recovery is exported, so callers can close it. Treat that as recovery disabled.
		_ = recover()
	}()

	// Panic status must reach Worker.Do even when recovery already has a pending signal.
	select {
	case recovery <- w.recoveryIdentity():
	default:
	}
}

func (w *Worker) isStopped() bool {
	w.Lock()
	defer w.Unlock()
	return w.stopped
}

func (w *Worker) didPanic() bool {
	w.Lock()
	defer w.Unlock()
	return w.panicked
}

func (w *Worker) isStarted() bool {
	w.Lock()
	defer w.Unlock()
	return w.started && !w.stopped
}

func (w *Worker) isInitialized() bool {
	return w != nil && w.Task != nil && w.Quit != nil && w.status != nil && w.logger != nil && w.done != nil
}

func (w *Worker) readyError() error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.Lock()
	defer w.Unlock()

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
	w.Lock()
	w.stopped = true
	w.Unlock()
}

// Wait waits for this worker to exit. It does not initiate shutdown.
func (w *Worker) Wait(ctx context.Context) error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	return waitDone(ctx, w.done)
}

// Shutdown stops accepting tasks and waits for an active task to finish.
// A deadline limits the wait; it cannot interrupt a Task method.
func (w *Worker) Shutdown(ctx context.Context) error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.Stop()
	return w.Wait(ctx)
}
