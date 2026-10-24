// Package worker runs tasks with an Init, Run, Done lifecycle on a pool of
// workers in one process. A Worker executes one task at a time; a Master
// schedules synchronous calls and a bounded asynchronous queue onto its
// workers, reports structured results, shuts down gracefully, and can replace
// workers that panic. The durable subpackage persists jobs before dispatch.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"sync"
	"uuid"
)

const (
	// normal events
	workerEventStart    = "starting"
	workerEventDone     = "done"
	workerEventReceived = "received-task"
	workerEventQuit     = "quit"
	// error
	workerErrInit = "error-init"
	workerErrRun  = "error-run"
	workerErrDone = "error-done"
	// panic
	workerPanic = "panic"
)

var (
	// ErrInvalidLogger indicates an explicitly configured nil logger.
	ErrInvalidLogger = errors.New("logger must not be nil")
	// ErrWorkerTaskInit means the task's Init returned an error; Result.Cause holds it.
	ErrWorkerTaskInit = errors.New("task Init phase failed")
	// ErrWorkerTaskRun means the task's Run returned an error; Result.Cause holds it.
	ErrWorkerTaskRun = errors.New("task Run phase failed")
	// ErrWorkerTaskDone means the task's Done returned an error; Result.Cause holds it.
	ErrWorkerTaskDone = errors.New("task Done phase failed")
	// ErrWorkerPanic means a task method panicked or called runtime.Goexit,
	// which stops the worker.
	ErrWorkerPanic = errors.New("task panicked")
	// ErrWorkerNotStarted means the worker has not been started.
	ErrWorkerNotStarted = errors.New("worker is not started")
	// ErrWorkerAlreadyStarted means Start was called on a started worker.
	ErrWorkerAlreadyStarted = errors.New("worker is already started")
	// ErrWorkerStopped means the worker has stopped accepting tasks.
	ErrWorkerStopped = errors.New("worker is stopped")
	// ErrWorkerNilTask denotes the worker was asked to process a nil task.
	ErrWorkerNilTask = errors.New("task is nil")
	// ErrWorkerInvalidName denotes a worker was configured with an empty name.
	ErrWorkerInvalidName = errors.New("worker name cannot be empty")
	// ErrWorkerNotInitialized denotes a worker was not created with package invariants.
	ErrWorkerNotInitialized = errors.New("worker is not initialized")
)

// Worker executes one task at a time. A standalone Worker, created by
// NewWorker, runs tasks given to Do. A Master creates its own workers.
type Worker struct {
	mu   sync.Mutex
	task chan *taskRequest
	name string
	// owner is the master whose queue a pool worker serves; nil if standalone.
	owner *Master

	logger           *slog.Logger
	loggerConfigured bool
	metrics          taskMetrics
	// ownerMetrics is the master's counters; it is set before a pool worker
	// starts and never changes.
	ownerMetrics *taskMetrics
	done         chan struct{}
	finishOnce   sync.Once
	// cancel cancels the context the worker goroutine passes to task phases;
	// canceled records a cancel requested before the goroutine set it.
	cancel   context.CancelFunc
	canceled bool

	// quit closes when a standalone worker stops accepting tasks, so callers
	// waiting to hand one over give up.
	quit chan any

	// A standalone worker goroutine reads tasks from task, a pool worker from
	// its master's readyQueue. ctrl carries at most one drain and one abort
	// message; initial is the state the goroutine starts in.
	ctrl      chan control
	initial   workerState
	drainOnce sync.Once
	abortOnce sync.Once

	started  bool
	stopped  bool
	stopOnce sync.Once
}

// WorkerOption configures a Worker in NewWorker.
type WorkerOption func(w *Worker)

// WithName sets the worker name used in its logs. The name must not be
// empty. By default NewWorker generates a UUID.
func WithName(name string) WorkerOption {
	return func(w *Worker) {
		w.name = name
	}
}

// WithLogger sets the worker's logger. The default is slog.Default(). A nil
// logger is rejected.
func WithLogger(logger *slog.Logger) WorkerOption {
	return func(w *Worker) {
		w.logger = logger
		w.loggerConfigured = true
	}
}

// NewWorker returns an initialized, unstarted worker.
func NewWorker(opts ...WorkerOption) (*Worker, error) {
	task := make(chan *taskRequest)
	w := &Worker{
		name: uuid.New().String(),
		quit: make(chan any), task: task,
		ctrl: make(chan control, 2), done: make(chan struct{}),
	}
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
		w.logger = slog.Default()
	}
	return w, nil
}

// newPoolWorker creates a worker that serves m's queue. Master.Stop cancels
// its task context through cancelTask.
func newPoolWorker(m *Master) (*Worker, error) {
	w := &Worker{
		name: uuid.New().String(), owner: m,
		quit: make(chan any), task: make(chan *taskRequest),
		ctrl: make(chan control, 2), done: make(chan struct{}),
		logger: m.logger,
	}
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

// runState is the task a worker is executing, kept for its panic handler.
type runState struct {
	request *taskRequest
	result  Result
	active  bool
}

func (w *Worker) run() {
	// The task context lives on this goroutine and is passed down; the
	// worker keeps only its cancel function for Stop.
	ctx, cancel := context.WithCancel(context.Background())
	w.mu.Lock()
	w.cancel = cancel
	if w.canceled {
		cancel()
	}
	w.mu.Unlock()

	var st runState
	defer w.finishOnce.Do(func() { close(w.done) })
	defer cancel()
	defer w.recoverRun(&st)

	w.logger.Info(workerEventStart, "worker", w.name)
	w.loop(ctx, &st)
	w.logger.Info(workerEventQuit, "worker", w.name)
}

func (w *Worker) execute(ctx context.Context, st *runState, request *taskRequest) {
	w.metrics.begin()
	if w.ownerMetrics != nil {
		w.ownerMetrics.begin()
	}
	st.request, st.active, st.result = request, true, Result{}
	w.executeTask(ctx, request.task, &st.result)
	w.finishTask(st.result)
	st.active = false
	request.reply(st.result)
	st.request = nil
}

// recoverRun ends the worker. A pool worker first tells its master, which
// may replace it, so a caller that sees ErrWorkerPanic can rely on the
// replacement. After a panic or runtime.Goexit, the active task then gets
// ErrWorkerPanic, before diagnostic logging.
func (w *Worker) recoverRun(st *runState) {
	reason := recover()
	// active also covers runtime.Goexit and legacy panic(nil), which would
	// otherwise abandon a caller waiting for its task result.
	panicked := reason != nil || st.active
	if panicked {
		st.result.Err = ErrWorkerPanic
		st.result.PanicValue = reason
		st.result.PanicStack = string(debug.Stack())
		if cause, ok := reason.(error); ok {
			st.result.Cause = cause
		}
		w.stop()
	}
	w.markStopped()
	if st.active {
		w.finishTask(st.result)
	}
	if w.owner != nil {
		w.owner.workerExited(w, panicked)
	}
	if st.active {
		st.request.reply(st.result)
	}
	if panicked {
		w.logPanic(reason)
	}
}

// Diagnostics must not raise a second panic while the worker is unwinding.
// The original failure is already available through the task result.
func (w *Worker) logPanic(reason any) {
	defer func() { _ = recover() }()
	w.logger.Error(workerPanic, "worker", w.name, "reason", reason)
}

func (w *Worker) finishTask(result Result) {
	w.metrics.finish(result)
	if w.ownerMetrics != nil {
		w.ownerMetrics.finish(result)
	}
}

// Stop stops accepting tasks and cancels the context of a running task,
// without waiting. It is safe to call more than once.
func (w *Worker) Stop() {
	if !w.isInitialized() {
		return
	}
	w.stop()
	w.requestAbort()
	w.cancelTask()
}

// requestDrain asks the worker goroutine to finish queued tasks and exit.
// Each request is sent at most once, so the two-slot ctrl never blocks.
func (w *Worker) requestDrain() { w.drainOnce.Do(func() { w.ctrl <- controlDrain }) }

// requestAbort asks the worker goroutine to exit without running queued
// tasks; a pool's master answers them.
func (w *Worker) requestAbort() { w.abortOnce.Do(func() { w.ctrl <- controlAbort }) }

// cancelTask cancels the context of the running task, or of the next one
// if the worker goroutine has not set its context up yet.
func (w *Worker) cancelTask() {
	w.mu.Lock()
	w.canceled = true
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
		w.requestDrain()
		if !started {
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

func (w *Worker) isInitialized() bool {
	return w != nil && w.task != nil && w.quit != nil && w.ctrl != nil && w.logger != nil && w.done != nil
}

func (w *Worker) readyError() error {
	if !w.isInitialized() {
		return ErrWorkerNotInitialized
	}
	w.mu.Lock()
	defer w.mu.Unlock()
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
