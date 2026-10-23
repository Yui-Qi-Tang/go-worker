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
	"time"
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
	// owner is the master whose queues a pool worker serves; nil if standalone.
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

	quit chan any

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
	w := &Worker{
		name: uuid.New().String(),
		quit: make(chan any), task: make(chan *taskRequest),
		done: make(chan struct{}),
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

// newPoolWorker creates a worker that serves m's queues. Master.Stop cancels
// its task context through cancelTask.
func newPoolWorker(m *Master) (*Worker, error) {
	w := &Worker{
		name: uuid.New().String(), owner: m,
		quit: make(chan any), task: make(chan *taskRequest),
		done:   make(chan struct{}),
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
	if w.owner != nil {
		w.servePool(ctx, &st)
		return
	}
	for {
		select {
		case <-w.quit:
			w.logger.Info(workerEventQuit, "worker", w.name)
			return
		case request := <-w.task:
			if w.isStopped() {
				request.reply(Result{Err: ErrWorkerStopped})
				return
			}
			w.execute(ctx, &st, request)
		}
	}
}

// servePool is a pool worker's loop. The worker decides what to do with each
// request from the master's state: it runs tasks while the master accepts
// work, keeps running queued tasks while the master drains, and answers them
// with ErrMasterStopped once the master aborts.
func (w *Worker) servePool(ctx context.Context, st *runState) {
	m := w.owner
	// Only a scaling pool tracks idle workers and retires them.
	var idle *time.Timer
	var idleC <-chan time.Time
	if m.scales() {
		idle = time.NewTimer(m.idleTimeout)
		defer idle.Stop()
		idleC = idle.C
	}
	setIdle := func(delta int32) {
		if idle != nil {
			m.idle.Add(delta)
		}
	}
	for {
		setIdle(1)
		var request *taskRequest
		select {
		case request = <-m.direct:
		case r, ok := <-m.queue:
			if !ok {
				setIdle(-1)
				return
			}
			request = r
		case <-m.acceptDone:
			setIdle(-1)
			// Draining: nothing new arrives on direct, and the queue is
			// closed once every producer has finished.
			if m.queue == nil {
				return
			}
			for request := range m.queue {
				w.serve(ctx, st, request)
			}
			return
		case <-idleC:
			setIdle(-1)
			if m.retire(w) {
				return
			}
			idle.Reset(m.idleTimeout)
			continue
		}
		setIdle(-1)
		if idle != nil && len(m.queue) > 0 {
			// A backlog remains: add a worker even if no new task arrives.
			// grow's error reports a pool with no workers, which cannot be
			// true while this worker runs, so it is ignored.
			_ = m.grow(len(m.queue))
		}
		w.serve(ctx, st, request)
		if idle != nil {
			idle.Reset(m.idleTimeout)
		}
	}
}

func (w *Worker) serve(ctx context.Context, st *runState, request *taskRequest) {
	if w.owner.aborted.Load() {
		request.reply(Result{Err: ErrMasterStopped})
		return
	}
	w.execute(ctx, st, request)
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
	w.cancelTask()
}

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

func (w *Worker) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stopped
}

func (w *Worker) isInitialized() bool {
	return w != nil && w.task != nil && w.quit != nil && w.logger != nil && w.done != nil
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
