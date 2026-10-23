package worker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Master schedules tasks onto a pool of workers and optionally replaces
// workers that panic.
type Master struct {
	mu   sync.RWMutex
	pool []*Worker

	workerPanic         chan string
	stopRecoveryRoutine chan any

	workerQueue chan *Worker
	quit        chan bool

	stopOnce sync.Once
	stopped  bool

	workerAdded       bool
	closing           bool
	acceptDone        chan struct{}
	done              chan struct{}
	tasksWG           sync.WaitGroup
	backgroundWG      sync.WaitGroup
	metrics           taskMetrics
	logger            *zap.Logger
	loggerConfigured  bool
	queueCapacity     int
	pending           []*taskRequest
	queueChanged      chan struct{}
	queueWatched      bool
	poolChanged       chan struct{}
	poolErr           error
	recoveryPolicy    RecoveryPolicy
	recoveryStats     RecoveryStats
	newRecoveryWorker func(...Option) (*Worker, error)
}

var (
	// ErrMasterSetupWithTooLargePoolSize is denoted too large pool size
	ErrMasterSetupWithTooLargePoolSize error = errors.New("exceed max pool size " + strconv.FormatUint(uint64(maxPoolSize), 10))
	// ErrMasterSetupWithInvalidWorkerCount denotes an invalid worker count.
	ErrMasterSetupWithInvalidWorkerCount error = errors.New("worker count can not be negative")
	// ErrMasterAddNilWorker is an error that denotes Add nil to Master
	ErrMasterAddNilWorker error = errors.New("worker can not be nil")
	// ErrMasterWorkerPoolIsFull is denote the pool size of Master is full
	ErrMasterWorkerPoolIsFull error = errors.New("pool of Master is full")
	// ErrMasterWorkerPoolIsEmpty denotes the pool is empty
	ErrMasterWorkerPoolIsEmpty error = errors.New("pool is empty")
	// ErrMasterStopped denotes the master has already stopped accepting tasks.
	ErrMasterStopped error = errors.New("master is stopped")
	// ErrMasterNotInitialized denotes a master was not created with package invariants.
	ErrMasterNotInitialized error = errors.New("master is not initialized")
	// ErrMasterDuplicateWorkerName denotes the master already has a worker with the same name.
	ErrMasterDuplicateWorkerName error = errors.New("worker name already exists")
)

const maxPoolSize uint = 1 << 8

// MasterOption is an option function form for master
type MasterOption func(m *Master)

// WithWorkerRecovery enables replacement of workers after a panic, subject to
// RecoveryPolicy (5 attempts per slot per minute by default). Recovery starts
// only after all constructor options have been applied.
func WithWorkerRecovery(enable bool) MasterOption {
	return func(m *Master) {
		if enable {
			m.workerPanic = make(chan string, maxPoolSize)
			m.stopRecoveryRoutine = make(chan any)
		} else {
			m.workerPanic = nil
			m.stopRecoveryRoutine = nil
		}
	}
}

// WithMasterLogger sets the logger for workers created by AddWorkers.
// Manually registered workers retain their logger. The caller owns final Sync.
func WithMasterLogger(logger *zap.Logger) MasterOption {
	return func(m *Master) { m.logger = logger; m.loggerConfigured = true }
}

// NewMaster returns an initialized master. Workers must be added and started
// before submitting tasks. The asynchronous queue is disabled by default.
func NewMaster(opts ...MasterOption) (*Master, error) {
	m := &Master{
		quit: make(chan bool), workerQueue: make(chan *Worker, maxPoolSize), pool: make([]*Worker, 0),
		acceptDone: make(chan struct{}), done: make(chan struct{}), queueChanged: make(chan struct{}), poolChanged: make(chan struct{}),
		recoveryPolicy:    RecoveryPolicy{MaxRestarts: 5, Window: time.Minute},
		newRecoveryWorker: NewWorker,
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.queueCapacity < 0 {
		return nil, ErrInvalidQueueCapacity
	}
	if m.loggerConfigured && m.logger == nil {
		return nil, ErrInvalidLogger
	}
	if m.recoveryPolicy.MaxRestarts <= 0 || m.recoveryPolicy.Window <= 0 {
		return nil, ErrInvalidRecoveryPolicy
	}
	if m.isRecoveryInitialized() {
		m.backgroundWG.Go(m.recoveryLoop)
	}
	if m.queueCapacity > 0 {
		m.backgroundWG.Go(m.runQueue)
	}
	return m, nil
}

func (m *Master) queueWorker(worker *Worker) {
	if !m.isInitialized() {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueWorkerLocked(worker)
}

func (m *Master) queueWorkerLocked(worker *Worker) {
	if m.stopped {
		return
	}
	// The buffer normally holds one token per pool slot, so a release queues
	// without a goroutine. Stale tokens of removed workers can still fill it;
	// then wait in the background rather than block while m is locked.
	select {
	case m.workerQueue <- worker:
		return
	default:
	}
	m.backgroundWG.Go(func() {
		select {
		case m.workerQueue <- worker:
		case <-m.quit:
		}
	})
}

// Schedule waits for a worker and then for the task's outcome.
func (m *Master) Schedule(task Task) error { return m.ScheduleContext(context.Background(), task) }

// ScheduleContext cancels waiting for a worker or a task handoff. Once handed
// off, the task runs to completion even if ctx expires. If handoff and
// cancellation race, either can win. A cancellation error means no handoff.
func (m *Master) ScheduleContext(ctx context.Context, task Task) error {
	return m.ScheduleResult(ctx, task).Err
}

// ScheduleResult preserves the task's original failure and panic information.
func (m *Master) ScheduleResult(ctx context.Context, task Task) Result {
	if isNilTask(task) {
		return Result{Err: ErrWorkerNilTask}
	}
	if !m.isInitialized() {
		return Result{Err: ErrMasterNotInitialized}
	}
	m.mu.Lock()
	if err := m.acceptReadyErrorLocked(); err != nil {
		m.mu.Unlock()
		return Result{Err: err}
	}
	m.tasksWG.Add(1)
	m.mu.Unlock()
	defer m.tasksWG.Done()
	request := newTaskRequest(task)
	w, err := m.dispatch(ctx, request, m.acceptDone)
	if err != nil {
		return Result{Err: err}
	}
	return m.releaseResult(w, <-request.result)
}

// dispatch returns only after a handoff, without waiting for execution. The
// async dispatcher uses a nil interrupt so graceful shutdown can drain its
// already accepted tasks. Quit still aborts pending handoffs on Stop.
func (m *Master) dispatch(ctx context.Context, request *taskRequest, interrupt <-chan struct{}) (*Worker, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-interrupt:
			return nil, ErrMasterStopped
		default:
		}
		changed, err := m.dispatchState()
		if err != nil {
			return nil, err
		}
		select {
		case <-changed:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-interrupt:
			return nil, ErrMasterStopped
		case <-m.quit:
			return nil, ErrMasterStopped
		case w := <-m.workerQueue:
			if m.isStopped() {
				return nil, ErrMasterStopped
			}
			if !m.hasWorker(w) {
				continue
			}
			err := w.prepareTask(ctx, request, interrupt)
			if err == nil {
				return w, nil
			}
			m.releaseWorker(w, err)
			if err == ErrWorkerStopped && m.isStopped() {
				err = ErrMasterStopped
			}
			if err == ErrWorkerStopped && m.workerPanic != nil && w.didPanic() {
				// A queued token can outlive a worker that panicked before
				// handoff. Report the replacement outcome, or try a live slot.
				if _, readyErr := m.dispatchState(); readyErr != nil {
					return nil, readyErr
				}
				continue
			}
			if err == ErrWorkerNotStarted && m.hasStartedWorker() {
				continue
			}
			return nil, err
		}
	}
}

// releaseResult returns w to the pool, or replaces it, after its task ended.
func (m *Master) releaseResult(w *Worker, result Result) Result {
	m.releaseWorker(w, result.Err)
	if result.Err == ErrWorkerStopped && m.isStopped() {
		result.Err = ErrMasterStopped
	}
	return result
}

func (m *Master) releaseWorker(w *Worker, err error) {
	switch {
	case m.workerPanic != nil && (err == ErrWorkerPanic || err == ErrWorkerStopped && w.didPanic()):
		m.recoverWorker(w.recoveryIdentity())
	case err == ErrWorkerStopped || err == ErrWorkerPanic:
		m.removeWorkerReference(w)
	default:
		m.queueWorker(w)
	}
}

func (m *Master) isStopped() bool {
	if !m.isInitialized() {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stopped
}

func (m *Master) acceptReadyErrorLocked() error {
	if m.closing || m.stopped {
		return ErrMasterStopped
	}
	if len(m.pool) == 0 && !m.workerAdded {
		return m.emptyPoolErrorLocked()
	}
	return nil
}

// Capture the pool notification and readiness under one lock, so losing the
// last worker wakes callers already waiting for an available worker.
func (m *Master) dispatchState() (<-chan struct{}, error) {
	if !m.isInitialized() {
		return nil, ErrMasterNotInitialized
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stopped {
		return nil, ErrMasterStopped
	}
	if len(m.pool) == 0 && !m.workerAdded {
		return nil, m.emptyPoolErrorLocked()
	}
	return m.poolChanged, nil
}

func (m *Master) emptyPoolErrorLocked() error {
	if m.poolErr != nil {
		return m.poolErr
	}
	return ErrMasterWorkerPoolIsEmpty
}

func (m *Master) isInitialized() bool {
	return m != nil && m.workerQueue != nil && m.quit != nil && m.acceptDone != nil && m.done != nil
}
