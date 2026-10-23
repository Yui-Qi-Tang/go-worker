package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// maxPoolSize bounds both the number of workers and the queue capacity.
const maxPoolSize = 1024

var (
	// ErrInvalidWorkers means WithWorkers was given counts outside
	// 0 <= min <= max, 1 <= max <= 1024.
	ErrInvalidWorkers = fmt.Errorf("workers must satisfy 0 <= min <= max and 1 <= max <= %d", maxPoolSize)
	// ErrMasterStopped means the master no longer accepts tasks, or a queued
	// task was abandoned by Stop.
	ErrMasterStopped = errors.New("master is stopped")
	// ErrMasterNotInitialized means a Master was not created by NewMaster.
	ErrMasterNotInitialized = errors.New("master is not initialized")
	// ErrInvalidIdleTimeout means WithIdleTimeout was given a non-positive duration.
	ErrInvalidIdleTimeout = errors.New("idle timeout must be positive")
)

// Master runs tasks on a pool of workers it creates and supervises. Workers
// take tasks directly from the master's queue; a worker that panics is
// replaced, subject to the RecoveryPolicy.
type Master struct {
	minWorkers, maxWorkers int
	idleTimeout            time.Duration
	queueCapacity          int
	logger                 *slog.Logger
	loggerConfigured       bool
	recoveryPolicy         RecoveryPolicy
	newWorker              func(*Master) (*Worker, error)

	// direct hands synchronous calls to a worker; queue holds submissions.
	direct chan *taskRequest
	queue  chan *taskRequest

	// Producers hold admit's read lock while checking acceptDone and sending;
	// shutdown takes the write lock before it closes the queue.
	admit sync.RWMutex
	// acceptDone is closed when the master stops accepting work; aborted is
	// set when Stop abandons queued work. Workers read both to decide what
	// to do with each task.
	acceptDone chan struct{}
	aborted    atomic.Bool

	// poolDown holds the error returned while no worker can run.
	poolDown atomic.Pointer[poolFailure]
	// live mirrors len(workers); idle counts workers waiting for a task.
	// Both let admission decide on scaling without the mutex.
	live atomic.Int32
	idle atomic.Int32

	mu           sync.Mutex
	workers      map[*Worker]struct{}
	closing      bool
	queueDrained bool
	// lost counts panicked workers not yet replaced. While it is positive,
	// every new worker is a replacement and spends the restart budget.
	lost          int
	budget        restartBudget
	recoveryStats RecoveryStats
	healTimer     *time.Timer
	drainStop     chan struct{}

	running    sync.WaitGroup // worker goroutines
	background sync.WaitGroup // drainers
	metrics    taskMetrics
	done       chan struct{}
}

type poolFailure struct{ err error }

// MasterOption configures a Master in NewMaster.
type MasterOption func(m *Master)

// WithWorkers sets the pool size. NewMaster starts minWorkers. The pool adds a
// worker, up to maxWorkers, when more tasks wait than workers are idle: when a
// task is admitted, or when a worker takes a task and leaves a backlog. A
// worker idle for the idle timeout retires while more than minWorkers run.
// Equal counts give a fixed pool with no scaling cost. The default is a fixed
// pool of runtime.GOMAXPROCS(0) workers, capped at 1024.
func WithWorkers(minWorkers, maxWorkers int) MasterOption {
	return func(m *Master) { m.minWorkers, m.maxWorkers = minWorkers, maxWorkers }
}

// WithIdleTimeout sets how long a worker above the minimum may stay idle
// before it retires. The default is 10 seconds. It has no effect on a fixed
// pool.
func WithIdleTimeout(d time.Duration) MasterOption {
	return func(m *Master) { m.idleTimeout = d }
}

// WithMasterLogger sets the logger shared by the master's workers. The
// default is slog.Default().
func WithMasterLogger(logger *slog.Logger) MasterOption {
	return func(m *Master) { m.logger = logger; m.loggerConfigured = true }
}

// NewMaster validates the options and starts the pool's workers. The
// asynchronous queue is disabled unless WithQueueCapacity is positive.
func NewMaster(opts ...MasterOption) (*Master, error) {
	workers := min(runtime.GOMAXPROCS(0), maxPoolSize)
	m := &Master{
		minWorkers: workers, maxWorkers: workers, idleTimeout: 10 * time.Second,
		recoveryPolicy: RecoveryPolicy{MaxRestarts: 5, Window: time.Minute},
		newWorker:      newPoolWorker,
		direct:         make(chan *taskRequest),
		acceptDone:     make(chan struct{}),
		workers:        make(map[*Worker]struct{}),
		done:           make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.queueCapacity < 0 || m.queueCapacity > maxPoolSize {
		return nil, ErrInvalidQueueCapacity
	}
	if m.minWorkers < 0 || m.maxWorkers < 1 || m.minWorkers > m.maxWorkers || m.maxWorkers > maxPoolSize {
		return nil, ErrInvalidWorkers
	}
	if m.idleTimeout <= 0 {
		return nil, ErrInvalidIdleTimeout
	}
	if m.loggerConfigured && m.logger == nil {
		return nil, ErrInvalidLogger
	}
	if m.recoveryPolicy.MaxRestarts <= 0 || m.recoveryPolicy.Window <= 0 {
		return nil, ErrInvalidRecoveryPolicy
	}
	if m.logger == nil {
		m.logger = slog.Default()
	}
	if m.queueCapacity > 0 {
		m.queue = make(chan *taskRequest, m.queueCapacity)
	}

	m.mu.Lock()
	var err error
	for len(m.workers) < m.minWorkers && err == nil {
		err = m.spawnLocked()
	}
	m.mu.Unlock()
	if err != nil {
		m.Stop()
		return nil, err
	}
	return m, nil
}

// Schedule waits for a worker and then for the task's outcome.
func (m *Master) Schedule(task Task) error { return m.ScheduleContext(context.Background(), task) }

// ScheduleContext cancels waiting for a worker. Once a worker takes the task,
// it runs to completion even if ctx expires. If handoff and cancellation race,
// either can win; a cancellation error means no worker took the task.
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
	request := newTaskRequest(task)
	if err := m.handoff(ctx, request); err != nil {
		return Result{Err: err}
	}
	return <-request.result
}

func (m *Master) handoff(ctx context.Context, request *taskRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.admit.RLock()
	defer m.admit.RUnlock()
	if err := m.admissionError(); err != nil {
		return err
	}
	if err := m.grow(len(m.queue) + 1); err != nil {
		return err
	}
	select {
	case m.direct <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.acceptDone:
		return ErrMasterStopped
	}
}

// admissionError must be called with admit's read lock held.
func (m *Master) admissionError() error {
	select {
	case <-m.acceptDone:
		return ErrMasterStopped
	default:
	}
	if failure := m.poolDown.Load(); failure != nil {
		return failure.err
	}
	return nil
}

// scales reports whether the pool size can change with demand.
func (m *Master) scales() bool { return m.minWorkers < m.maxWorkers }

func (m *Master) isInitialized() bool {
	return m != nil && m.done != nil && m.direct != nil && m.acceptDone != nil
}
