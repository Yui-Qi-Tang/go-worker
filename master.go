package worker

import (
	"errors"
	"strconv"
	"sync"
)

// Master manages worker
type Master struct {
	sync.RWMutex
	Pool []*Worker // memo: save worker here, can we re-run the stopped worker?

	workerPanic         chan string
	stopRecoveryRoutine chan interface{}

	WorkerQueue chan *Worker
	Quit        chan bool

	stopOnce sync.Once
	stopped  bool

	workerAdded bool
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
	// ErrMasterDuplicateWorkerName denotes the master already has a worker with the same name.
	ErrMasterDuplicateWorkerName error = errors.New("worker name already exists")
)

const maxPoolSize uint = 1 << 8

// MasterOption is an option function form for master
type MasterOption func(m *Master)

// WithWorkerRecovery starts a routine for killing panic worker & re-create a new worker
func WithWorkerRecovery(enable bool) MasterOption {
	return func(m *Master) {
		if enable {
			m.workerPanic = make(chan string, 1)
			m.stopRecoveryRoutine = make(chan interface{})

			go m.RecoveryWorker()
		}
	}
}

// WithConcurrency allows number of workers take task simultaneously
// func WithConcurrency(concurrency int) MasterOption {
// 	return func(m *Master) {
// 		m.WorkerQueue = make(chan *Worker, concurrency)
// 	}
// }

// NewMaster returns 'Master' instance
func NewMaster(opts ...MasterOption) (*Master, error) {
	master := &Master{
		Quit:        make(chan bool),
		WorkerQueue: make(chan *Worker), // defaut: unbuffered chan
		Pool:        make([]*Worker, 0),
	}

	for _, opt := range opts {
		opt(master)
	}

	return master, nil
}

// AddWorker adds worker to pool
func (m *Master) AddWorker(worker *Worker) error {
	if worker == nil {
		return ErrMasterAddNilWorker
	}

	m.Lock()
	defer m.Unlock()

	if m.stopped {
		return ErrMasterStopped
	}

	if worker.isStopped() {
		return ErrWorkerStopped
	}

	if worker.Name == "" {
		return ErrWorkerInvalidName
	}

	if !worker.isInitialized() {
		return ErrWorkerNotInitialized
	}

	if m.hasWorkerIdentityLocked(worker.identity()) {
		return ErrMasterDuplicateWorkerName
	}

	if uint(len(m.Pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	m.addWorkerLocked(worker)

	return nil
}

func (m *Master) hasWorkerIdentityLocked(id string) bool {
	for _, worker := range m.Pool {
		if worker.identity() == id {
			return true
		}
	}
	return false
}

func (m *Master) addWorkerLocked(worker *Worker) {
	// Master owns recovery signaling after a worker enters the pool.
	worker.Recovery = m.workerPanic

	m.Pool = append(m.Pool, worker)
	m.workerAdded = true

	m.queueWorker(worker)
}

func (m *Master) queueWorker(worker *Worker) {
	go func() {
		select {
		case m.WorkerQueue <- worker:
		case <-m.Quit:
		}
	}()
}

// AddWorkers creates number of workers with counts; HINT: the workers support recovery
func (m *Master) AddWorkers(counts int) error {
	if counts < 0 {
		return ErrMasterSetupWithInvalidWorkerCount
	}
	if uint(counts) > maxPoolSize {
		return ErrMasterSetupWithTooLargePoolSize
	}

	m.RLock()
	if m.stopped {
		m.RUnlock()
		return ErrMasterStopped
	}
	if counts == 0 {
		m.RUnlock()
		return nil
	}
	if uint(len(m.Pool)+counts) > maxPoolSize {
		m.RUnlock()
		return ErrMasterWorkerPoolIsFull
	}
	m.RUnlock()

	workers := make([]*Worker, 0, counts)
	for i := 0; i < counts; i++ {
		w, err := NewWorker(WithRecovery(m.workerPanic != nil))
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}

	m.Lock()
	defer m.Unlock()

	if m.stopped {
		return ErrMasterStopped
	}

	if uint(len(m.Pool)+len(workers)) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	for _, w := range workers {
		m.addWorkerLocked(w)
	}

	return nil
}

// Dispatch dispatches task to worker
func (m *Master) Dispatch(task Task) error {
	// add rate limit on task?
	return m.Schedule(task)
}

// Schedule schedules task to worker
func (m *Master) Schedule(task Task) error {
	if isNilTask(task) {
		return ErrWorkerNilTask
	}

	for {
		if err := m.scheduleReadyError(); err != nil {
			return err
		}

		select {
		case worker := <-m.WorkerQueue: // pick a worker from queue
			if m.isStopped() {
				return ErrMasterStopped
			}
			if !m.hasWorker(worker) {
				continue
			}

			err := worker.Do(task)
			if err == ErrWorkerStopped && m.isStopped() {
				return ErrMasterStopped
			}

			if err == ErrWorkerPanic && m.workerPanic != nil {
				m.recoverWorker(worker.identity())
			}
			if err == ErrWorkerStopped || (err == ErrWorkerPanic && m.workerPanic == nil) {
				m.removeWorker(worker.identity(), false)
			}

			if err != ErrWorkerPanic && err != ErrWorkerStopped { // let worker back if the worker is still available
				m.queueWorker(worker)
			}
			// drop the task when the worker panics or stops.
			return err
		case <-m.Quit:
			return ErrMasterStopped
		}
	}
}

// Stop stops master
// TODO: use context to close the workers under master
func (m *Master) Stop() {
	m.stopOnce.Do(func() {
		m.Lock()
		defer m.Unlock()
		m.stopped = true
		for _, w := range m.Pool {
			w.Stop()
		}
		m.stopWorkerRecovery()
		close(m.Quit)
	})
}

func (m *Master) stopWorkerRecovery() {
	if m.stopRecoveryRoutine != nil {
		close(m.stopRecoveryRoutine)
	}
}

func (m *Master) hasWorker(worker *Worker) bool {
	if worker == nil {
		return false
	}

	m.RLock()
	defer m.RUnlock()
	for _, poolWorker := range m.Pool {
		if poolWorker == worker {
			return true
		}
	}
	return false
}

func (m *Master) isStopped() bool {
	m.RLock()
	defer m.RUnlock()
	return m.stopped
}

func (m *Master) scheduleReadyError() error {
	m.RLock()
	defer m.RUnlock()

	if m.stopped {
		return ErrMasterStopped
	}
	if len(m.Pool) == 0 && !m.workerAdded {
		return ErrMasterWorkerPoolIsEmpty
	}
	return nil
}

func (m *Master) removeWorker(id string, expectRecovery bool) bool {
	m.Lock()
	defer m.Unlock()

	for i, worker := range m.Pool {
		if worker.identity() == id {
			m.Pool = append(m.Pool[:i], m.Pool[i+1:]...)
			if len(m.Pool) == 0 && !expectRecovery {
				m.workerAdded = false
			}
			return true
		}
	}

	return false
}

func (m *Master) recoverWorker(id string) bool {
	m.Lock()
	defer m.Unlock()

	for i, oldWorker := range m.Pool {
		if oldWorker.identity() == id {
			m.Pool = append(m.Pool[:i], m.Pool[i+1:]...)

			if m.stopped {
				m.markWorkerPoolEmptyLocked()
				return true
			}

			worker, err := NewWorker(WithRecovery(true))
			if err != nil {
				m.markWorkerPoolEmptyLocked()
				return true
			}

			if err := worker.Start(); err != nil {
				m.markWorkerPoolEmptyLocked()
				return true
			}

			if uint(len(m.Pool)+1) > maxPoolSize {
				worker.Stop()
				m.markWorkerPoolEmptyLocked()
				return true
			}

			m.addWorkerLocked(worker)
			return true
		}
	}

	return false
}

func (m *Master) markWorkerPoolEmptyLocked() {
	if len(m.Pool) == 0 {
		m.workerAdded = false
	}
}

// GetWorkers returns number of workers
func (m *Master) GetWorkers() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.Pool)
}

// GetPoolSize returns number of workers in the pool.
func (m *Master) GetPoolSize() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.Pool)
}

// WakeAllWorkersUp weaks all of workers in the pool up
func (m *Master) WakeAllWorkersUp() error {

	m.Lock()
	defer m.Unlock()

	if m.stopped {
		return ErrMasterStopped
	}

	if len(m.Pool) == 0 {
		return ErrMasterWorkerPoolIsEmpty
	}

	var wakeErr error
	for i := 0; i < len(m.Pool); {
		w := m.Pool[i]
		if err := w.Start(); err != nil && err != ErrWorkerAlreadyStarted {
			if err == ErrWorkerStopped {
				m.Pool = append(m.Pool[:i], m.Pool[i+1:]...)
				if len(m.Pool) == 0 {
					m.workerAdded = false
				}
				wakeErr = err
				continue
			}
			return err
		}
		i++
	}

	return wakeErr
}

// RecoveryWorker re-creates a new worker when receives worker panic
func (m *Master) RecoveryWorker() {
	for {
		select {
		case id := <-m.workerPanic:
			m.recoverWorker(id)
		case <-m.stopRecoveryRoutine:
			return
		}
	}
}
