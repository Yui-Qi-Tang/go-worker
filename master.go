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
}

var (
	// ErrMasterSetupWithTooLargePoolSize is denoted too large pool size
	ErrMasterSetupWithTooLargePoolSize error = errors.New("exceed max pool size " + strconv.FormatUint(uint64(maxPoolSize), 10))
	// ErrMasterAddNilWorker is an error that denotes Add nil to Master
	ErrMasterAddNilWorker error = errors.New("worker can not be nil")
	// ErrMasterWorkerPoolIsFull is denote the pool size of Master is full
	ErrMasterWorkerPoolIsFull error = errors.New("pool of Master is full")
	// ErrMasterWorkerPoolIsEmpty denotes the pool is empty
	ErrMasterWorkerPoolIsEmpty error = errors.New("pool is empty")
)

const maxPoolSize uint = 1 << 8

// MasterOption is an option function form for master
type MasterOption func(m *Master)

// WithWorkerRecovery starts a routine for killing panic worker & re-create a new worker
func WithWorkerRecovery(enable bool) MasterOption {
	return func(m *Master) {
		if enable {
			m.workerPanic = make(chan string)
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

	if uint(len(m.Pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	// attach worker to master recovery chan
	if worker.Recovery != nil && m.workerPanic != nil {
		worker.Recovery = m.workerPanic
	}

	m.Pool = append(m.Pool, worker)

	m.queueWorker(worker)

	return nil
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
	for i := 0; i < counts; i++ {
		w, err := NewWorker(WithRecovery(true))
		if err != nil {
			return err
		}

		if err := m.AddWorker(w); err != nil {
			return err
		}
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

	for {
		select {
		case worker := <-m.WorkerQueue: // pick a worker from queue
			err := worker.Do(task)

			if err != ErrWorkerPanic && err != ErrWorkerStopped { // let worker back if the worker is still available
				m.queueWorker(worker)
			}
			// drop the task when the worker panics or stops.
			return err
		case <-m.Quit:
			return nil
		}
	}
}

// Stop stops master
// TODO: use context to close the workers under master
func (m *Master) Stop() {
	m.stopOnce.Do(func() {
		m.Lock()
		defer m.Unlock()
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

// GetWorkers returns number of workers
func (m *Master) GetWorkers() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.Pool)
}

// GetPoolSize returns number of workers
func (m *Master) GetPoolSize() int {
	m.RLock()
	defer m.RUnlock()
	return cap(m.Pool)
}

// WakeAllWorkersUp weaks all of workers in the pool up
func (m *Master) WakeAllWorkersUp() error {

	m.Lock()
	defer m.Unlock()

	if len(m.Pool) == 0 {
		return ErrMasterWorkerPoolIsEmpty
	}

	for _, w := range m.Pool {
		w.Start()
	}

	return nil
}

// RecoveryWorker re-creates a new worker when receives worker panic
func (m *Master) RecoveryWorker() {
	for {
		select {
		case name := <-m.workerPanic:
			m.Lock()
			found := false
			for i, worker := range m.Pool {
				if worker.Name == name {
					m.Pool = append(m.Pool[:i], m.Pool[i+1:]...) // delete painc routine from pool
					found = true
					break
				}
			}
			m.Unlock()

			if !found {
				continue
			}

			worker, err := NewWorker(WithRecovery(true))
			if err != nil {
				continue
			}

			worker.Start()
			if err := m.AddWorker(worker); err != nil {
				worker.Stop()
			}
		case <-m.stopRecoveryRoutine:
			return
		}
	}
}
