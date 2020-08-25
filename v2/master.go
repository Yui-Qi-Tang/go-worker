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
}

var (
	// ErrMasterSetupWithTooLargePoolSize is denoted too large pool size
	ErrMasterSetupWithTooLargePoolSize error = errors.New("exceed max pool size " + strconv.FormatUint(uint64(maxPoolSize), 10))
	// ErrMasterAddNilWorker is an error that denotes Add nil to Master
	ErrMasterAddNilWorker error = errors.New("worker can not be nil")
	// ErrMasterWorkerPoolIsFull is denote the pool size of Master is full
	ErrMasterWorkerPoolIsFull error = errors.New("pool of Master is full")
)

const maxPoolSize uint = 1 << 8

// MasterOption is an option function form for master
type MasterOption func(m *Master)

// WithWorkerRecovery starts a routine for killing panic worker & re-create a new worker
func WithWorkerRecovery(enanle bool) MasterOption {
	return func(m *Master) {
		if enanle {
			m.workerPanic = make(chan string)
			m.stopRecoveryRoutine = make(chan interface{})

			m.workerPanic = make(chan string)

			go m.RecoveryWorker()
		}
	}
}

// WithConcurrency allows number of workers can
func WithConcurrency(concurrency int) MasterOption {
	return func(m *Master) {
		m.WorkerQueue = make(chan *Worker, concurrency)
	}
}

// NewMaster returns 'Master' instance
func NewMaster(opts ...MasterOption) (*Master, error) {
	master := &Master{
		Quit:        make(chan bool),
		WorkerQueue: make(chan *Worker),
		Pool:        make([]*Worker, 0),
	}

	for _, opt := range opts {
		opt(master)
	}

	return master, nil
}

// AddWorker adds worker to pool and start it
func (m *Master) AddWorker(worker *Worker) error {
	if worker == nil {
		return ErrMasterAddNilWorker
	}

	if uint(len(m.Pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	// attach worker to master recovery chan
	if worker.Recovery != nil && m.workerPanic != nil {
		worker.Recovery = m.workerPanic
	}

	worker.Start()

	go func() {
		m.WorkerQueue <- worker
	}()

	m.Lock()
	m.Pool = append(m.Pool, worker)
	m.Unlock()
	return nil
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
	m.Schedule(task)
	return nil
}

// Schedule schedules task to worker
func (m *Master) Schedule(task Task) {

	for {
		select {
		case worker := <-m.WorkerQueue: // pick a worker from queue
			worker.Task <- task // worker waits for task

			if worker.waitStatus() != workerPanic { // let worker back if the worker with no panic
				go func() { m.WorkerQueue <- worker }()
				return
			}
			return
		case <-m.Quit:
			return
		}
	}
}

// Stop stops master
// TODO: use context to close the workers under master
func (m *Master) Stop() {
	m.Lock()
	defer m.Unlock()
	for _, w := range m.Pool {
		w.Stop()
	}
	m.stopWorkerRecovery()
	close(m.Quit)
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

// RecoveryWorker re-creates a new worker when receives worker panic
func (m *Master) RecoveryWorker() {
	for {
		select {
		case v := <-m.workerPanic:
			for i, worker := range m.Pool {
				if worker.Name == v {

					// update pool: remove panic g and add new g
					m.Lock()
					m.Pool = append(m.Pool[:i], m.Pool[i+1:]...) // delete painc routine from pool
					m.Unlock()

					worker, _ := NewWorker()
					m.AddWorker(worker)
					break
				}
			}
		case <-m.stopRecoveryRoutine:
			return
		}
	}
}
