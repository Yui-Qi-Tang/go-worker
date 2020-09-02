package worker

import (
	"errors"
	"strconv"
	"sync"
)

// Master manages worker
type Master struct {
	sync.RWMutex
	Pool []*Worker

	//workerMsg           chan message
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
	// ErrMasterWorkerPoolIsEmpty denotes the pool is empty
	ErrMasterWorkerPoolIsEmpty error = errors.New("pool is empty")
)

const maxPoolSize uint = 1 << 8

// MasterOption is an option function form for master
type MasterOption func(m *Master)

// WithWorkerRecovery starts a routine for killing panic worker & re-create a new worker
func WithWorkerRecovery(enanle bool) MasterOption {
	return func(m *Master) {
		if enanle {
			//m.workerMsg = make(chan message)

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

// AddWorker adds worker to pool & queue
func (m *Master) AddWorker(worker *Worker) error {
	if worker == nil {
		return ErrMasterAddNilWorker
	}

	if uint(len(m.Pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	// assign worker to queue
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
		w, err := NewWorker()
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
			worker.Task <- task // send task to worker

			if worker.status.load() != taskPanicErr { // let worker back if the worker with no panic
				go func() { m.WorkerQueue <- worker }() // worker goes back to queue
				return
			}
			// drop the worker, becasue the task makes the worker panic
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
		m.Lock()
		for i, worker := range m.Pool {
			if worker.status.load() == taskPanicErr {
				// update pool: remove panic g and add new g
				//m.Lock()
				m.Pool = append(m.Pool[:i], m.Pool[i+1:]...) // delete painc routine from pool
				//m.Unlock()

				m.Stop()

				worker, _ := NewWorker()
				m.AddWorker(worker)
				worker.Start()
			}
		}
		m.Unlock()
		select {

		case <-m.stopRecoveryRoutine:
			return
		}
	}
}

func (m *Master) workerRecovery() {

}
