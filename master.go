package worker

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
)

// the length of pool needs to be read automatically

// Master manages worker
type Master struct {
	sync.RWMutex
	Pool                []*Worker
	jobCount            uint
	workerPanic         chan string
	stopRecvWorkerEvent chan interface{}
	WorkerQueue         chan *Worker
	Quit                chan bool
	Counts              uint64
}

// PoolSize default 100
// You can modify the value if any
var PoolSize uint = 100

const maxUint = ^uint(0)
const maxPoolSize uint = 1 << 32
const maxJobCount uint = maxUint

// NewMaster returns 'Master' instance
func NewMaster() (*Master, error) {
	// limitation of PoolSize is 'maxPoolSize'
	if PoolSize > maxPoolSize {
		return nil, errors.New("exceed max pool size " + strconv.FormatUint(uint64(maxPoolSize), 10))
	}
	master := &Master{
		Pool:                make([]*Worker, 0, PoolSize),
		workerPanic:         make(chan string),
		stopRecvWorkerEvent: make(chan interface{}),
		Quit:                make(chan bool),
		WorkerQueue:         make(chan *Worker),
		Counts:              0,
	}

	go master.RecvWorkerEvent()

	return master, nil
}

// AddWorker adds worker to pool and start it
func (m *Master) AddWorker(worker *Worker) error {
	if worker == nil {
		return errors.New("invalid worker")
	}
	if uint(len(m.Pool)) > PoolSize {
		worker.Stop()
		return errors.New("exceed max pool size")
	}

	worker.Recovery = m.workerPanic
	worker.Start()
	go func() {
		m.WorkerQueue <- worker
	}()

	m.Lock()
	m.Pool = append(m.Pool, worker)
	m.Unlock()
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
		case worker := <-m.WorkerQueue: // pick worker from queue
			worker.Task <- task
			atomic.AddUint64(&m.Counts, 1)
			if worker.Status() != workerPanic { // let worker back if the worker with no panic
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
	close(m.stopRecvWorkerEvent)
	close(m.Quit)
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

// GetJobCounts returns counts of job
func (m *Master) GetJobCounts() uint64 {
	atomic.LoadUint64(&m.Counts)
	return atomic.LoadUint64(&m.Counts)
}

// RecvWorkerEvent receives event from worker
func (m *Master) RecvWorkerEvent() {
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
		case <-m.stopRecvWorkerEvent:
			return
		}
	}
}
