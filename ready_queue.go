package worker

import (
	"sync"
	"sync/atomic"
)

// readyQueue is the FIFO of tasks waiting for a pool worker, shared by
// Schedule and Submit. A worker takes the next task without parking while
// any is waiting, and a producer wakes a parked worker whenever one is idle.
//
// It replaces channel handoffs. A send to a parked worker woke that worker on
// the sender's processor for every task, which kept each worker running in
// turn with one caller and left other runnable callers waiting for
// milliseconds (benchmarks/results/2026-10-24-load.md).
type readyQueue struct {
	mu    sync.Mutex
	items []*taskRequest
	head  int
	// active counts workers that are not parked. parked counts parked
	// workers no wake token is reserved for; parked plus len(wake) is the
	// number of parked workers.
	active, parked int
	wake           chan struct{}
	// scheduleErr rejects synchronous calls once shutdown began; stopErr
	// rejects every task after Stop; downErr rejects every task while the
	// pool has no worker.
	scheduleErr, stopErr, downErr error

	// waiting counts queued tasks for admission's scaling decision, which
	// reads it without mu.
	waiting atomic.Int32
	// slots bounds Submit's waiting tasks; nil when the queue is disabled.
	slots chan struct{}
	// closed closes once no producer can push.
	closed chan struct{}
}

func (q *readyQueue) init(capacity int) {
	q.wake = make(chan struct{}, maxPoolSize)
	q.closed = make(chan struct{})
	if capacity > 0 {
		q.items = make([]*taskRequest, 0, capacity)
		q.slots = make(chan struct{}, capacity)
	}
}

// push queues request, or returns why it is rejected. A Submit request must
// hold a slot; a rejected one gives it back.
func (q *readyQueue) push(request *taskRequest) error {
	q.mu.Lock()
	if err := q.rejectionLocked(request); err != nil {
		q.mu.Unlock()
		q.release(request)
		return err
	}
	request.queued = true
	q.items = append(q.items, request)
	q.waiting.Add(1)
	if q.parked > 0 {
		q.parked--
		q.active++
		q.wake <- struct{}{} // never blocks: wake holds one token per parked worker at most
	}
	q.mu.Unlock()
	return nil
}

func (q *readyQueue) rejectionLocked(request *taskRequest) error {
	switch {
	case q.stopErr != nil:
		return q.stopErr
	case q.downErr != nil:
		return q.downErr
	case request.future == nil:
		return q.scheduleErr
	}
	return nil
}

// take returns the next task and how many remain. When none is waiting it
// counts the calling worker as parked; the worker must then receive from wake
// or call unpark.
func (q *readyQueue) take() (*taskRequest, int) {
	q.mu.Lock()
	request := q.popLocked()
	if request == nil {
		q.active--
		q.parked++
	}
	remaining := int(q.waiting.Load())
	q.mu.Unlock()
	q.release(request)
	return request, remaining
}

// pop returns the next task without parking the caller.
func (q *readyQueue) pop() *taskRequest {
	q.mu.Lock()
	request := q.popLocked()
	q.mu.Unlock()
	q.release(request)
	return request
}

func (q *readyQueue) popLocked() *taskRequest {
	for q.head < len(q.items) {
		request := q.items[q.head]
		q.items[q.head] = nil
		q.head++
		if request.queued {
			request.queued = false
			q.waiting.Add(-1)
			q.compactLocked()
			return request
		}
	}
	q.compactLocked()
	return nil
}

// compactLocked reuses the slice once every item has been taken.
func (q *readyQueue) compactLocked() {
	if q.head == len(q.items) {
		q.items, q.head = q.items[:0], 0
	}
}

// release gives back a Submit request's slot when it leaves the queue.
func (q *readyQueue) release(request *taskRequest) {
	if request != nil && request.future != nil {
		<-q.slots
	}
}

// unpark undoes take for a worker that stopped waiting without a wake token.
// If a token is reserved for every parked worker, this worker consumes one.
func (q *readyQueue) unpark() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.parked > 0 {
		q.parked--
		q.active++
		return
	}
	<-q.wake
}

// addActive records a worker joining (1) or leaving (-1) the pool. A worker
// is active while it runs, so it leaves without being parked.
func (q *readyQueue) addActive(n int) {
	q.mu.Lock()
	q.active += n
	q.mu.Unlock()
}

// cancel removes request if no worker has taken it yet and reports whether
// it did. A canceled request stays in items until a worker skips it.
func (q *readyQueue) cancel(request *taskRequest) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !request.queued {
		return false
	}
	request.queued = false
	q.waiting.Add(-1)
	return true
}

// rejectSchedules answers queued synchronous calls with err and rejects later
// ones. Shutdown rejects calls still waiting for a worker.
func (q *readyQueue) rejectSchedules(err error) {
	q.mu.Lock()
	q.scheduleErr = err
	var rejected []*taskRequest
	for _, request := range q.items[q.head:] {
		if request.queued && request.future == nil {
			request.queued = false
			q.waiting.Add(-1)
			rejected = append(rejected, request)
		}
	}
	q.mu.Unlock()
	for _, request := range rejected {
		request.reply(Result{Err: err})
	}
}

// stop answers every queued task with err and rejects later ones.
func (q *readyQueue) stop(err error) {
	q.mu.Lock()
	q.stopErr = err
	q.mu.Unlock()
	q.drain(err)
}

// markDown rejects tasks with err while the pool has no worker, answering
// those already queued; nil accepts tasks again.
func (q *readyQueue) markDown(err error) {
	q.mu.Lock()
	q.downErr = err
	q.mu.Unlock()
	if err != nil {
		q.drain(err)
	}
}

// drain answers every queued task with err.
func (q *readyQueue) drain(err error) {
	for request := q.pop(); request != nil; request = q.pop() {
		request.reply(Result{Err: err})
	}
}
