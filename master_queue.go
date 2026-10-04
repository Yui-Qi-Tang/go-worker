package worker

import (
	"context"
	"errors"
)

var (
	// ErrQueueDisabled means NewMaster was not configured with a positive capacity.
	ErrQueueDisabled = errors.New("asynchronous task queue is disabled")
	// ErrQueueFull means TrySubmit could not accept a task immediately.
	ErrQueueFull = errors.New("asynchronous task queue is full")
	// ErrInvalidQueueCapacity means a negative queue capacity was configured.
	ErrInvalidQueueCapacity = errors.New("task queue capacity cannot be negative")
)

// WithQueueCapacity bounds pending asynchronous tasks, including the head being
// handed to a worker. Running tasks do not consume queue capacity. Zero disables
// Submit and TrySubmit. Synchronous Schedule calls bypass this task queue.
func WithQueueCapacity(capacity int) MasterOption {
	return func(m *Master) { m.queueCapacity = capacity }
}

type submission struct {
	task   Task
	future *Future
}

// Submit waits for queue space and returns a reusable result handle. ctx only
// controls admission: accepted tasks run independently of it. Use Future.Wait
// with a separate context to limit result waiting. Tasks must not perform a
// blocking Submit to their own saturated pool; use TrySubmit or a deadline.
func (m *Master) Submit(ctx context.Context, task Task) (*Future, error) {
	return m.submit(ctx, task, true)
}

// TrySubmit rejects a full queue immediately with ErrQueueFull.
func (m *Master) TrySubmit(task Task) (*Future, error) {
	return m.submit(context.Background(), task, false)
}

func (m *Master) submit(ctx context.Context, task Task, block bool) (*Future, error) {
	if isNilTask(task) {
		return nil, ErrWorkerNilTask
	}
	if !m.isInitialized() {
		return nil, ErrMasterNotInitialized
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.Lock()
		if err := m.acceptReadyErrorLocked(); err != nil {
			m.Unlock()
			return nil, err
		}
		if m.queueCapacity == 0 {
			m.Unlock()
			return nil, ErrQueueDisabled
		}
		if len(m.pending) < m.queueCapacity {
			future := newFuture()
			m.tasksWG.Add(1)
			m.pending = append(m.pending, &submission{task: task, future: future})
			m.signalQueueChangedLocked()
			m.Unlock()
			return future, nil
		}
		changed := m.queueChanged
		m.Unlock()
		if !block {
			return nil, ErrQueueFull
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.acceptDone:
			return nil, ErrMasterStopped
		case <-changed:
		}
	}
}

func (m *Master) signalQueueChangedLocked() {
	close(m.queueChanged)
	m.queueChanged = make(chan struct{})
}

func (m *Master) runQueue() {
	for {
		m.Lock()
		if len(m.pending) == 0 {
			stopped, changed := m.stopped, m.queueChanged
			m.Unlock()
			if stopped {
				return
			}
			select {
			case <-changed:
			case <-m.Quit:
			}
			continue
		}
		job := m.pending[0]
		m.Unlock()

		// Retain the head in the queue until handoff so waiting submissions
		// cannot escape the configured bound by spawning dispatch goroutines.
		w, request, err := m.dispatch(context.Background(), job.task, nil)
		m.Lock()
		m.pending[0] = nil
		m.pending = m.pending[1:]
		m.signalQueueChangedLocked()
		m.Unlock()
		if err != nil {
			job.future.complete(Result{Err: err})
			m.tasksWG.Done()
			continue
		}
		// Already counted by tasksWG at admission, including this completion.
		go func() {
			defer m.tasksWG.Done()
			job.future.complete(m.finishDispatch(w, request))
		}()
	}
}
