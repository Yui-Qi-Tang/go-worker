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
	// ErrInvalidQueueCapacity means the queue capacity is negative or above 1024.
	ErrInvalidQueueCapacity = errors.New("task queue capacity must be between 0 and 1024")
)

// WithQueueCapacity bounds submitted tasks waiting for a worker; running tasks
// do not count. Zero, the default, disables Submit and TrySubmit. The queue's
// memory is allocated by NewMaster. Synchronous Schedule calls wait in the
// same order but do not count against this capacity.
func WithQueueCapacity(capacity int) MasterOption {
	return func(m *Master) { m.queueCapacity = capacity }
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.admit.RLock()
	defer m.admit.RUnlock()
	if err := m.admissionError(); err != nil {
		return nil, err
	}
	if m.ready.slots == nil {
		return nil, ErrQueueDisabled
	}
	if err := m.grow(int(m.ready.waiting.Load()) + 1); err != nil {
		return nil, err
	}
	if !block {
		select {
		case m.ready.slots <- struct{}{}:
		default:
			return nil, ErrQueueFull
		}
	} else {
		select {
		case m.ready.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-m.acceptDone:
			return nil, ErrMasterStopped
		}
	}
	request := &taskRequest{task: task, future: newFuture()}
	if err := m.ready.push(request); err != nil {
		return nil, err
	}
	return request.future, nil
}
