package worker

import (
	"context"
	"errors"
)

// ErrFutureNotInitialized indicates a Future not returned by Submit/TrySubmit.
var ErrFutureNotInitialized = errors.New("future is not initialized")

// Future holds one asynchronous outcome. Any number of callers can wait for the
// same result. A Future must be obtained from Submit or TrySubmit.
type Future struct {
	done   chan struct{}
	result Result
}

func newFuture() *Future { return &Future{done: make(chan struct{})} }

func (f *Future) complete(result Result) {
	f.result = result
	close(f.done)
}

// Done closes when the outcome is available. It is nil for an invalid Future.
func (f *Future) Done() <-chan struct{} {
	if f == nil {
		return nil
	}
	return f.done
}

// Wait waits for the outcome. Its error describes cancellation of this wait;
// task errors are stored in Result.Err. Canceling a wait does not cancel work.
func (f *Future) Wait(ctx context.Context) (Result, error) {
	if f == nil || f.done == nil {
		return Result{}, ErrFutureNotInitialized
	}
	if err := waitDone(ctx, f.done); err != nil {
		return Result{}, err
	}
	return f.result, nil
}

func waitDone(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
