package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
)

// ctxTask reports the context its Run phase received and blocks until it is
// released or that context is canceled. ctxDone closes once that context is
// done, even after Run returns.
type ctxTask struct {
	entered chan struct{}
	release chan struct{}
	seen    chan error
	ctxDone chan struct{}
}

func newCtxTask() *ctxTask {
	return &ctxTask{entered: make(chan struct{}), release: make(chan struct{}), seen: make(chan error, 1), ctxDone: make(chan struct{})}
}

func (t *ctxTask) ID() string                 { return "ctx-task" }
func (t *ctxTask) Init(context.Context) error { return nil }
func (t *ctxTask) Done(context.Context) error { return nil }
func (t *ctxTask) Run(ctx context.Context) error {
	go func() { <-ctx.Done(); close(t.ctxDone) }()
	close(t.entered)
	select {
	case <-t.release:
	case <-ctx.Done():
	}
	t.seen <- ctx.Err()
	return ctx.Err()
}

func (t *ctxTask) awaitSeen(tb testing.TB) error {
	tb.Helper()
	select {
	case err := <-t.seen:
		return err
	case <-time.After(5 * time.Second):
		tb.Fatal("task did not return")
		return nil
	}
}

func startedWorker(t *testing.T) *Worker {
	t.Helper()
	w, err := NewWorker(WithLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return w
}

func TestWorkerStopCancelsRunningTaskContext(t *testing.T) {
	w := startedWorker(t)
	task := newCtxTask()
	result := make(chan Result, 1)
	go func() { result <- w.DoResult(context.Background(), task) }()
	awaitSignal(t, task.entered)

	w.Stop()
	if err := task.awaitSeen(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("task ctx error = %v, want %v", err, context.Canceled)
	}
	if got := <-result; got.Err != ErrWorkerTaskRun || !errors.Is(got.Cause, context.Canceled) {
		t.Fatalf("result = %+v", got)
	}
}

func TestWorkerShutdownLetsRunningTaskFinish(t *testing.T) {
	w := startedWorker(t)
	task := newCtxTask()
	result := make(chan Result, 1)
	go func() { result <- w.DoResult(context.Background(), task) }()
	awaitSignal(t, task.entered)

	shutdown := make(chan error, 1)
	go func() { shutdown <- w.Shutdown(testContext(t)) }()
	select {
	case err := <-task.seen:
		t.Fatalf("Shutdown canceled the running task: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(task.release)
	if err := task.awaitSeen(t); err != nil {
		t.Fatalf("task ctx error = %v, want nil", err)
	}
	if got := <-result; got.Err != nil {
		t.Fatalf("result = %+v", got)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	// The worker cancels the task context when it exits.
	awaitSignal(t, task.ctxDone)
}

func TestMasterStopCancelsRunningTaskButShutdownDoesNot(t *testing.T) {
	for _, stop := range []string{"Stop", "Shutdown"} {
		t.Run(stop, func(t *testing.T) {
			m := newFeatureMaster(t, 1, WithQueueCapacity(1))
			task := newCtxTask()
			future := submitTask(t, m, task)
			awaitSignal(t, task.entered)

			if stop == "Stop" {
				m.Stop()
				if err := task.awaitSeen(t); !errors.Is(err, context.Canceled) {
					t.Fatalf("task ctx error = %v, want %v", err, context.Canceled)
				}
				return
			}
			shutdown := make(chan error, 1)
			go func() { shutdown <- m.Shutdown(testContext(t)) }()
			select {
			case err := <-task.seen:
				t.Fatalf("Shutdown canceled the running task: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			close(task.release)
			if err := task.awaitSeen(t); err != nil {
				t.Fatalf("task ctx error = %v, want nil", err)
			}
			if got := awaitResult(t, future); got.Err != nil {
				t.Fatalf("result = %+v", got)
			}
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSubmitContextDoesNotReachTask(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(1))
	task := newCtxTask()
	ctx, cancel := context.WithCancel(context.Background())
	future, err := m.Submit(ctx, task)
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, task.entered)
	cancel()
	select {
	case err := <-task.seen:
		t.Fatalf("canceling the Submit context reached the task: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(task.release)
	if err := task.awaitSeen(t); err != nil {
		t.Fatalf("task ctx error = %v, want nil", err)
	}
	if got := awaitResult(t, future); got.Err != nil {
		t.Fatalf("result = %+v", got)
	}
}
