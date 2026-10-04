package worker

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestWorkerTaskChannelRejectsNilTaskWithoutPanic(t *testing.T) {
	w, err := NewWorker(WithName("nil-task-channel-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	var task Task
	go func() {
		w.Task <- task
	}()

	if got := w.waitStatus(); got != workerErrNil {
		t.Fatalf("nil task status = %s, want %s", got, workerErrNil)
	}

	if got := w.Do(normal); got != nil {
		t.Fatalf("Do() after nil channel task error = %v, want nil", got)
	}
}

func TestWorkerTaskChannelCloseStopsWorkerAndDoReturnsStopped(t *testing.T) {
	w, err := NewWorker(WithName("closed-task-channel-worker"))
	if err != nil {
		t.Fatal(err)
	}
	w.logger = zap.NewNop()
	defer w.Stop()

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	close(w.Task)

	done := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				done <- fmt.Errorf("Do() panicked after Task channel close: %v", recovered)
			}
		}()
		done <- w.Do(normal)
	}()

	select {
	case got := <-done:
		if got != ErrWorkerStopped {
			t.Fatalf("Do() after Task channel close error = %v, want %v", got, ErrWorkerStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("Do() after Task channel close blocked")
	}

	deadline := time.After(time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if w.isStopped() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("worker did not stop after Task channel close")
		case <-tick.C:
		}
	}
}
