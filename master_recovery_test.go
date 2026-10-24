package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type recoveryTask struct {
	id          string
	panicInInit bool
	runs        *int32
}

func (t recoveryTask) Init(context.Context) error {
	if t.panicInInit {
		panic("recover me")
	}
	return nil
}

func (t recoveryTask) Run(context.Context) error {
	if t.runs != nil {
		atomic.AddInt32(t.runs, 1)
	}
	return nil
}

func (t recoveryTask) Done(context.Context) error {
	return nil
}

func (t recoveryTask) ID() string {
	return t.id
}

func TestMasterRecoveryStartsReplacementWorker(t *testing.T) {
	ms, err := NewMaster(WithWorkers(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	for range 2 {
		oldWorker := currentWorker(t, ms)
		if err := ms.Schedule(recoveryTask{id: "panic-recovery", panicInInit: true}); err != ErrWorkerPanic {
			t.Fatalf("panic task error = %v, want %v", err, ErrWorkerPanic)
		}
		// The replacement starts before the panic result is reported.
		if replacement := currentWorker(t, ms); replacement == oldWorker {
			t.Fatal("panicked worker is still in the pool")
		}

		var runs int32
		if err := scheduleWithTimeout(t, ms, recoveryTask{id: "normal-after-recovery", runs: &runs}); err != nil {
			t.Fatalf("replacement worker returned error: %v", err)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Fatalf("replacement worker runs = %d, want 1", got)
		}
	}
}

func scheduleWithTimeout(t *testing.T, ms *Master, task Task) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- ms.Schedule(task)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for task %s to finish", task.ID())
		return nil
	}
}
