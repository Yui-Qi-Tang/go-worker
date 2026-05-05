package worker

import (
	"sync/atomic"
	"testing"
	"time"
)

type recoveryTask struct {
	id          string
	panicInInit bool
	runs        *int32
}

func (t recoveryTask) Init() error {
	if t.panicInInit {
		panic("recover me")
	}
	return nil
}

func (t recoveryTask) Run() error {
	if t.runs != nil {
		atomic.AddInt32(t.runs, 1)
	}
	return nil
}

func (t recoveryTask) Done() error {
	return nil
}

func (t recoveryTask) ID() string {
	return t.id
}

func TestMasterRecoveryStartsReplacementWorker(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		oldName := currentWorkerName(t, ms)
		if err := ms.Schedule(recoveryTask{id: "panic-recovery", panicInInit: true}); err != ErrWorkerPanic {
			t.Fatalf("panic task error = %v, want %v", err, ErrWorkerPanic)
		}
		waitForWorkerReplacement(t, ms, oldName)

		var runs int32
		if err := scheduleWithTimeout(t, ms, recoveryTask{id: "normal-after-recovery", runs: &runs}); err != nil {
			t.Fatalf("replacement worker returned error: %v", err)
		}
		if got := atomic.LoadInt32(&runs); got != 1 {
			t.Fatalf("replacement worker runs = %d, want 1", got)
		}
	}
}

func currentWorkerName(t *testing.T, ms *Master) string {
	t.Helper()

	ms.RLock()
	defer ms.RUnlock()

	if len(ms.Pool) == 0 {
		t.Fatal("master has no workers")
	}

	return ms.Pool[0].Name
}

func waitForWorkerReplacement(t *testing.T, ms *Master, oldName string) {
	t.Helper()

	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for worker %s to be replaced", oldName)
		case <-tick.C:
			if currentWorkerName(t, ms) != oldName {
				return
			}
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
