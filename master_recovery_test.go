package worker

import (
	"testing"
	"time"
)

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

	scheduleWithTimeout := func(task Task, name string) {
		t.Helper()

		done := make(chan struct{})
		go func() {
			ms.Schedule(task)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Schedule(%s) did not complete", name)
		}
	}

	scheduleWithTimeout(panicErr, "panic task")

	var task atomicInt32
	scheduleWithTimeout(&task, "replacement worker task")

	if got := task.load(); got != 1 {
		t.Fatalf("replacement worker task ran %d times, expected 1", got)
	}
}
