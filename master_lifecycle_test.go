package worker

import (
	"testing"
	"time"
)

func TestMasterDispatchAfterStopReturnsError(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	ms.Stop()

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "after-stop"})
	}()

	select {
	case got := <-done:
		if got != ErrMasterStopped {
			t.Fatalf("Dispatch() error = %v, want %v", got, ErrMasterStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("Dispatch deadlocked after master stopped")
	}
}

func TestMasterRejectsLifecycleOperationsAfterStop(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	ms.Stop()

	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if got := ms.AddWorker(w); got != ErrMasterStopped {
		t.Fatalf("AddWorker() error = %v, want %v", got, ErrMasterStopped)
	}

	if got := ms.WakeAllWorkersUp(); got != ErrMasterStopped {
		t.Fatalf("WakeAllWorkersUp() error = %v, want %v", got, ErrMasterStopped)
	}
}

func TestMasterAddWorkersZeroCountAfterStopReturnsStopped(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	ms.Stop()

	if got := ms.AddWorkers(0); got != ErrMasterStopped {
		t.Fatalf("AddWorkers(0) after Stop() error = %v, want %v", got, ErrMasterStopped)
	}
}
