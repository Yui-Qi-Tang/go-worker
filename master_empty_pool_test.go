package worker

import (
	"testing"
	"time"
)

func TestMasterDispatchWithEmptyPoolReturnsError(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "empty-pool"})
	}()

	select {
	case got := <-done:
		if got != ErrMasterWorkerPoolIsEmpty {
			t.Fatalf("Dispatch() error = %v, want %v", got, ErrMasterWorkerPoolIsEmpty)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Dispatch deadlocked with an empty worker pool")
	}
}

func TestMasterDispatchWithRecoveryEnabledEmptyPoolReturnsError(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "recovery-empty-pool"})
	}()

	select {
	case got := <-done:
		if got != ErrMasterWorkerPoolIsEmpty {
			t.Fatalf("Dispatch() error = %v, want %v", got, ErrMasterWorkerPoolIsEmpty)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Dispatch deadlocked with recovery enabled and an empty worker pool")
	}
}
