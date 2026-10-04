package worker

import (
	"testing"
	"time"
)

func TestWorkerDoBeforeStartReturnsError(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	if got := w.Do(phaseErrorTask{id: "before-start"}); got != ErrWorkerNotStarted {
		t.Fatalf("Do() error = %v, want %v", got, ErrWorkerNotStarted)
	}
}

func TestMasterDispatchBeforeWorkersStartReturnsError(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "before-start"})
	}()

	select {
	case got := <-done:
		if got != ErrWorkerNotStarted {
			t.Fatalf("Dispatch() error = %v, want %v", got, ErrWorkerNotStarted)
		}
	case <-time.After(time.Second):
		t.Fatal("Dispatch deadlocked before workers were started")
	}

	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	done = make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "after-start"})
	}()

	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("Dispatch() after WakeAllWorkersUp error = %v, want nil", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Dispatch deadlocked after workers were started")
	}
}
