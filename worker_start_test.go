package worker

import (
	"testing"
	"time"
)

func TestWorkerStartReturnsLifecycleErrors(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	if got := w.Start(); got != nil {
		t.Fatalf("Start() error = %v, want nil", got)
	}

	if got := w.Start(); got != ErrWorkerAlreadyStarted {
		t.Fatalf("second Start() error = %v, want %v", got, ErrWorkerAlreadyStarted)
	}

	w.Stop()

	if got := w.Start(); got != ErrWorkerStopped {
		t.Fatalf("Start() after Stop() error = %v, want %v", got, ErrWorkerStopped)
	}
}

func TestMasterWakeAllWorkersUpReturnsStoppedWorkerError(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	w.Stop()

	if err := ms.AddWorker(w); err != nil {
		t.Fatal(err)
	}

	if got := ms.WakeAllWorkersUp(); got != ErrWorkerStopped {
		t.Fatalf("WakeAllWorkersUp() error = %v, want %v", got, ErrWorkerStopped)
	}
}

func TestMasterWakeAllWorkersUpRemovesStoppedWorker(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	w.Stop()

	if err := ms.AddWorker(w); err != nil {
		t.Fatal(err)
	}

	if got := ms.WakeAllWorkersUp(); got != ErrWorkerStopped {
		t.Fatalf("WakeAllWorkersUp() error = %v, want %v", got, ErrWorkerStopped)
	}
	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after stopped worker wakeup = %d, want 0", got)
	}

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(phaseErrorTask{id: "after-stopped-worker"})
	}()

	select {
	case got := <-done:
		if got != ErrMasterWorkerPoolIsEmpty {
			t.Fatalf("Dispatch() after stopped worker wakeup error = %v, want %v", got, ErrMasterWorkerPoolIsEmpty)
		}
	case <-time.After(time.Second):
		t.Fatal("Dispatch deadlocked after stopped worker wakeup")
	}
}
