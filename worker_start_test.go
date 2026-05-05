package worker

import "testing"

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
