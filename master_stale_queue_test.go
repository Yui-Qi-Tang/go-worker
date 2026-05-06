package worker

import "testing"

func TestMasterScheduleSkipsRemovedQueuedWorker(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	ms.WorkerQueue = make(chan *Worker, 2)
	defer ms.Stop()

	stale, err := NewWorker(WithName("stale-worker"))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := NewWorker(WithName("valid-worker"))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorker(stale); err != nil {
		t.Fatal(err)
	}
	if err := ms.AddWorker(valid); err != nil {
		t.Fatal(err)
	}

	stale.Stop()
	if got := ms.WakeAllWorkersUp(); got != ErrWorkerStopped {
		t.Fatalf("WakeAllWorkersUp() error = %v, want %v", got, ErrWorkerStopped)
	}
	if got := ms.WakeAllWorkersUp(); got != nil {
		t.Fatalf("second WakeAllWorkersUp() error = %v, want nil", got)
	}

	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("Dispatch() error = %v, want nil", got)
	}
}
