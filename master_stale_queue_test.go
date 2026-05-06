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

func TestMasterScheduleSkipsQueuedWorkerWithReusedIdentity(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	ms.WorkerQueue = make(chan *Worker, 3)
	defer ms.Stop()

	stale, err := NewWorker(WithName("reused-worker"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.AddWorker(stale); err != nil {
		t.Fatal(err)
	}
	if got := receiveQueuedWorker(t, ms); got != stale {
		t.Fatal("master queued an unexpected worker")
	}

	stale.Stop()
	if removed := ms.removeWorker(stale.identity(), false); !removed {
		t.Fatal("failed to remove stale worker from pool")
	}

	ms.WorkerQueue <- stale

	replacement, err := NewWorker(WithName("reused-worker"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.AddWorker(replacement); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("Dispatch() error = %v, want nil", got)
	}
	if !workerInPool(ms, replacement) {
		t.Fatal("master removed the replacement worker after seeing a stale queued worker with the same identity")
	}
}
