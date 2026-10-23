package worker

import "testing"

func TestMasterRecoversDirectWorkerDoPanic(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	ms.workerQueue = make(chan *Worker, 2)
	defer ms.Stop()

	worker, err := NewWorker(WithName("direct-panic-worker"))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorker(worker); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	if got := worker.Do(panicErr); got != ErrWorkerPanic {
		t.Fatalf("direct Do() error = %v, want %v", got, ErrWorkerPanic)
	}

	waitForWorkerReplacement(t, ms, worker)

	if workerInPool(ms, worker) {
		t.Fatal("master left the directly panicked worker in the pool")
	}
	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("replacement worker Dispatch() error = %v, want nil", got)
	}
}
