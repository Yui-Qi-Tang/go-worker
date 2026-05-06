package worker

import "testing"

func TestMasterRecoversDirectWorkerTaskPanicWithoutStatusObserver(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	ms.WorkerQueue = make(chan *Worker, 2)
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

	oldName := worker.Name
	go func() {
		worker.Task <- panicErr
	}()

	waitForWorkerReplacement(t, ms, oldName)

	if workerInPool(ms, worker) {
		t.Fatal("master left the directly panicked worker in the pool")
	}
	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("replacement worker Dispatch() error = %v, want nil", got)
	}
}
