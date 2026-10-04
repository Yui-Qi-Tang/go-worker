package worker

import "testing"

func TestMasterScheduleSkipsUnstartedWorkerWhenStartedWorkerAvailable(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	ms.WorkerQueue = make(chan *Worker, 3)
	defer ms.Stop()

	unstarted, err := NewWorker(WithName("unstarted-queued-worker"))
	if err != nil {
		t.Fatal(err)
	}
	started, err := NewWorker(WithName("started-queued-worker"))
	if err != nil {
		t.Fatal(err)
	}
	if err := started.Start(); err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorker(unstarted); err != nil {
		t.Fatal(err)
	}
	if got := receiveQueuedWorker(t, ms); got != unstarted {
		t.Fatal("master queued an unexpected unstarted worker")
	}
	if err := ms.AddWorker(started); err != nil {
		t.Fatal(err)
	}
	if got := receiveQueuedWorker(t, ms); got != started {
		t.Fatal("master queued an unexpected started worker")
	}

	ms.WorkerQueue <- unstarted
	ms.WorkerQueue <- started

	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("Dispatch() error = %v, want nil", got)
	}
}
