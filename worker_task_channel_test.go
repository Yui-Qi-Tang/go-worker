package worker

import "testing"

func TestWorkerTaskChannelRejectsNilTaskWithoutPanic(t *testing.T) {
	w, err := NewWorker(WithName("nil-task-channel-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	var task Task
	go func() {
		w.Task <- task
	}()

	if got := w.waitStatus(); got != workerErrNil {
		t.Fatalf("nil task status = %s, want %s", got, workerErrNil)
	}

	if got := w.Do(normal); got != nil {
		t.Fatalf("Do() after nil channel task error = %v, want nil", got)
	}
}
