package worker

import (
	"testing"
	"time"
)

func TestWorkerDoRejectsNilTaskWithoutStoppingWorker(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	nilTasks := []Task{
		nil,
		(*phaseErrorTask)(nil),
	}

	for _, task := range nilTasks {
		if got := w.Do(task); got != ErrWorkerNilTask {
			t.Fatalf("Do(nil task) error = %v, want %v", got, ErrWorkerNilTask)
		}
	}

	if got := w.Do(phaseErrorTask{id: "after-nil"}); got != nil {
		t.Fatalf("Do() after nil task error = %v, want nil", got)
	}
}

func TestMasterDispatchRejectsNilTaskWithoutConsumingWorker(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	nilTasks := []Task{
		nil,
		(*phaseErrorTask)(nil),
	}

	for _, task := range nilTasks {
		if got := dispatchWithTimeout(t, ms, task); got != ErrWorkerNilTask {
			t.Fatalf("Dispatch(nil task) error = %v, want %v", got, ErrWorkerNilTask)
		}
	}

	if got := dispatchWithTimeout(t, ms, phaseErrorTask{id: "after-nil"}); got != nil {
		t.Fatalf("Dispatch() after nil task error = %v, want nil", got)
	}
}

func dispatchWithTimeout(t *testing.T, ms *Master, task Task) error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		done <- ms.Dispatch(task)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for task dispatch")
		return nil
	}
}
