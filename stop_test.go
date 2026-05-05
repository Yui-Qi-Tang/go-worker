package worker

import "testing"

func TestWorkerStopIsIdempotent(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	mustNotPanic(t, func() {
		w.Stop()
		w.Stop()
	})
}

func TestMasterStopIsIdempotent(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	mustNotPanic(t, func() {
		ms.Stop()
		ms.Stop()
	})
}

func mustNotPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if err := recover(); err != nil {
			t.Fatalf("unexpected panic: %v", err)
		}
	}()

	fn()
}
