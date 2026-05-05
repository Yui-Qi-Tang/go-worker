package worker

import "testing"

func TestMasterDispatchAfterPanicWithoutRecoveryReturnsEmptyPool(t *testing.T) {
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

	if got := dispatchWithTimeout(t, ms, panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Dispatch() error = %v, want %v", got, ErrWorkerPanic)
	}

	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after panic = %d, want 0", got)
	}

	if got := dispatchWithTimeout(t, ms, phaseErrorTask{id: "after-panic"}); got != ErrMasterWorkerPoolIsEmpty {
		t.Fatalf("Dispatch() after panic error = %v, want %v", got, ErrMasterWorkerPoolIsEmpty)
	}
}
