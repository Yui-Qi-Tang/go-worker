package worker

import "testing"

func TestWithWorkerRecoveryFalseAfterTrueDisablesRecovery(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true), WithWorkerRecovery(false))
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
		t.Fatalf("workers after panic = %d, want 0 because recovery was disabled", got)
	}

	if got := dispatchWithTimeout(t, ms, normal); got != ErrMasterWorkerPoolIsEmpty {
		t.Fatalf("Dispatch() after disabled recovery panic error = %v, want %v", got, ErrMasterWorkerPoolIsEmpty)
	}
}
