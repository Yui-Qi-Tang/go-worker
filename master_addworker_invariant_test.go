package worker

import "testing"

func TestMasterAddWorkerRejectsZeroValueWorkerAsUninitialized(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	w := &Worker{}

	if got := ms.AddWorker(w); got != ErrWorkerNotInitialized {
		t.Fatalf("AddWorker(zero-value worker) error = %v, want %v", got, ErrWorkerNotInitialized)
	}
	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after rejected zero-value worker = %d, want 0", got)
	}
}
