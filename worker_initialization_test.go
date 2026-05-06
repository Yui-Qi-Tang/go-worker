package worker

import "testing"

func TestMasterAddWorkerRejectsUninitializedWorker(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	w := &Worker{Name: "uninitialized-worker"}

	if got := ms.AddWorker(w); got != ErrWorkerNotInitialized {
		t.Fatalf("AddWorker() error = %v, want %v", got, ErrWorkerNotInitialized)
	}
	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after rejected uninitialized worker = %d, want 0", got)
	}
}
