package worker

import "testing"

func TestAddWorkersRejectsInvalidCountsWithoutPartialPoolMutation(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if got := ms.AddWorkers(-1); got != ErrMasterSetupWithInvalidWorkerCount {
		t.Fatalf("AddWorkers(-1) error = %v, want %v", got, ErrMasterSetupWithInvalidWorkerCount)
	}
	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after rejected negative count = %d, want 0", got)
	}

	if got := ms.AddWorkers(int(maxPoolSize) + 1); got != ErrMasterSetupWithTooLargePoolSize {
		t.Fatalf("AddWorkers(maxPoolSize+1) error = %v, want %v", got, ErrMasterSetupWithTooLargePoolSize)
	}
	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("workers after rejected oversized count = %d, want 0", got)
	}
}

func TestAddWorkersRejectsOverCapacityWithoutPartialPoolMutation(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if err := ms.AddWorkers(1); err != nil {
		t.Fatal(err)
	}

	if got := ms.AddWorkers(int(maxPoolSize)); got != ErrMasterWorkerPoolIsFull {
		t.Fatalf("AddWorkers(over remaining capacity) error = %v, want %v", got, ErrMasterWorkerPoolIsFull)
	}
	if got := ms.GetWorkers(); got != 1 {
		t.Fatalf("workers after rejected over-capacity add = %d, want 1", got)
	}
}
