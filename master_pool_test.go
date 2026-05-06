package worker

import "testing"

func TestMasterGetPoolSizeReturnsWorkerCount(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	const workers = 3
	if err := ms.AddWorkers(workers); err != nil {
		t.Fatal(err)
	}

	if got := ms.GetPoolSize(); got != workers {
		t.Fatalf("GetPoolSize() = %d, want %d", got, workers)
	}
}
