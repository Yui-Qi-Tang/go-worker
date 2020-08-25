package worker

import (
	"testing"
)

func TestMasterWithNormalTask(t *testing.T) {

	const workerNums = 10
	const taskConuts = 1000

	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorkers(workerNums); err != nil {
		t.Fatal(err)
	}

	if ms.GetWorkers() != workerNums {
		t.Fatalf("wrong on number of workers: %d, expected: %d", ms.GetWorkers(), workerNums)
	}

	for i := 0; i < taskConuts; i++ {
		ms.Schedule(normal) // normal is a test case from worker_test
	}

	ms.Stop()
}
