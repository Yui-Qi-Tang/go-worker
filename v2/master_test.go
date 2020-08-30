package worker

import (
	"sync/atomic"
	"testing"
)

type atomicInt32 int32

func (a *atomicInt32) addone() {
	atomic.AddInt32(((*int32)(a)), 1)
}

func (a *atomicInt32) load() int32 {
	return atomic.LoadInt32((*int32)(a))
}

func (a *atomicInt32) Init() error {
	return nil
}

func (a *atomicInt32) Run() error {
	a.addone()
	return nil
}

func (a *atomicInt32) Done() error {
	return nil
}

func (a *atomicInt32) ID() string {
	return "atomic-type"
}

func TestAtomicType(t *testing.T) {
	var test atomicInt32 = 0
	testvar := &test

	var taskCounts = 1000

	const workers = 10

	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorkers(workers); err != nil {
		t.Fatal(err)
	}

	if ms.GetWorkers() != workers {
		t.Fatalf("wrong on number of workers: %d, expected: %d", ms.GetWorkers(), workers)
	}

	for i := 0; i < taskCounts; i++ {
		ms.Schedule(testvar) // normal is a test case from worker_test
	}

	if testvar.load() != int32(taskCounts) || taskCounts != int(*testvar) {
		t.Logf("wrong on result of sum of testvar: %d, expected: %d", testvar.load(), taskCounts)
	}

	ms.Stop()

	t.Log("... Passed")
}

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

	t.Log("... Passed")

}
