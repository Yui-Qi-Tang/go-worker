package worker

import (
	"context"
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

func (a *atomicInt32) Init(context.Context) error {
	return nil
}

func (a *atomicInt32) Run(context.Context) error {
	a.addone()
	return nil
}

func (a *atomicInt32) Done(context.Context) error {
	return nil
}

func (a *atomicInt32) ID() string {
	return "atomic-type"
}

func TestAtomicType(t *testing.T) {
	var test atomicInt32 = 0
	testvar := &test

	var taskCounts = 1000

	const workers = 20

	ms, err := NewMaster(WithWorkers(workers, workers))
	if err != nil {
		t.Fatal(err)
	}

	if ms.Workers() != workers {
		t.Fatalf("wrong on number of workers: %d, expected: %d", ms.Workers(), workers)
	}

	for range taskCounts {
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

	ms, err := NewMaster(WithWorkers(workerNums, workerNums))
	if err != nil {
		t.Fatal(err)
	}

	if ms.Workers() != workerNums {
		t.Fatalf("wrong on number of workers: %d, expected: %d", ms.Workers(), workerNums)
	}

	for range taskConuts {
		ms.Schedule(normal) // normal is a test case from worker_test
	}

	ms.Stop()

	t.Log("... Passed")

}

func TestMasterRecoversPanickedWorkerBeforeScheduleReturns(t *testing.T) {
	ms, err := NewMaster(WithWorkers(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	originalWorker := currentWorker(t, ms)
	if err := ms.Schedule(panicErr); err != ErrWorkerPanic {
		t.Fatalf("wrong schedule error: %v, expected: %v", err, ErrWorkerPanic)
	}
	if workers := ms.Workers(); workers != 1 {
		t.Fatalf("wrong number of workers after recovery: %d, expected: %d", workers, 1)
	}
	if recoveredWorker := currentWorker(t, ms); recoveredWorker == originalWorker {
		t.Fatalf("panic worker stayed in pool: %p", recoveredWorker)
	}
	if err := ms.Schedule(normal); err != nil {
		t.Fatalf("recovered worker should process tasks: %v", err)
	}
}
