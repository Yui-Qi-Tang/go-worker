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

	const workers = 20

	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorkers(workers); err != nil {
		t.Fatal(err)
	}

	if err := ms.WakeAllWorkersUp(); err != nil {
		if err == ErrMasterWorkerPoolIsEmpty {
			t.Fatal(err)
		}
		panic(err) // unexcepted error
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

	if err := ms.WakeAllWorkersUp(); err != nil {
		if err == ErrMasterWorkerPoolIsEmpty {
			t.Fatal(err)
		}
		panic(err) // unexcepted error
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

func TestMasterRecoversPanickedWorkerBeforeScheduleReturns(t *testing.T) {
	ms, err := NewMaster(withBufferedRecoverySignalOnly())
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

	ms.RLock()
	originalWorker := ms.Pool[0]
	ms.RUnlock()

	if err := ms.Schedule(panicErr); err != ErrWorkerPanic {
		t.Fatalf("wrong schedule error: %v, expected: %v", err, ErrWorkerPanic)
	}

	if workers := ms.GetWorkers(); workers != 1 {
		t.Fatalf("wrong number of workers after recovery: %d, expected: %d", workers, 1)
	}

	ms.RLock()
	recoveredWorker := ms.Pool[0]
	ms.RUnlock()

	if recoveredWorker == originalWorker {
		t.Fatalf("panic worker stayed in pool: %p", recoveredWorker)
	}

	if err := ms.Schedule(normal); err != nil {
		t.Fatalf("recovered worker should process tasks: %v", err)
	}
}

func withBufferedRecoverySignalOnly() MasterOption {
	return func(m *Master) {
		m.workerPanic = make(chan string, 1)
	}
}
