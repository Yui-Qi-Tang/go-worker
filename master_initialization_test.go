package worker

import (
	"testing"
	"time"
)

func TestMasterMethodsRejectUninitializedMaster(t *testing.T) {
	var ms Master

	w, err := NewWorker(WithName("uninitialized-master-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	testcases := []struct {
		name string
		call func() error
	}{
		{
			name: "AddWorker",
			call: func() error { return ms.AddWorker(w) },
		},
		{
			name: "AddWorkers",
			call: func() error { return ms.AddWorkers(1) },
		},
		{
			name: "Dispatch",
			call: func() error { return ms.Dispatch(phaseErrorTask{id: "uninitialized-dispatch"}) },
		},
		{
			name: "Schedule",
			call: func() error { return ms.Schedule(phaseErrorTask{id: "uninitialized-schedule"}) },
		},
		{
			name: "WakeAllWorkersUp",
			call: ms.WakeAllWorkersUp,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			if got := testcase.call(); got != ErrMasterNotInitialized {
				t.Fatalf("%s() error = %v, want %v", testcase.name, got, ErrMasterNotInitialized)
			}
		})
	}

	assertNotPanics(t, func() {
		ms.Stop()
	})

	if got := ms.GetWorkers(); got != 0 {
		t.Fatalf("GetWorkers() = %d, want 0", got)
	}
	if got := ms.GetPoolSize(); got != 0 {
		t.Fatalf("GetPoolSize() = %d, want 0", got)
	}
}

func TestNilMasterMethodsRejectUninitializedMaster(t *testing.T) {
	var ms *Master

	w, err := NewWorker(WithName("nil-master-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if got := ms.AddWorker(w); got != ErrMasterNotInitialized {
		t.Fatalf("AddWorker() error = %v, want %v", got, ErrMasterNotInitialized)
	}
	if got := ms.AddWorkers(1); got != ErrMasterNotInitialized {
		t.Fatalf("AddWorkers() error = %v, want %v", got, ErrMasterNotInitialized)
	}
	if got := ms.Dispatch(phaseErrorTask{id: "nil-master"}); got != ErrMasterNotInitialized {
		t.Fatalf("Dispatch() error = %v, want %v", got, ErrMasterNotInitialized)
	}
	if got := ms.WakeAllWorkersUp(); got != ErrMasterNotInitialized {
		t.Fatalf("WakeAllWorkersUp() error = %v, want %v", got, ErrMasterNotInitialized)
	}

	assertNotPanics(t, func() {
		ms.Stop()
	})
}

func TestUninitializedMasterRecoveryWorkerReturns(t *testing.T) {
	var ms Master
	done := make(chan struct{})

	go func() {
		ms.RecoveryWorker()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RecoveryWorker blocked on uninitialized master")
	}
}
