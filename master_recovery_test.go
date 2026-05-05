package worker

import (
	"sync/atomic"
	"testing"
	"time"
)

type recoveryTask struct {
	panicInInit bool
	runs        *int32
}

func (t recoveryTask) Init() error {
	if t.panicInInit {
		panic("recover me")
	}
	return nil
}

func (t recoveryTask) Run() error {
	if t.runs != nil {
		atomic.AddInt32(t.runs, 1)
	}
	return nil
}

func (t recoveryTask) Done() error {
	return nil
}

func (t recoveryTask) ID() string {
	if t.panicInInit {
		return "panic"
	}
	return "count"
}

func TestMasterRecoveryStartsReplacementWorker(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
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

	ms.Schedule(recoveryTask{panicInInit: true})

	var runs int32
	done := make(chan struct{})
	go func() {
		ms.Schedule(recoveryTask{runs: &runs})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("master recovery queued a replacement worker that did not process work")
	}

	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("replacement worker runs = %d, want 1", got)
	}
}
