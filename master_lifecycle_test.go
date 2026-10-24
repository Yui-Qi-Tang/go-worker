package worker

import (
	"context"
	"testing"
	"time"
)

func TestMasterRejectsWorkAfterStop(t *testing.T) {
	ms, err := NewMaster(WithWorkers(1, 1), WithQueueCapacity(1))
	if err != nil {
		t.Fatal(err)
	}
	ms.Stop()

	done := make(chan error, 1)
	go func() { done <- ms.Schedule(phaseErrorTask{id: "after-stop"}) }()
	select {
	case got := <-done:
		if got != ErrMasterStopped {
			t.Fatalf("Schedule() error = %v, want %v", got, ErrMasterStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("Schedule deadlocked after master stopped")
	}
	if _, got := ms.Submit(context.Background(), phaseErrorTask{}); got != ErrMasterStopped {
		t.Fatalf("Submit() error = %v, want %v", got, ErrMasterStopped)
	}
	if err := ms.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := ms.Workers(); got != 0 {
		t.Fatalf("Workers() after shutdown = %d", got)
	}
}
