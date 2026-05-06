package worker

import (
	"sync/atomic"
	"testing"
	"time"
)

type countingTask struct {
	count *int32
}

func (t countingTask) Init() error {
	return nil
}

func (t countingTask) Run() error {
	atomic.AddInt32(t.count, 1)
	return nil
}

func (t countingTask) Done() error {
	return nil
}

func (t countingTask) ID() string {
	return "counting-task"
}

func TestWorkerTaskChannelWithoutStatusObserverDoesNotBlockFutureDo(t *testing.T) {
	w, err := NewWorker(WithName("direct-task-status-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	var count int32
	task := countingTask{count: &count}

	sent := make(chan struct{})
	go func() {
		w.Task <- task
		close(sent)
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("direct Worker.Task send timed out")
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Do(task)
	}()

	select {
	case got := <-done:
		if got != nil {
			t.Fatalf("Do() after direct Worker.Task error = %v, want nil", got)
		}
	case <-time.After(time.Second):
		t.Fatal("Do() after direct Worker.Task blocked waiting for stale status")
	}

	if got := atomic.LoadInt32(&count); got != 2 {
		t.Fatalf("task run count = %d, want 2", got)
	}
}
