package worker

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMasterRecoveryDoesNotStartDuplicateReplacement(t *testing.T) {
	restoreStderr := captureStderr(t)

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

	oldName := currentWorkerName(t, ms)
	if got := dispatchWithTimeout(t, ms, panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Dispatch() error = %v, want %v", got, ErrWorkerPanic)
	}
	waitForWorkerReplacement(t, ms, oldName)

	time.Sleep(20 * time.Millisecond)

	logs := restoreStderr()
	if starts := strings.Count(logs, "\tstarting\t"); starts != 2 {
		t.Fatalf("worker start log count after one recovery = %d, want 2\nlogs:\n%s", starts, logs)
	}
}

func TestMasterRecoveryUsesWorkerIdentityAfterNameMutation(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	first, err := NewWorker(WithName("first-worker"), WithRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorker(WithName("second-worker"), WithRecovery(true))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorker(first); err != nil {
		t.Fatal(err)
	}
	if got := receiveQueuedWorker(t, ms); got != first {
		t.Fatalf("queued worker = %p, want first worker %p", got, first)
	}
	if err := ms.AddWorker(second); err != nil {
		t.Fatal(err)
	}
	second.Name = first.Name

	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	if got := dispatchWithTimeout(t, ms, panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Dispatch() error = %v, want %v", got, ErrWorkerPanic)
	}

	if !workerInPool(ms, first) {
		t.Fatal("recovery removed the non-panicked worker after another worker reused its public name")
	}
	if workerInPool(ms, second) {
		t.Fatal("recovery left the panicked worker in the pool")
	}
	if got := ms.GetWorkers(); got != 2 {
		t.Fatalf("workers after duplicate-name recovery = %d, want 2", got)
	}
}

func receiveQueuedWorker(t *testing.T, ms *Master) *Worker {
	t.Helper()

	select {
	case worker := <-ms.WorkerQueue:
		return worker
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued worker")
		return nil
	}
}

func workerInPool(ms *Master, target *Worker) bool {
	ms.RLock()
	defer ms.RUnlock()

	for _, worker := range ms.Pool {
		if worker == target {
			return true
		}
	}
	return false
}

func captureStderr(t *testing.T) func() string {
	t.Helper()

	original := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}

	os.Stderr = writer

	var logs bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&logs, reader)
		close(done)
	}()

	restored := false
	restore := func() string {
		if restored {
			return logs.String()
		}
		restored = true
		os.Stderr = original
		_ = writer.Close()
		<-done
		_ = reader.Close()
		return logs.String()
	}

	t.Cleanup(func() {
		restore()
	})

	return restore
}
