package worker

import (
	"bytes"
	"io"
	"os"
	"slices"
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

	oldWorker := currentWorker(t, ms)
	if got := dispatchWithTimeout(t, ms, panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Dispatch() error = %v, want %v", got, ErrWorkerPanic)
	}
	waitForWorkerReplacement(t, ms, oldWorker)

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

	first, err := NewWorker(WithName("first-worker"), withRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorker(WithName("second-worker"), withRecovery(true))
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
	second.name = first.name

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
	if got := ms.Workers(); got != 2 {
		t.Fatalf("workers after duplicate-name recovery = %d, want 2", got)
	}
}

func TestMasterRecoveryPreservesRegisteredWorkerIdentity(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	worker, err := NewWorker(WithName("stable-recovery-worker"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.AddWorker(worker); err != nil {
		t.Fatal(err)
	}
	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	registeredIdentity := worker.identity()
	if got := dispatchWithTimeout(t, ms, panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Dispatch() error = %v, want %v", got, ErrWorkerPanic)
	}
	waitForWorkerReplacement(t, ms, worker)

	duplicate, err := NewWorker(WithName(registeredIdentity))
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Stop()

	if got := ms.AddWorker(duplicate); got != ErrMasterDuplicateWorkerName {
		t.Fatalf("AddWorker(duplicate after recovery) error = %v, want %v", got, ErrMasterDuplicateWorkerName)
	}
	if got := ms.Workers(); got != 1 {
		t.Fatalf("workers after rejected duplicate recovery identity = %d, want 1", got)
	}
}

func TestMasterAddWorkerAttachesRecoveryChannel(t *testing.T) {
	ms, err := NewMaster(WithWorkerRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	worker, err := NewWorker(WithName("manual-recovery-worker"))
	if err != nil {
		t.Fatal(err)
	}

	if err := ms.AddWorker(worker); err != nil {
		t.Fatal(err)
	}
	if worker.recovery != ms.workerPanic {
		t.Fatal("AddWorker did not attach the master recovery channel")
	}
	if got := receiveQueuedWorker(t, ms); got != worker {
		t.Fatalf("queued worker = %p, want added worker %p", got, worker)
	}

	if err := ms.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}

	if got := worker.Do(panicErr); got != ErrWorkerPanic {
		t.Fatalf("direct Do() error = %v, want %v", got, ErrWorkerPanic)
	}

	waitForWorkerReplacement(t, ms, worker)

	if workerInPool(ms, worker) {
		t.Fatal("master left the panicked worker in the pool")
	}
	if got := dispatchWithTimeout(t, ms, normal); got != nil {
		t.Fatalf("replacement worker Dispatch() error = %v, want nil", got)
	}
}

func TestMasterAddWorkerClearsRecoveryChannelWhenDisabled(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	worker, err := NewWorker(WithName("orphan-recovery-worker"), withRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	if worker.recovery == nil {
		t.Fatal("test worker should start with a recovery channel")
	}

	if err := ms.AddWorker(worker); err != nil {
		t.Fatal(err)
	}
	if worker.recovery != nil {
		t.Fatal("AddWorker kept an orphan recovery channel on a non-recovery master")
	}
}

func receiveQueuedWorker(t *testing.T, ms *Master) *Worker {
	t.Helper()

	select {
	case worker := <-ms.workerQueue:
		return worker
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued worker")
		return nil
	}
}

func workerInPool(ms *Master, target *Worker) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return slices.Contains(ms.pool, target)
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
