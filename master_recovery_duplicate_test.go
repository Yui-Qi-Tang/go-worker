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
