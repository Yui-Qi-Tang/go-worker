package worker

import (
	"testing"
	"time"
)

func TestRecoveryWorkerReturnsWhenRecoveryChannelClosed(t *testing.T) {
	ms := &Master{
		workerPanic:         make(chan string),
		stopRecoveryRoutine: make(chan interface{}),
	}

	done := make(chan struct{})
	go func() {
		ms.recoveryLoop()
		close(done)
	}()

	close(ms.workerPanic)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		close(ms.stopRecoveryRoutine)
		t.Fatal("recoveryLoop kept running after workerPanic channel was closed")
	}
}
