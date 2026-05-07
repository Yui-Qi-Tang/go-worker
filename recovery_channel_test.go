package worker

import (
	"testing"
	"time"
)

func TestWorkerNotifyRecoveryIgnoresClosedRecoveryChannel(t *testing.T) {
	w, err := NewWorker(WithRecovery(true))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	close(w.Recovery)

	if panicked := didPanic(w.notifyRecovery); panicked {
		t.Fatal("notifyRecovery panicked after Recovery channel was closed")
	}
}

func TestRecoveryWorkerReturnsWhenRecoveryChannelClosed(t *testing.T) {
	ms := &Master{
		workerPanic:         make(chan string),
		stopRecoveryRoutine: make(chan interface{}),
	}

	done := make(chan struct{})
	go func() {
		ms.RecoveryWorker()
		close(done)
	}()

	close(ms.workerPanic)

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		close(ms.stopRecoveryRoutine)
		t.Fatal("RecoveryWorker kept running after workerPanic channel was closed")
	}
}

func didPanic(fn func()) (panicked bool) {
	defer func() {
		panicked = recover() != nil
	}()

	fn()
	return false
}
