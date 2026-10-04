package worker

import "testing"

func TestWithRecoveryFalseAfterTrueDisablesWorkerRecovery(t *testing.T) {
	w, err := NewWorker(WithRecovery(true), WithRecovery(false))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if w.Recovery != nil {
		t.Fatal("WithRecovery(false) after WithRecovery(true) kept a recovery channel")
	}
}
