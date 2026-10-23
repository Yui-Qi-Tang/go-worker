package worker

import "testing"

func TestWithRecoveryFalseAfterTrueDisablesWorkerRecovery(t *testing.T) {
	w, err := NewWorker(withRecovery(true), withRecovery(false))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if w.recovery != nil {
		t.Fatal("withRecovery(false) after withRecovery(true) kept a recovery channel")
	}
}
