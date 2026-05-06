package worker

import "testing"

func TestWorkerMethodsRejectUninitializedWorker(t *testing.T) {
	var w Worker

	if got := w.Start(); got != ErrWorkerNotInitialized {
		t.Fatalf("Start() error = %v, want %v", got, ErrWorkerNotInitialized)
	}
	if got := w.Do(phaseErrorTask{id: "uninitialized"}); got != ErrWorkerNotInitialized {
		t.Fatalf("Do() error = %v, want %v", got, ErrWorkerNotInitialized)
	}

	assertNotPanics(t, func() {
		w.Stop()
	})
}

func assertNotPanics(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("function panicked: %v", r)
		}
	}()

	fn()
}
