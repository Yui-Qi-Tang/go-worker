package worker

import "testing"

func TestWorkerDoBeforeStartReturnsError(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	if got := w.Do(phaseErrorTask{id: "before-start"}); got != ErrWorkerNotStarted {
		t.Fatalf("Do() error = %v, want %v", got, ErrWorkerNotStarted)
	}
}
