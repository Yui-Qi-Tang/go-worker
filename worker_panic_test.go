package worker

import (
	"testing"
	"time"
)

func TestWorkerPanicClosesQuit(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	if got := w.Do(panicErr); got != ErrWorkerPanic {
		t.Fatalf("Do() error = %v, want %v", got, ErrWorkerPanic)
	}

	select {
	case <-w.Quit:
	case <-time.After(time.Second):
		t.Fatal("worker Quit was not closed after panic")
	}

	w.Stop()
}

func TestWorkerDoAfterPanicReturnsStopped(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	if got := w.Do(panicErr); got != ErrWorkerPanic {
		t.Fatalf("panic Do() error = %v, want %v", got, ErrWorkerPanic)
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Do(normal)
	}()

	select {
	case got := <-done:
		if got != ErrWorkerStopped {
			t.Fatalf("Do() after panic error = %v, want %v", got, ErrWorkerStopped)
		}
	case <-time.After(time.Second):
		t.Fatal("Do() after panic deadlocked")
	}
}
