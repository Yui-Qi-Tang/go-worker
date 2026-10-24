package worker

import "testing"

func TestNewWorkerRejectsEmptyName(t *testing.T) {
	w, err := NewWorker(WithName(""))
	if err != ErrWorkerInvalidName {
		t.Fatalf("NewWorker(WithName(\"\")) error = %v, want %v", err, ErrWorkerInvalidName)
	}
	if w != nil {
		t.Fatalf("NewWorker(WithName(\"\")) worker = %#v, want nil", w)
	}
}
