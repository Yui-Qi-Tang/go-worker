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

func TestMasterRejectsDuplicateWorkerNamesWithoutPoolMutation(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	first, err := NewWorker(WithName("duplicate-worker"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorker(WithName("duplicate-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()

	if got := ms.AddWorker(first); got != nil {
		t.Fatalf("AddWorker(first) error = %v, want nil", got)
	}
	if got := ms.AddWorker(second); got != ErrMasterDuplicateWorkerName {
		t.Fatalf("AddWorker(duplicate) error = %v, want %v", got, ErrMasterDuplicateWorkerName)
	}
	if got := ms.GetWorkers(); got != 1 {
		t.Fatalf("workers after rejected duplicate = %d, want 1", got)
	}
}

func TestMasterRejectsDuplicateRegisteredIdentityAfterNameMutation(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	first, err := NewWorker(WithName("registered-worker"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewWorker(WithName("registered-worker"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Stop()

	if got := ms.AddWorker(first); got != nil {
		t.Fatalf("AddWorker(first) error = %v, want nil", got)
	}

	first.Name = "renamed-worker"

	if got := ms.AddWorker(second); got != ErrMasterDuplicateWorkerName {
		t.Fatalf("AddWorker(duplicate identity after rename) error = %v, want %v", got, ErrMasterDuplicateWorkerName)
	}
	if got := ms.GetWorkers(); got != 1 {
		t.Fatalf("workers after rejected duplicate identity = %d, want 1", got)
	}
}
