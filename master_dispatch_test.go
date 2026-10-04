package worker

import (
	"errors"
	"testing"
)

type phaseErrorTask struct {
	id      string
	initErr error
	runErr  error
	doneErr error
}

func (t phaseErrorTask) Init() error { return t.initErr }
func (t phaseErrorTask) Run() error  { return t.runErr }
func (t phaseErrorTask) Done() error { return t.doneErr }
func (t phaseErrorTask) ID() string  { return t.id }

func TestMasterDispatchReturnsTaskErrors(t *testing.T) {
	ms, err := NewMaster()
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

	testcases := []struct {
		name string
		task phaseErrorTask
		want error
	}{
		{
			name: "success",
			task: phaseErrorTask{id: "success"},
			want: nil,
		},
		{
			name: "init error",
			task: phaseErrorTask{id: "init-error", initErr: errors.New("init failed")},
			want: ErrWorkerTaskInit,
		},
		{
			name: "run error",
			task: phaseErrorTask{id: "run-error", runErr: errors.New("run failed")},
			want: ErrWorkerTaskRun,
		},
		{
			name: "done error",
			task: phaseErrorTask{id: "done-error", doneErr: errors.New("done failed")},
			want: ErrWorkerTaskDone,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			if got := ms.Dispatch(testcase.task); got != testcase.want {
				t.Fatalf("Dispatch() error = %v, want %v", got, testcase.want)
			}
		})
	}
}

func TestMasterDispatchNilTaskReturnsTaskErrorBeforePoolState(t *testing.T) {
	ms, err := NewMaster()
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Stop()

	if got := ms.Dispatch(nil); got != ErrWorkerNilTask {
		t.Fatalf("Dispatch(nil) error = %v, want %v", got, ErrWorkerNilTask)
	}

	var typedNil *phaseErrorTask
	if got := ms.Dispatch(typedNil); got != ErrWorkerNilTask {
		t.Fatalf("Dispatch(typed nil) error = %v, want %v", got, ErrWorkerNilTask)
	}
}
