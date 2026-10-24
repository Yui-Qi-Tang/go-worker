package worker

import "testing"

func TestWorkerStateTransitions(t *testing.T) {
	tests := []struct {
		from workerState
		msg  control
		want workerState
	}{
		{stateServing, controlDrain, stateDraining},
		{stateServing, controlAbort, stateAborting},
		{stateDraining, controlDrain, stateDraining},
		{stateDraining, controlAbort, stateAborting},
		{stateAborting, controlDrain, stateAborting},
		{stateAborting, controlAbort, stateAborting},
		{stateExited, controlAbort, stateExited},
	}
	for _, tt := range tests {
		if got := tt.from.next(tt.msg); got != tt.want {
			t.Errorf("%v.next(%v) = %v, want %v", tt.from, tt.msg, got, tt.want)
		}
	}
}

func TestWorkerRunsTasksByState(t *testing.T) {
	standalone := &Worker{}
	pooled := &Worker{owner: &Master{}}
	tests := []struct {
		w     *Worker
		state workerState
		want  bool
	}{
		{standalone, stateServing, true},
		{standalone, stateDraining, false},
		{standalone, stateAborting, false},
		{pooled, stateServing, true},
		{pooled, stateDraining, true},
		{pooled, stateAborting, false},
	}
	for _, tt := range tests {
		if got := tt.w.runs(tt.state); got != tt.want {
			t.Errorf("owner=%v runs(%v) = %v, want %v", tt.w.owner != nil, tt.state, got, tt.want)
		}
	}
	if standalone.rejection() != ErrWorkerStopped || pooled.rejection() != ErrMasterStopped {
		t.Fatal("rejection errors do not match the worker kind")
	}
}
