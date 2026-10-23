package worker

import (
	"context"
	"reflect"
)

// Task is the unit of work a Worker executes as Init, Run, then Done,
// stopping at the first error.
//
// Each phase receives the executing worker's context. It is canceled when
// Worker.Stop or Master.Stop aborts work, and when the worker exits; Shutdown
// lets running work finish without canceling it. It is not derived from the
// context passed to Submit, Schedule, or Do, which only limits admission.
// The pool never interrupts a phase; honoring ctx is up to the Task.
type Task interface {
	// Init prepares the task. Return nil if there is nothing to prepare.
	Init(ctx context.Context) error
	// Run performs the work.
	Run(ctx context.Context) error
	// Done completes the task after Init and Run succeed. Return nil if
	// there is nothing to complete.
	Done(ctx context.Context) error
	// ID returns the task identity; it may be called more than once.
	ID() string
}

func isNilTask(task Task) bool {
	if task == nil {
		return true
	}

	value := reflect.ValueOf(task)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
