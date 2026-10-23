package worker

import (
	"context"
	"time"
)

// workerState is owned by the worker goroutine; nothing else reads or writes
// it. Others change it only by sending a control message.
type workerState int

const (
	// stateServing runs tasks from the inbox and the backlog.
	stateServing workerState = iota
	// stateDraining runs the remaining backlog, then exits.
	stateDraining
	// stateAborting answers the remaining backlog without running it, then exits.
	stateAborting
	stateExited
)

// control is a message that moves a worker toward exit.
type control int

const (
	controlDrain control = iota
	controlAbort
)

// next returns the state after receiving c. Abort wins over drain, and a
// worker never moves back to serving.
func (s workerState) next(c control) workerState {
	switch {
	case c == controlAbort && s != stateExited:
		return stateAborting
	case c == controlDrain && s == stateServing:
		return stateDraining
	default:
		return s
	}
}

// loop is the worker actor: each step selects only on the channels its
// current state reads, and every received task is handled according to the
// state at that moment.
func (w *Worker) loop(ctx context.Context, st *runState) {
	var idle *time.Timer
	if w.owner != nil && w.owner.scales() {
		idle = time.NewTimer(w.owner.idleTimeout)
		defer idle.Stop()
	}
	state := w.initial
	for state != stateExited {
		switch state {
		case stateServing:
			state = w.serving(ctx, st, idle)
		case stateDraining:
			state = w.draining(ctx, st)
		case stateAborting:
			state = w.aborting()
		}
	}
}

func (w *Worker) serving(ctx context.Context, st *runState, idle *time.Timer) workerState {
	// Only a scaling pool counts idle workers; the count must drop before
	// the worker runs a task or retires.
	var idleC <-chan time.Time
	if idle != nil {
		idleC = idle.C
		w.owner.idle.Add(1)
	}
	notIdle := func() {
		if idle != nil {
			w.owner.idle.Add(-1)
		}
	}
	select {
	case c := <-w.ctrl:
		notIdle()
		return stateServing.next(c)
	case request := <-w.inbox:
		notIdle()
		return w.handle(ctx, st, stateServing, request, idle)
	case request, ok := <-w.backlog:
		notIdle()
		if !ok {
			return stateExited
		}
		return w.handle(ctx, st, stateServing, request, idle)
	case <-idleC:
		notIdle()
		if w.owner.retire(w) {
			return stateExited
		}
		idle.Reset(w.owner.idleTimeout)
		return stateServing
	}
}

func (w *Worker) draining(ctx context.Context, st *runState) workerState {
	if w.backlog == nil {
		return stateExited
	}
	select {
	case c := <-w.ctrl:
		return stateDraining.next(c)
	case request, ok := <-w.backlog:
		if !ok {
			return stateExited
		}
		return w.handle(ctx, st, stateDraining, request, nil)
	}
}

// aborting answers what is left in the backlog. The master closes the
// backlog once no producer can send, so the loop ends.
func (w *Worker) aborting() workerState {
	if w.backlog == nil {
		return stateExited
	}
	request, ok := <-w.backlog
	if !ok {
		return stateExited
	}
	request.reply(Result{Err: w.rejection()})
	return stateAborting
}

// handle runs or rejects a received task. A control message may have arrived
// together with the task, so the worker applies it first and decides from the
// resulting state.
func (w *Worker) handle(ctx context.Context, st *runState, state workerState, request *taskRequest, idle *time.Timer) workerState {
	select {
	case c := <-w.ctrl:
		state = state.next(c)
	default:
	}
	if !w.runs(state) {
		request.reply(Result{Err: w.rejection()})
		return state
	}
	if idle != nil && len(w.backlog) > 0 {
		// A backlog remains: add a worker even if no new task arrives.
		// grow's error reports a pool with no workers, which cannot be
		// true while this worker runs, so it is ignored.
		_ = w.owner.grow(len(w.backlog))
	}
	w.execute(ctx, st, request)
	if idle != nil {
		idle.Reset(w.owner.idleTimeout)
	}
	return state
}

// runs reports whether a task received in state is executed. A pool worker
// finishes accepted work while draining; a standalone worker stops taking
// tasks once it is asked to stop.
func (w *Worker) runs(state workerState) bool {
	switch state {
	case stateServing:
		return true
	case stateDraining:
		return w.owner != nil
	default:
		return false
	}
}

func (w *Worker) rejection() error {
	if w.owner != nil {
		return ErrMasterStopped
	}
	return ErrWorkerStopped
}
