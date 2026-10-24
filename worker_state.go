package worker

import (
	"context"
	"time"
)

// workerState is owned by the worker goroutine; nothing else reads or writes
// it. Others change it only by sending a control message.
type workerState int

const (
	// stateServing runs tasks as they arrive.
	stateServing workerState = iota
	// stateDraining runs a pool's remaining queued tasks, then exits.
	stateDraining
	// stateAborting exits without running queued tasks.
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

// loop is the worker actor: each step waits only for what its current state
// reads, and every received task is handled according to the state at that
// moment.
func (w *Worker) loop(ctx context.Context, st *runState) {
	var idle *time.Timer
	if w.owner != nil && w.owner.scales() {
		idle = time.NewTimer(w.owner.idleTimeout)
		defer idle.Stop()
	}
	state := w.initial
	for state != stateExited {
		switch {
		case state == stateServing && w.owner != nil:
			state = w.poolServing(ctx, st, idle)
		case state == stateServing:
			state = w.serving(ctx, st)
		case state == stateDraining && w.owner != nil:
			state = w.poolDraining(ctx, st)
		default:
			// A standalone worker has nothing queued to drain, and an
			// aborting pool worker's master answers the queued tasks.
			state = stateExited
		}
	}
}

// serving waits for a standalone worker's next task or control message.
func (w *Worker) serving(ctx context.Context, st *runState) workerState {
	select {
	case c := <-w.ctrl:
		return stateServing.next(c)
	case request := <-w.task:
		return w.handle(ctx, st, stateServing, request, nil)
	}
}

// poolServing runs the next queued task, parking only when none is waiting.
func (w *Worker) poolServing(ctx context.Context, st *runState, idle *time.Timer) workerState {
	q := &w.owner.ready
	if request, remaining := q.take(); request != nil {
		if idle != nil && remaining > 0 {
			// A backlog remains: add a worker even if no new task arrives.
			// grow's error reports a pool with no workers, which cannot be
			// true while this worker runs, so it is ignored.
			_ = w.owner.grow(remaining)
		}
		return w.handle(ctx, st, stateServing, request, idle)
	}
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
	case <-q.wake:
		notIdle()
		return stateServing
	case c := <-w.ctrl:
		notIdle()
		q.unpark()
		return stateServing.next(c)
	case <-q.closed:
		notIdle()
		q.unpark()
		return stateDraining
	case <-idleC:
		notIdle()
		q.unpark()
		if w.owner.retire(w) {
			return stateExited
		}
		idle.Reset(w.owner.idleTimeout)
		return stateServing
	}
}

// poolDraining runs queued tasks and exits once the queue is closed and empty.
func (w *Worker) poolDraining(ctx context.Context, st *runState) workerState {
	q := &w.owner.ready
	if request := q.pop(); request != nil {
		return w.handle(ctx, st, stateDraining, request, nil)
	}
	select {
	case c := <-w.ctrl:
		return stateDraining.next(c)
	case <-q.closed:
		if request := q.pop(); request != nil {
			return w.handle(ctx, st, stateDraining, request, nil)
		}
		return stateExited
	}
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
