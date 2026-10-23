package worker

import (
	"context"
	"go.uber.org/zap"
)

type taskRequest struct {
	Task
	result chan Result
	// An async request completes future on the worker goroutine instead of
	// sending on result, so no goroutine has to wait for it.
	master *Master
	future *Future
}

func newTaskRequest(task Task) *taskRequest {
	return &taskRequest{Task: task, result: make(chan Result, 1)}
}

func (w *Worker) executeTask(task Task, result *Result) {
	// ID is read even when logging is disabled; Result.TaskID depends on it.
	// Check avoids building log fields for a disabled level on every task.
	taskID := readTaskID(task, result)
	if ce := w.logger.Check(zap.InfoLevel, workerEventReceived); ce != nil {
		ce.Write(zap.String("worker", w.name), zap.String("task_name", taskID))
	}
	result.Phase = PhaseInit
	if err := task.Init(w.ctx); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskInit, err, workerErrInit)
		return
	}
	result.Phase = PhaseRun
	if err := task.Run(w.ctx); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskRun, err, workerErrRun)
		return
	}
	result.Phase = PhaseDone
	if err := task.Done(w.ctx); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskDone, err, workerErrDone)
		return
	}
	taskID = readTaskID(task, result)
	if ce := w.logger.Check(zap.InfoLevel, workerEventDone); ce != nil {
		ce.Write(zap.String("worker", w.name), zap.String("task_id", taskID))
	}
	result.Phase = ""
}

func readTaskID(task Task, result *Result) string {
	phase := result.Phase
	result.Phase = PhaseID
	result.TaskID = task.ID()
	result.Phase = phase
	return result.TaskID
}

func (w *Worker) taskFailure(task Task, result *Result, sentinel, cause error, event string) {
	result.Err, result.Cause = sentinel, cause
	w.logger.Error(event, zap.String("worker", w.name), zap.String("task_name", readTaskID(task, result)), zap.Any("reason", cause))
}

func (w *Worker) reportTaskResult(request *taskRequest, result Result) {
	if request.future != nil {
		request.master.completeAsync(w, request.future, result)
		return
	}
	request.result <- result
}

// sendTaskContext only responds to cancellation before the channel handoff.
// Once the send wins, the caller must await its result before reusing the worker.
func (w *Worker) sendTaskContext(ctx context.Context, request *taskRequest, interrupt <-chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-interrupt:
		return ErrMasterStopped
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-interrupt:
		return ErrMasterStopped
	case <-w.quit:
		return ErrWorkerStopped
	case w.task <- request:
		return nil
	}
}

// prepareTask hands request to the worker. A failed handoff leaves request
// unsent, so the caller may offer it to another worker.
func (w *Worker) prepareTask(ctx context.Context, request *taskRequest, interrupt <-chan struct{}) error {
	if err := w.readyError(); err != nil {
		return err
	}
	if isNilTask(request.Task) {
		return ErrWorkerNilTask
	}
	return w.sendTaskContext(ctx, request, interrupt)
}

// Do processes a task and preserves the package's phase sentinel errors.
func (w *Worker) Do(task Task) error { return w.DoResult(context.Background(), task).Err }

// DoContext cancels waiting to hand a task to the worker. After handoff it waits
// for completion, even if ctx expires. Simultaneous handoff/cancellation may let
// either operation win; a cancellation result means the task was not sent.
func (w *Worker) DoContext(ctx context.Context, task Task) error { return w.DoResult(ctx, task).Err }

// DoResult is DoContext with the original failure and panic details retained.
func (w *Worker) DoResult(ctx context.Context, task Task) Result {
	request := newTaskRequest(task)
	if err := w.prepareTask(ctx, request, nil); err != nil {
		return Result{Err: err}
	}
	return <-request.result
}
