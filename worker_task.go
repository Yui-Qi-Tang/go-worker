package worker

import (
	"context"
	"log/slog"
)

// taskRequest carries a task and where its result goes: future for a
// submission, completed on the worker goroutine, or result for a waiting caller.
type taskRequest struct {
	task   Task
	result chan Result
	future *Future
}

func newTaskRequest(task Task) *taskRequest {
	return &taskRequest{task: task, result: make(chan Result, 1)}
}

func (w *Worker) executeTask(ctx context.Context, task Task, result *Result) {
	// ID is read even when logging is disabled; Result.TaskID depends on it.
	// Enabled avoids building log attributes for a disabled level on every task.
	taskID := readTaskID(task, result)
	if w.logger.Enabled(ctx, slog.LevelInfo) {
		w.logger.LogAttrs(ctx, slog.LevelInfo, workerEventReceived, slog.String("worker", w.name), slog.String("task_name", taskID))
	}
	result.Phase = PhaseInit
	if err := task.Init(ctx); err != nil {
		w.taskFailure(ctx, task, result, ErrWorkerTaskInit, err, workerErrInit)
		return
	}
	result.Phase = PhaseRun
	if err := task.Run(ctx); err != nil {
		w.taskFailure(ctx, task, result, ErrWorkerTaskRun, err, workerErrRun)
		return
	}
	result.Phase = PhaseDone
	if err := task.Done(ctx); err != nil {
		w.taskFailure(ctx, task, result, ErrWorkerTaskDone, err, workerErrDone)
		return
	}
	taskID = readTaskID(task, result)
	if w.logger.Enabled(ctx, slog.LevelInfo) {
		w.logger.LogAttrs(ctx, slog.LevelInfo, workerEventDone, slog.String("worker", w.name), slog.String("task_id", taskID))
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

func (w *Worker) taskFailure(ctx context.Context, task Task, result *Result, sentinel, cause error, event string) {
	result.Err, result.Cause = sentinel, cause
	w.logger.LogAttrs(ctx, slog.LevelError, event, slog.String("worker", w.name), slog.String("task_name", readTaskID(task, result)), slog.Any("reason", cause))
}

func (r *taskRequest) reply(result Result) {
	if r.future != nil {
		r.future.complete(result)
		return
	}
	r.result <- result
}

// sendTaskContext only responds to cancellation before the channel handoff.
// Once the send wins, the caller must await its result before reusing the worker.
func (w *Worker) sendTaskContext(ctx context.Context, request *taskRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.quit:
		return ErrWorkerStopped
	case w.task <- request:
		return nil
	}
}

// prepareTask hands request to a standalone worker.
func (w *Worker) prepareTask(ctx context.Context, request *taskRequest) error {
	if err := w.readyError(); err != nil {
		return err
	}
	if isNilTask(request.task) {
		return ErrWorkerNilTask
	}
	return w.sendTaskContext(ctx, request)
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
	if err := w.prepareTask(ctx, request); err != nil {
		return Result{Err: err}
	}
	return <-request.result
}
