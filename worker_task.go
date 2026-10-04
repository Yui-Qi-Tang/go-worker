package worker

import (
	"context"
	"go.uber.org/zap"
)

type taskRequest struct {
	Task
	result chan Result
}

func newTaskRequest(task Task) *taskRequest {
	return &taskRequest{Task: task, result: make(chan Result, 1)}
}

func (w *Worker) executeTask(task Task, result *Result) {
	if isNilTask(task) {
		w.logger.Error(workerErrNil, zap.String("worker", w.Name))
		result.Err = ErrWorkerNilTask
		return
	}
	w.logger.Info(workerEventReceived, zap.String("worker", w.Name), zap.String("task_name", readTaskID(task, result)))
	result.Phase = PhaseInit
	if err := task.Init(); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskInit, err, workerErrInit)
		return
	}
	result.Phase = PhaseRun
	if err := task.Run(); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskRun, err, workerErrRun)
		return
	}
	result.Phase = PhaseDone
	if err := task.Done(); err != nil {
		w.taskFailure(task, result, ErrWorkerTaskDone, err, workerErrDone)
		return
	}
	w.logger.Info(workerEventDone, zap.String("worker", w.Name), zap.String("task_id", readTaskID(task, result)))
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
	w.logger.Error(event, zap.String("worker", w.Name), zap.String("task_name", readTaskID(task, result)), zap.Any("reason", cause))
}

func (w *Worker) waitStatus() string { return <-w.status }

func (w *Worker) reportTaskResult(task Task, result Result) {
	if request, ok := task.(*taskRequest); ok {
		request.result <- result
		return
	}
	status := workerEventDone
	switch result.Err {
	case ErrWorkerPanic:
		status = workerPanic
	case ErrWorkerTaskInit:
		status = workerErrInit
	case ErrWorkerTaskRun:
		status = workerErrRun
	case ErrWorkerTaskDone:
		status = workerErrDone
	case ErrWorkerNilTask:
		status = workerErrNil
	case ErrWorkerStopped:
		status = workerEventQuit
	}
	select {
	case w.status <- status:
	default:
	}
}

// sendTaskContext only responds to cancellation before the channel handoff.
// Once the send wins, the caller must await its result before reusing the worker.
func (w *Worker) sendTaskContext(ctx context.Context, task Task, interrupt <-chan struct{}) (err error) {
	defer func() {
		if recover() != nil {
			w.stop()
			err = ErrWorkerStopped
		}
	}()
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
	case <-w.Quit:
		return ErrWorkerStopped
	case w.Task <- task:
		return nil
	}
}

func (w *Worker) prepareTask(ctx context.Context, task Task, interrupt <-chan struct{}) (*taskRequest, error) {
	if err := w.readyError(); err != nil {
		return nil, err
	}
	if isNilTask(task) {
		return nil, ErrWorkerNilTask
	}
	request := newTaskRequest(task)
	if err := w.sendTaskContext(ctx, request, interrupt); err != nil {
		return nil, err
	}
	return request, nil
}

// Do processes a task and preserves the package's phase sentinel errors.
func (w *Worker) Do(task Task) error { return w.DoResult(context.Background(), task).Err }

// DoContext cancels waiting to hand a task to the worker. After handoff it waits
// for completion, even if ctx expires. Simultaneous handoff/cancellation may let
// either operation win; a cancellation result means the task was not sent.
func (w *Worker) DoContext(ctx context.Context, task Task) error { return w.DoResult(ctx, task).Err }

// DoResult is DoContext with the original failure and panic details retained.
func (w *Worker) DoResult(ctx context.Context, task Task) Result {
	request, err := w.prepareTask(ctx, task, nil)
	if err != nil {
		return Result{Err: err}
	}
	return <-request.result
}
