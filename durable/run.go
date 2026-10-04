package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	worker "yuki-tang.github.com"
)

// Builder reconstructs a fresh Task from the immutable specification and attempt
// metadata. It runs in a Worker during Init, so the existing panic policy applies.
// A Builder should only reconstruct the Task; business effects belong to its phases.
type Builder func(Job) (worker.Task, error)

// Run dispatches through Master.Submit with at most concurrency in-flight jobs.
// Configure the Master with a positive queue capacity. Run owns neither the
// Master's lifecycle nor its Worker recovery policy. One Queue permits one Run.
//
// Cancellation stops new admission, then waits for accepted Tasks and commits
// their outcomes before returning. It never binds Tasks to ctx or interrupts
// their required work. A blocked Task can therefore delay Run's return.
func (q *Queue) Run(ctx context.Context, master *worker.Master, build Builder, concurrency int) error {
	if q == nil || q.db == nil {
		return ErrClosed
	}
	if master == nil || build == nil || concurrency < 1 {
		return ErrInvalidRunner
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	if q.running {
		q.mu.Unlock()
		return ErrRunning
	}
	q.running = true
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		q.running = false
		q.mu.Unlock()
	}()
	// A previous Run may have finished its Tasks but failed to commit an outcome.
	// Its goroutines have all exited before the running flag is released.
	if err := q.recoverInterrupted(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failures := make(chan error, concurrency)
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			if err := q.process(runCtx, master, build); err != nil {
				failures <- err
				cancel()
			}
		})
	}
	wg.Wait()
	close(failures)
	var result error
	for err := range failures {
		result = errors.Join(result, err)
	}
	return errors.Join(result, ctx.Err())
}

func (q *Queue) process(ctx context.Context, master *worker.Master, build Builder) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		q.mu.Lock()
		changed := q.changed
		q.mu.Unlock()
		job, ready, err := q.claim(ctx)
		if err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				return nil
			}
			return err
		}
		if !ready {
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-ctx.Done():
			case <-changed:
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		if err := q.execute(ctx, master, build, job); err != nil {
			return err
		}
	}
}

func (q *Queue) execute(ctx context.Context, master *worker.Master, build Builder, job Job) error {
	future, err := master.Submit(ctx, &rebuiltTask{job: job, build: build})
	if err != nil {
		return q.deferDispatch(job.ID, err)
	}
	// A canceled observation is not a completed Task. Retain ownership until the
	// Task actually finishes, rather than allowing a second Run to overlap it.
	result, err := future.Wait(context.Background())
	if err != nil {
		return err
	}
	if result.Err != nil && result.Phase == "" && result.Err != worker.ErrWorkerPanic {
		// Master acceptance can precede loss of dispatch capacity or Stop. A
		// pre-execution rejection has no Task phase and spends no Task attempt.
		return q.deferDispatch(job.ID, result.Err)
	}
	now := time.Now().UTC()
	outcome := Outcome{
		Phase: result.Phase, Panicked: result.Err == worker.ErrWorkerPanic,
		PanicStack: result.PanicStack,
	}
	if result.Err != nil {
		outcome.Error = fmt.Sprint(result.Err)
	}
	if result.Cause != nil {
		outcome.Cause = diagnostic(result.Cause)
	}
	if outcome.Panicked {
		outcome.PanicValue = diagnostic(result.PanicValue)
	}
	return q.changeJob(job.ID, func(j *Job) {
		if result.Err != nil {
			j.unsuccessful(outcome, now)
			return
		}
		j.State = Succeeded
		j.UpdatedAt = now
		j.NextAttemptAt = time.Time{}
		j.LastOutcome = outcome
	})
}

func (q *Queue) deferDispatch(id string, dispatchErr error) error {
	persistErr := q.changeJob(id, func(j *Job) {
		j.State = Pending
		j.Attempts--
		j.UpdatedAt = time.Now().UTC()
	})
	return errors.Join(dispatchErr, persistErr)
}

func diagnostic(value any) string {
	// Error/String methods are caller code too. A formatter's panic or Goexit
	// must not silently terminate a processor and abandon a Running job.
	formatted := make(chan string, 1)
	go func() {
		text := "<diagnostic unavailable>"
		defer func() {
			_ = recover()
			formatted <- text
		}()
		text = fmt.Sprint(value)
	}()
	return <-formatted
}

// rebuiltTask keeps caller reconstruction within the Worker's existing failure
// boundary and uses the durable JobID to correlate logs and outcomes.
type rebuiltTask struct {
	job   Job
	build Builder
	task  worker.Task
}

func (t *rebuiltTask) ID() string { return t.job.ID }

func (t *rebuiltTask) Init() error {
	task, err := t.build(t.job)
	if err != nil {
		return err
	}
	if task == nil {
		return worker.ErrWorkerNilTask
	}
	value := reflect.ValueOf(task)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return worker.ErrWorkerNilTask
		}
	}
	t.task = task
	return task.Init()
}

func (t *rebuiltTask) Run() error  { return t.task.Run() }
func (t *rebuiltTask) Done() error { return t.task.Done() }
