package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	worker "github.com/Yui-Qi-Tang/go-worker"
)

// Builder reconstructs a fresh Task from the immutable specification and attempt
// metadata. It runs in a Worker during Init with that phase's context, so the
// existing panic policy applies. A Builder should only reconstruct the Task;
// business effects belong to its phases.
type Builder func(context.Context, Job) (worker.Task, error)

// Submitter runs a Task asynchronously and returns its Future.
// *worker.Master implements it; Run needs nothing else from the pool.
type Submitter interface {
	Submit(ctx context.Context, task worker.Task) (*worker.Future, error)
}

// RunOption configures Run.
type RunOption func(*runConfig)

type runConfig struct {
	dispatchRetry bool
	retryDelay    time.Duration
}

// WithDispatchRetry keeps Run going while the Master has no running worker
// because worker replacement hit its restart limit or failed. The job stays
// pending without spending a Task attempt, and the processor waits delay
// before claiming again; the Master restores workers when its restart window
// allows. Run continues until ctx is canceled or another error occurs.
// Without this option, those errors end Run. delay must be positive.
func WithDispatchRetry(delay time.Duration) RunOption {
	return func(c *runConfig) {
		c.dispatchRetry = true
		c.retryDelay = delay
	}
}

// retryable reports whether err means only that the Master currently has no
// running worker, which it repairs on its own.
func (c runConfig) retryable(err error) bool {
	return c.dispatchRetry && (errors.Is(err, worker.ErrRecoveryExhausted) || errors.Is(err, worker.ErrRecoveryFailed))
}

// errDispatchDeferred reports a retryable dispatch rejection already persisted.
var errDispatchDeferred = errors.New("durable dispatch deferred")

// Run executes jobs through pool, usually a *worker.Master, with at most
// concurrency in-flight jobs. Configure the Master with a positive queue
// capacity. Run owns neither the pool's lifecycle nor its recovery policy.
// One Queue permits one Run.
//
// Cancellation stops new admission, then waits for accepted Tasks and commits
// their outcomes before returning. Tasks receive their Worker's context, not
// ctx, so canceling Run never cancels required work; Master.Stop does. A
// blocked Task can therefore delay Run's return.
func (q *Queue) Run(ctx context.Context, pool Submitter, build Builder, concurrency int, opts ...RunOption) error {
	if q == nil || q.db == nil {
		return ErrClosed
	}
	var cfg runConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	if pool == nil || build == nil || concurrency < 1 || (cfg.dispatchRetry && cfg.retryDelay <= 0) {
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
			if err := q.process(runCtx, pool, build, cfg); err != nil {
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

func (q *Queue) process(ctx context.Context, pool Submitter, build Builder, cfg runConfig) error {
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
		err = q.execute(ctx, pool, build, job, cfg)
		if errors.Is(err, errDispatchDeferred) {
			timer := time.NewTimer(cfg.retryDelay)
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
			timer.Stop()
			continue
		}
		if err != nil {
			return err
		}
	}
}

func (q *Queue) execute(ctx context.Context, pool Submitter, build Builder, job Job, cfg runConfig) error {
	future, err := pool.Submit(ctx, &rebuiltTask{job: job, build: build})
	if err != nil {
		return q.deferDispatch(job.ID, err, cfg)
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
		return q.deferDispatch(job.ID, result.Err, cfg)
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

func (q *Queue) deferDispatch(id string, dispatchErr error, cfg runConfig) error {
	retry := cfg.retryable(dispatchErr)
	now := time.Now().UTC()
	persistErr := q.changeJob(id, func(j *Job) {
		j.State = Pending
		j.Attempts--
		j.UpdatedAt = now
		if retry {
			j.NextAttemptAt = now.Add(cfg.retryDelay)
		}
	})
	if retry && persistErr == nil {
		return errDispatchDeferred
	}
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

func (t *rebuiltTask) Init(ctx context.Context) error {
	task, err := t.build(ctx, t.job)
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
	return task.Init(ctx)
}

func (t *rebuiltTask) Run(ctx context.Context) error  { return t.task.Run(ctx) }
func (t *rebuiltTask) Done(ctx context.Context) error { return t.task.Done(ctx) }
