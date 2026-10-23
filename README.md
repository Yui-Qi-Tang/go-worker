# go-worker

A Go worker pool for tasks with an `Init → Run → Done` lifecycle: synchronous
calls, a bounded asynchronous queue, structured results, graceful shutdown,
optional replacement of workers that panic, and an optional durable job queue.
The pool runs in one process and scales between a minimum and maximum number
of workers (up to 1024) with demand.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Examples](#examples)
- [Behavior](#behavior)
- [Development](#development)
- [Versioning](#versioning)
- [License](#license)

## Install

Requires Go 1.27.3 or newer. The package name is `worker`:

```sh
go get github.com/Yui-Qi-Tang/go-worker@v1.0.0
```

```go
import worker "github.com/Yui-Qi-Tang/go-worker"
```

## Quick start

Start two workers, submit eight tasks through a queue of four, wait for the
results, and shut down:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	worker "github.com/Yui-Qi-Tang/go-worker"
)

type printTask struct{ id string }

func (t printTask) ID() string                 { return t.id }
func (t printTask) Init(context.Context) error { return nil }
func (t printTask) Run(context.Context) error {
	fmt.Println(t.id)
	return nil
}
func (t printTask) Done(context.Context) error { return nil }

func run() error {
	m, err := worker.NewMaster(worker.WithWorkers(2, 2), worker.WithQueueCapacity(4))
	if err != nil {
		return err
	}
	defer m.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	futures := make([]*worker.Future, 0, 8)
	for i := range 8 {
		future, err := m.Submit(ctx, printTask{id: fmt.Sprintf("job-%d", i)})
		if err != nil {
			return err
		}
		futures = append(futures, future)
	}
	for _, future := range futures {
		result, err := future.Wait(ctx)
		if err != nil {
			return err
		}
		if result.Err != nil {
			return fmt.Errorf("task %s: %w", result.TaskID, result.Err)
		}
	}
	return m.Shutdown(context.Background())
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

It prints `job-0` through `job-7` in any order.

## Examples

The examples use `taskFunc`, which turns a function into a `worker.Task`.

```go
type taskFunc struct {
	id  string
	run func(ctx context.Context) error
}

func (t taskFunc) ID() string                    { return t.id }
func (t taskFunc) Init(context.Context) error    { return nil }
func (t taskFunc) Run(ctx context.Context) error { return t.run(ctx) }
func (t taskFunc) Done(context.Context) error    { return nil }
```

### Size the pool for bursty load

`WithWorkers(min, max)` starts `min` workers. When tasks wait and no worker is
idle, the pool adds workers up to `max`; workers idle for the idle timeout
retire down to `min`. Equal values give a fixed pool.

```go
func newBurstyPool() (*worker.Master, error) {
	return worker.NewMaster(
		worker.WithWorkers(2, 64),              // 2 when quiet, up to 64 under load
		worker.WithIdleTimeout(30*time.Second), // shrink after 30s without work
		worker.WithQueueCapacity(256),
	)
}
```

### Run tasks one at a time on a single worker

A standalone `Worker` runs one task per call and needs no Master.

```go
func runOnOneWorker() error {
	w, err := worker.NewWorker(worker.WithName("single"))
	if err != nil {
		return err
	}
	if err := w.Start(); err != nil {
		return err
	}
	defer w.Stop()

	return w.Do(taskFunc{id: "hello", run: func(context.Context) error {
		fmt.Println("hello")
		return nil
	}})
}
```

### Find out why a task failed

`ScheduleResult` waits for a worker and returns a `Result` with the failed
phase, the original error, and panic details.

```go
var errDeclined = errors.New("card declined")

func chargeCard(m *worker.Master) error {
	result := m.ScheduleResult(context.Background(), taskFunc{
		id:  "charge-42",
		run: func(context.Context) error { return errDeclined },
	})
	switch {
	case result.Err == nil:
		return nil
	case errors.Is(result.Cause, errDeclined):
		log.Printf("task %s declined in phase %s", result.TaskID, result.Phase)
		return result.Cause
	case errors.Is(result.Err, worker.ErrWorkerPanic):
		log.Printf("task %s panicked: %v\n%s", result.TaskID, result.PanicValue, result.PanicStack)
	}
	return result.Err
}
```

### Shed load when the queue is full

`TrySubmit` never waits: it accepts the task or returns `ErrQueueFull`.

```go
func submitOrReject(m *worker.Master, task worker.Task) (*worker.Future, error) {
	future, err := m.TrySubmit(task)
	if errors.Is(err, worker.ErrQueueFull) {
		return nil, fmt.Errorf("busy, retry later: %w", err)
	}
	return future, err
}
```

### Limit how long callers wait

The `Submit` context limits only the wait for queue space, and the
`Future.Wait` context limits only this wait for the result. Neither cancels an
accepted task.

```go
func submitWithDeadlines(m *worker.Master, task worker.Task) (worker.Result, error) {
	admit, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel() // safe: the accepted task keeps running
	future, err := m.Submit(admit, task)
	if err != nil {
		return worker.Result{}, err // context.DeadlineExceeded: still no queue space
	}

	wait, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	return future.Wait(wait) // on timeout, call Wait again later for the same result
}
```

### Stop long-running work

Each phase receives its worker's context. `Shutdown` lets running tasks finish;
`Stop` cancels that context, so a task that watches it can return early.

```go
func pollUntilStopped(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // Master.Stop or Worker.Stop
		case <-ticker.C:
			// Do one unit of work.
		}
	}
}

func stopPool(m *worker.Master) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Shutdown(ctx); err == nil {
		return nil // all accepted work finished
	}
	m.Stop() // abort queued tasks and cancel running ones
	wait, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	return m.Wait(wait)
}
```

Submit the poller with `m.Submit(ctx, taskFunc{id: "poller", run: pollUntilStopped})`.

### Limit restarts of workers that panic

A worker that panics is always replaced; the failed task still gets
`ErrWorkerPanic`. `RecoveryPolicy` limits replacements across the pool, so
tasks that keep panicking cannot restart workers in a tight loop.

```go
func newResilientPool() (*worker.Master, error) {
	return worker.NewMaster(
		worker.WithWorkers(4, 4),
		worker.WithQueueCapacity(16),
		worker.WithRecoveryPolicy(worker.RecoveryPolicy{MaxRestarts: 3, Window: time.Minute}),
	)
}

func reportRecovery(m *worker.Master) {
	stats := m.RecoveryStats()
	if stats.LastFailure.Err != nil {
		log.Printf("replacement of %s failed at %s: %v",
			stats.LastFailure.WorkerID, stats.LastFailure.Stage, stats.LastFailure.Err)
	}
}
```

Over the limit, a lost worker is not replaced until the window allows it. If no
worker is left meanwhile, calls get `ErrRecoveryExhausted` (or
`ErrRecoveryFailed` if a worker could not be created) until the master restores
one on its own.

### Log and observe the pool

```go
func newLoggedPool() (*worker.Master, error) {
	// Log warnings and errors as JSON; per-task Info events are skipped.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return worker.NewMaster(worker.WithWorkers(2, 8), worker.WithQueueCapacity(8), worker.WithMasterLogger(logger))
}

func logStats(m *worker.Master) {
	s := m.Stats()
	log.Printf("workers=%d queued=%d/%d running=%d done=%d failed=%d panicked=%d",
		s.Workers, s.Queued, s.QueueCapacity, s.InFlight, s.Succeeded, s.Failed, s.Panicked)
}
```

### Keep jobs across restarts

The [`durable`](durable/README.md) package commits each job to a local file
before it runs, and rebuilds the task from stored data on every attempt.
`sendEmail` stands for your own code.

```go
func runEmailJobs(ctx context.Context, m *worker.Master) error {
	q, err := durable.Open("jobs.db")
	if err != nil {
		return err
	}
	defer q.Close()

	if _, err := q.Enqueue(ctx, durable.Spec{
		ID:      "email-1001", // stable: repeating the request finds this job
		Kind:    "send-email",
		Version: 1,
		Payload: []byte(`{"to":"user@example.com"}`),
	}); err != nil {
		return err
	}

	build := func(_ context.Context, job durable.Job) (worker.Task, error) {
		if job.Kind != "send-email" || job.Version != 1 {
			return nil, fmt.Errorf("unsupported job %s/%d", job.Kind, job.Version)
		}
		return taskFunc{id: job.ID, run: func(ctx context.Context) error {
			return sendEmail(ctx, job.Payload)
		}}, nil
	}
	// Runs until ctx is canceled; waits for workers if the pool is empty.
	return q.Run(ctx, m, build, 4, durable.WithDispatchRetry(time.Second))
}
```

## Behavior

**Task phases.** `Init`, `Run`, and `Done` run in order and stop at the first
error; return `nil` from a phase you don't need. `Done` runs only after success,
so put cleanup in `defer`. A phase error leaves the worker available; a panic or
`runtime.Goexit` stops that worker and yields `ErrWorkerPanic`.

**Context.** Each phase gets its worker's context. `Stop` cancels it; `Shutdown`
does not, so running tasks finish first. The worker also cancels it on exit. The contexts given to `Submit`,
`Schedule`, and `Do` only limit waiting before handoff; once a task is handed to
a worker, the call waits for its real result. The pool never interrupts a
phase; honoring the context is up to the task.

**Calls.**

| API | Behavior |
| --- | --- |
| `Worker.Do` / `DoContext` / `DoResult` | Run on that worker and wait for the result |
| `Master.Schedule` / `ScheduleContext` / `ScheduleResult` | Wait for a free worker, run, and wait for the result |
| `Master.Submit(ctx, task)` | Wait for queue space, then return a `*Future` |
| `Master.TrySubmit(task)` | Return a `*Future` or `ErrQueueFull` at once |
| `Future.Wait(ctx)` / `Future.Done()` | Wait for the result; any number of callers may wait |

The queue is off by default; enable it with `WithQueueCapacity(n)`, otherwise
`Submit` returns `ErrQueueDisabled`. Capacity counts tasks not yet handed to a
worker. Queued tasks start in submission order. A task must not block on
submitting to its own full pool.

**Results.** `Result.Err` holds a sentinel: `ErrWorkerTaskInit`,
`ErrWorkerTaskRun`, `ErrWorkerTaskDone`, `ErrWorkerPanic`, or a lifecycle,
admission, or context error. `Result.Cause` holds the original phase error,
`Result.Phase` the failed phase (`id`, `init`, `run`, `done`), and `TaskID` the
last ID read.

**Shutdown.**

| Method | `Master` | `Worker` |
| --- | --- | --- |
| `Stop()` | Reject new work, fail queued tasks with `ErrMasterStopped`, cancel running tasks' contexts | Stop and cancel the running task's context |
| `Shutdown(ctx)` | Reject new work, run accepted work to completion, wait for exit | Stop accepting and wait for the running task |
| `Wait(ctx)` | Wait for shutdown without starting it | Wait for exit without starting it |

A `Shutdown` deadline limits only the wait; draining continues. Call `Shutdown`
from the code that owns the pool, not from one of its tasks.

**Pool size.** The master creates its own workers. `WithWorkers(min, max)`
defaults to a fixed pool of `GOMAXPROCS` workers; `WithIdleTimeout` (default
10 seconds) applies when `min < max`. Both limits, and the queue capacity, are
at most 1024. `Workers()` reports how many run now.

**Recovery.** A worker that panics is replaced. At most 5 replacements happen in
any rolling minute across the pool unless `WithRecoveryPolicy` says otherwise;
while a panicked worker is unreplaced, workers added for load count against the
same limit. When the window allows, the master restores the pool by itself.

**Logging and stats.** Logging uses `log/slog`. `WithLogger` sets a standalone
worker's logger and `WithMasterLogger` the logger for the master's workers; both
default to `slog.Default()`. Each task logs `received-task` and `done` at Info
and failures at Error. `Stats()` counters satisfy
`Started = Succeeded + Failed + Panicked + InFlight`.

**Durable jobs.** See the [durable package](durable/README.md) and its
[design](docs/durable-storage-design.md).

## Development

```sh
go build ./...
go vet ./...
go test -race -shuffle=on -count=20 ./...
go mod tidy -diff
```

CI also vets and race-checks the [benchmark module](benchmarks/README.md). The
[API examples](example_test.go) run as tests.

| File | Responsibility |
| --- | --- |
| `worker.go`, `worker_task.go` | Worker lifecycle and task execution |
| `task.go`, `result.go` | Task contract and results |
| `master.go`, `master_queue.go` | Admission: synchronous calls and the bounded queue |
| `master_pool.go` | Worker supervision: scaling, replacement, and the drainer |
| `master_lifecycle.go` | Stop, shutdown, and waiting |
| `recovery.go`, `future.go` | Restart limits and futures |
| `stats.go` | Execution counters |
| `durable/` | Persistent jobs |

## Versioning

v1.0.0 is the first release under `github.com/Yui-Qi-Tang/go-worker` and
follows semantic versioning. Compared with unreleased checkouts under
`yuki-tang.github.com`, it:

- moves the module path;
- passes the worker's context to `Init`, `Run`, `Done`, and the durable `Builder`;
- hides `Master` and `Worker` internals and removes raw `Worker.Task` sends;
  `Worker.Name()` replaces the `Name` field;
- lets the master create and supervise its own workers: `WithWorkers(min, max)`
  and `WithIdleTimeout` replace `AddWorker`, `AddWorkers`, and
  `WakeAllWorkersUp`, and `Workers` replaces `GetWorkers`/`GetPoolSize`;
- always replaces panicked workers under a pool-wide `RecoveryPolicy`, so
  `WithWorkerRecovery` and `RecoveryWorker` are removed;
- removes the `Dispatch` alias;
- logs through `log/slog` instead of zap: `WithLogger` and `WithMasterLogger`
  take a `*slog.Logger`;
- migrates durable queue files to schema version 2 when opened.

## License

[MIT](LICENSE)
