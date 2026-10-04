# go-worker

A Go worker pool for tasks with an `Init → Run → Done` lifecycle. It supports
synchronous execution, a bounded asynchronous queue, structured results,
graceful shutdown, optional worker replacement after a panic, and an optional
durable task queue.

The pool runs in one process and keeps its submission queue in memory. The
`durable` package can persist reconstructible jobs before dispatch. The pool
supports up to 256 registered workers.

## Contents

- [Requirements and import](#requirements-and-import)
- [Quick start](#quick-start)
- [Task contract](#task-contract)
- [Submission and cancellation](#submission-and-cancellation)
- [Results and errors](#results-and-errors)
- [Shutdown](#shutdown)
- [Worker recovery](#worker-recovery)
- [Durable tasks](#durable-tasks)
- [Logging and statistics](#logging-and-statistics)
- [Development](#development)

## Requirements and import

Requires **Go 1.27.1 or newer**, as declared in `go.mod`. The package name is
`worker`; the module currently uses the legacy path `yuki-tang.github.com`.
Import it as `worker "yuki-tang.github.com"`.

To use this checkout from another local module, point a `replace` directive at
it. For example, with a demo directory beside `go-worker`:

```sh
mkdir worker-demo
cd worker-demo
go mod init example.com/worker-demo
go mod edit -require=yuki-tang.github.com@v0.0.0
go mod edit -replace=yuki-tang.github.com=../go-worker
```

Here `v0.0.0` is a local dependency placeholder, not a published release version.
Save the following program as `main.go`, then run `go mod tidy` and `go run .`.

## Quick start

This example starts two workers, submits eight tasks through a queue of capacity
four, waits for their results, and shuts down the pool.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	worker "yuki-tang.github.com"
)

type printTask struct{ id string }

func (t printTask) ID() string  { return t.id }
func (t printTask) Init() error { return nil }
func (t printTask) Run() error {
	fmt.Println(t.id)
	return nil
}
func (t printTask) Done() error { return nil }

func run() error {
	m, err := worker.NewMaster(
		worker.WithQueueCapacity(4),
		worker.WithWorkerRecovery(true),
	)
	if err != nil {
		return err
	}
	defer m.Stop()

	if err := m.AddWorkers(2); err != nil {
		return err
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		return err
	}

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

	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	return m.Shutdown(shutdownCtx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

The program prints `job-0` through `job-7`; their execution order can vary.
Create workers with `AddWorkers` or `AddWorker`, then call `WakeAllWorkersUp`
before submitting work. `NewMaster` starts with an empty pool.

For a standalone worker, use `NewWorker → Start → Do/DoResult → Shutdown`.
Constructors return concrete `*Master` and `*Worker` values. See the
[runnable API examples](example_test.go) for additional usage.

## Task contract

Implement `worker.Task` in your application:

| Method | Purpose |
| --- | --- |
| `ID() string` | Return the task identity; it may be read more than once |
| `Init() error` | Prepare the task |
| `Run() error` | Execute the work |
| `Done() error` | Complete the task after `Init` and `Run` succeed |

Return `nil` from an unused phase. Execution follows `Init → Run → Done` and
stops at the first error. `Done` is a success-path phase; put unconditional
cleanup in `defer` inside the method that owns the resource.

A phase error leaves the worker available. A panic stops that worker. A task
calling `runtime.Goexit()` also produces `ErrWorkerPanic` and stops the worker.
Task methods are never forcibly interrupted by the pool. Any goroutines created
by a task need their own lifecycle and panic handling.

## Submission and cancellation

| API | Behavior |
| --- | --- |
| `Worker.Do(task)` | Wait for execution on one worker; return an error |
| `Master.Schedule(task)` / `Dispatch(task)` | Wait for an available worker and the result |
| `DoContext(ctx, task)` / `ScheduleContext(ctx, task)` | Cancel waiting before task handoff |
| `DoResult(ctx, task)` / `ScheduleResult(ctx, task)` | Same handoff rules, returning a `Result` |
| `Master.Submit(ctx, task)` | Wait for queue space; return a `*Future` after admission |
| `Master.TrySubmit(task)` | Return a future or immediately reject a full queue |
| `Future.Wait(ctx)` | Wait for the accepted task's result |
| `Future.Done()` | Receive a channel that closes when the result is ready |

**Synchronous calls:** context cancellation applies before handoff. After a task
is handed to a worker, the call waits for its real result even if the context
expires. If cancellation and handoff are both ready, either may win; a returned
context cancellation error means the task was not handed off. Sequential
`Schedule` calls execute sequentially; use concurrent callers or `Submit` to
utilize multiple workers.

**Asynchronous calls:** enable the queue with `WithQueueCapacity(n)` for `n > 0`.
The default capacity is zero, which disables submission with `ErrQueueDisabled`;
a negative capacity returns `ErrInvalidQueueCapacity`. `TrySubmit` returns
`ErrQueueFull` when all pending slots are occupied. Capacity includes the queue
head awaiting handoff, and excludes running tasks.

`Submit`'s context controls admission only. Once accepted, the task runs
independently of that context. Canceling `Future.Wait` cancels that wait only;
you can wait again or have multiple callers observe the same future. The task
error is in `Result.Err`; the second return value of `Wait` describes a wait error.

Queued tasks are handed off in admission order; completion order can differ.
Synchronous calls bypass the queue and have no fairness ordering with queued
work. Blocking submission to the same saturated pool from one of its tasks can
cause circular waiting; use `TrySubmit`, a deadline, or submit from outside.

## Results and errors

`Do` and `Schedule` return task, lifecycle, or admission errors.
`DoResult` and `ScheduleResult` also preserve failure details:

| `Result` field | Meaning |
| --- | --- |
| `TaskID` | Last identity successfully returned by `ID()` |
| `Phase` | Failed method: `id`, `init`, `run`, or `done`; empty on success or pre-execution rejection |
| `Err` | Task sentinel, lifecycle/admission error, or context error |
| `Cause` | Original phase error, or an error-valued panic |
| `PanicValue`, `PanicStack` | Details of an abnormal worker exit |

For an initialized master and a task, inspect the result inside your application:

```go
result := m.ScheduleResult(ctx, task)
if result.Err != nil {
	log.Printf("task=%s phase=%s err=%v cause=%v",
		result.TaskID, result.Phase, result.Err, result.Cause)
}
```

Phase errors use `ErrWorkerTaskInit`, `ErrWorkerTaskRun`, or
`ErrWorkerTaskDone`; direct sentinel comparison remains supported. Use
`errors.Is/As` on `Result.Cause` when you need the original error. `Result` is a
value snapshot; referenced error and panic payloads remain caller-owned.

## Shutdown

| Method | `Master` | `Worker` |
| --- | --- | --- |
| `Stop()` | Reject new work and stop queued work; return immediately | Signal stop; return immediately |
| `Shutdown(ctx)` | Reject new work, drain accepted work to terminal results, and wait for internal goroutines | Signal stop and wait for the active task and worker exit |
| `Wait(ctx)` | Wait for shutdown completion without initiating it | Wait for exit without initiating it |

`Stop` is idempotent. Queued tasks aborted by `Master.Stop` receive
`ErrMasterStopped`; running tasks finish with their own result. Graceful shutdown
also rejects synchronous callers that are still waiting for handoff.
Only tasks that have begun execution are allowed to finish; a handed-off task
that loses a race with `Stop` can still be rejected with `ErrMasterStopped`.
Draining resolves accepted work to a result: losing the pool can fail queued
tasks before execution, as described under [worker recovery](#worker-recovery).

A shutdown deadline limits the wait, not the task's execution. Background draining
continues after a timeout; call `Wait` again, or `Stop` to abort queued work.
Use a separate shutdown context, as in the quick start, so an expired submission
context does not immediately expire the shutdown wait.

Call `Shutdown` from the component that owns the pool. A task waiting for its own
pool's shutdown would wait for itself.

## Worker recovery

Recovery is disabled by default. Enable it with `WithWorkerRecovery(true)` to
replace a panicked worker for subsequent tasks. Recovery replaces execution
capacity; the failed task receives `ErrWorkerPanic` and is not retried, and any
side effects it already performed are not undone.

The default budget is **5 replacement attempts per worker slot in a rolling
minute**. To customize it inside a function that returns an error:

```go
m, err := worker.NewMaster(
	worker.WithWorkerRecovery(true),
	worker.WithRecoveryPolicy(worker.RecoveryPolicy{
		MaxRestarts: 3,
		Window:      time.Minute,
	}),
)
if err != nil {
	return err
}
defer m.Stop()
```

Both policy fields must be positive (`ErrInvalidRecoveryPolicy` otherwise).
Setting a policy alone does not enable recovery. The budget follows the
registered worker identity across replacement instances; successful tasks or a
successful `Start` do not reset it. Attempts at or before the rolling window's
start expire from the count.

An exhausted budget or failed replacement removes that slot; other workers keep
serving tasks. If that failure removes the last slot, pending and new work receive:

| Condition | Error |
| --- | --- |
| Restart budget exhausted | `ErrRecoveryExhausted` |
| Replacement construction or startup failed | `ErrRecoveryFailed` |
| Empty pool without a recovery failure | `ErrMasterWorkerPoolIsEmpty` |

Exhausted/failed slots do not revive automatically when the window expires.
Create new worker instances with `AddWorkers` or `NewWorker` plus `AddWorker`,
then start them with `WakeAllWorkersUp` to repair capacity and begin fresh budgets.

`Master.RecoveryStats()` returns a consistent snapshot:

| Field | Meaning |
| --- | --- |
| `Attempts` | Actual replacement attempts |
| `Started` | Replacements successfully started and registered; not a continuing health guarantee |
| `Failed` | Failed replacement attempts |
| `Exhausted` | Attempts denied by the restart limit |
| `LastFailure` | Latest failure's worker/instance IDs, stage, original error, and timestamp |

`Attempts = Started + Failed`; denied attempts increment only `Exhausted`.
`LastFailure` remains available after a repair. Its stages are `create`, `start`,
`capacity`, and `limit`; inspect `LastFailure.Err` with `errors.Is/As`.

## Durable tasks

The optional [`durable` package](durable/README.md) persists JobID, task kind,
data version, payload, retry policy, and outcome in a local bbolt database.
`Queue.Enqueue` commits acceptance independently of Master availability;
`Queue.Run` rebuilds a fresh Task through the application's Builder and dispatches
it with the existing Master. Master/Worker state remains in memory.

Task phases must finish all required work before returning success. Only a
committed completion record marks a job succeeded. Panicked/interrupted work can
have unknown external effects; automatic retries require explicit application
idempotency or deduplication and a separate Task attempt budget. Worker replacement
keeps its existing policy. Tasks gain no context binding.

This first version supports one database owner and one runner, with bounded
attempts rather than exactly-once effects. See the [durable contracts and usage](durable/README.md)
and [runnable example](durable/example_test.go).

## Logging and statistics

`WithLogger(logger)` configures one worker. `WithMasterLogger(logger)` configures
workers created by `AddWorkers`; manually registered workers retain their logger.
Replacements inherit the original logger. Explicit nil loggers return
`ErrInvalidLogger`; callers own the final `Sync` of injected loggers.

Panic results are published before diagnostic logging, and a second panic from
that diagnostic logger is contained. An indefinitely blocking logger can still
delay worker exit and shutdown; injected loggers should return promptly.

`Worker.Stats()` and `Master.Stats()` expose `Started`, `Succeeded`, `Failed`,
`Panicked`, and `InFlight`. Each execution-counter snapshot satisfies:

```text
Started = Succeeded + Failed + Panicked + InFlight
```

Pre-execution rejections do not count. Master counters include direct tasks
started after registration and survive worker replacement. Master snapshots also
include worker count, pending queue count, capacity, and shutdown state; those
fields and execution counters are sampled separately, not as a global atomic view.

Use a worker in one master at a time. Configure exported fields before work
starts; do not mutate channels, names, or pool membership concurrently with the
library. Direct `Worker.Task` sends bypass master admission and queue capacity;
use the method APIs when those guarantees are required.

## Development

CI reads the Go version from `go.mod` and runs build, vet, and race tests:

```sh
go build ./...
go vet ./...
go test -race ./...
```

For repeated scheduling checks, run `go test -race -shuffle=on -count=20 ./...`.
See the [worker pool benchmarks](benchmarks/README.md) for the separate comparison
module and its fixed workload protocol.
See the [validation record](docs/feature-evaluation.md),
[API examples](example_test.go), and [CI workflow](.github/workflows/deploy.yml).

| File | Responsibility |
| --- | --- |
| `worker.go`, `worker_task.go` | Worker lifecycle, task execution, and per-call results |
| `task.go`, `result.go` | Task contract and structured outcomes |
| `master.go`, `master_pool.go` | Synchronous dispatch and pool membership |
| `master_queue.go`, `future.go` | Bounded submission and reusable outcomes |
| `master_lifecycle.go` | Admission shutdown, draining, and waiting |
| `master_recovery.go`, `recovery.go` | Replacement workers, restart limits, and failure snapshots |
| `stats.go` | Execution counters and snapshots |
| `durable/` | Persisted job specifications, reconstruction, outcomes, and safe bounded retries |

The local codebase-memory-mcp graph lives in `.codebase-memory/`, which is ignored
by Git. Refresh it after substantial source changes.

The Go upgrade preserves existing public method signatures and task-phase
sentinels. Errors use standard-library `errors` and `fmt.Errorf`; the previous
`pkg/errors` stack formatting and `Cause()` API are not retained. UUID and Zap
versions remain unchanged.
