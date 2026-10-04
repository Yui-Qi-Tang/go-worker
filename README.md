# go-worker

A worker pool for tasks with an `Init → Run → Done` lifecycle and optional
worker replacement after a panic.

## Development

Requires **Go 1.27.1 or newer**. CI reads the Go version from `go.mod`.

```sh
go build ./...
go vet ./...
go test -race ./...
```

The module path remains `yuki-tang.github.com`; import it with the package name
`worker`:

```go
import worker "yuki-tang.github.com"
```

## Single worker

Given a `task` that implements `worker.Task`:

```go
w, err := worker.NewWorker(worker.WithName("my-worker"))
if err != nil {
    return err
}
defer w.Stop()

if err := w.Start(); err != nil {
    return err
}
if err := w.Do(task); err != nil {
    return err
}
```

`Do` waits for the task result. A phase error skips the remaining phases and
leaves the worker available. A panic stops that worker and returns
`ErrWorkerPanic`. `Done` runs only after both `Init` and `Run` succeed; it is not
an unconditional cleanup callback.

## Worker pool

Given a `tasks` slice whose elements implement `worker.Task`:

```go
m, err := worker.NewMaster(worker.WithWorkerRecovery(true))
if err != nil {
    return err
}
defer m.Stop()

if err := m.AddWorkers(4); err != nil {
    return err
}
if err := m.WakeAllWorkersUp(); err != nil {
    return err
}

for _, task := range tasks {
    if err := m.Schedule(task); err != nil {
        return err
    }
}
```

`Schedule` and its alias `Dispatch` wait for an available worker and then the
task result. The loop above is sequential; concurrent callers can use multiple
workers. The pool allows up to 256 registered workers.

Recovery replaces a panicked worker for subsequent tasks. It does **not** retry
the failed task. `Stop` is idempotent and signals shutdown without waiting;
`Shutdown(ctx)` stops admission, drains accepted asynchronous tasks, and waits
for workers and internal goroutines. A timeout only limits the wait: Task methods
are never forcibly interrupted.

## Recovery limits and failures

Enabling `WithWorkerRecovery(true)` permits up to **5 replacement attempts per
worker slot in a rolling minute**. Use `WithRecoveryPolicy` to change the limit:

```go
m, err := worker.NewMaster(
    worker.WithWorkerRecovery(true),
    worker.WithRecoveryPolicy(worker.RecoveryPolicy{
        MaxRestarts: 3,
        Window: time.Minute,
    }),
)
```

Both policy fields must be positive; invalid values return
`ErrInvalidRecoveryPolicy`. Setting the policy alone does not enable recovery.
The budget follows the registered worker identity across replacement instances.
Successful tasks and a successful `Start` do not reset it. Attempts at or before
the start of the rolling window expire from the count.

When the limit is reached, that slot is removed. A replacement construction or
startup failure also removes the slot. Remaining workers continue serving tasks.
If this failure leaves no slots, pending and new work receive `ErrRecoveryExhausted` or
`ErrRecoveryFailed`, respectively. The original panicked task still receives
`ErrWorkerPanic`. Exhausted/failed slots do not revive automatically when the
window expires: add and start a new worker to repair capacity and start its budget.

`m.RecoveryStats()` exposes cumulative `Attempts`, `Started`, `Failed`, and
`Exhausted` counters. `Attempts = Started + Failed`; denied attempts increment
only `Exhausted`. `Started` means a replacement was started and registered, not
that it remains healthy. `LastFailure` retains worker identity, instance recovery
identity, failure stage, original error, and timestamp, including after a repair.
For example, inspect `stats.LastFailure.Err` with `errors.Is/As`. Snapshots copy
the fields; callers must treat referenced error values as immutable.

Recovery notifications and worker exit observers use the same instance identity
to prevent duplicate replacement. Exit observation supplies a fallback when a
notification was dropped. Panic results are published before diagnostic logging,
and a second panic from that diagnostic logger is contained. An indefinitely
blocking logger can still delay worker exit and shutdown; injected loggers should
return promptly. Recovery does not undo task side effects or retry the task.

## Cancellation and detailed results

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

result := m.ScheduleResult(ctx, task)
if result.Err != nil {
    // Err is the existing sentinel; Cause is the original task error.
    log.Printf("task=%s phase=%s err=%v cause=%v",
        result.TaskID, result.Phase, result.Err, result.Cause)
}
```

`ScheduleContext(ctx, task)` and `Worker.DoContext(ctx, task)` return only the
sentinel error. `ScheduleResult` and `Worker.DoResult` also return the task ID,
failing phase, original error, panic value, and panic stack. Context cancellation
applies **before handoff**. After handoff these calls wait for the real result,
so a worker is never reused while its previous task is still running. If handoff
and cancellation happen concurrently, either can win.

## Bounded asynchronous queue

Configure `NewMaster` with `WithQueueCapacity(n)` to enable asynchronous work.
Start the workers before submitting. Given that configured master:

```go
future, err := m.Submit(ctx, task) // waits for queue capacity
if err != nil {
    return err
}
result, err := future.Wait(ctx) // only cancels this wait, not the accepted task
if err != nil {
    return err
}
if result.Err != nil {
    return result.Err
}
```

`TrySubmit(task)` returns `ErrQueueFull` immediately when the queue is full.
The capacity bounds queued tasks, including a task being offered to a worker;
running tasks use worker slots. `Submit`'s context controls admission only.
After acceptance, the task is independent of that context. A `Future` supports
multiple waiters and repeated reads, and `Done()` closes when its result is ready.

Queued tasks are handed off in admission order; completion order can differ.
Synchronous `Schedule` bypasses the task queue and has no fairness ordering with
asynchronous submissions. `Stop` rejects queued tasks with `ErrMasterStopped`;
`Shutdown` drains them. If the pool loses all workers, pending work receives a
terminal pool error instead of waiting indefinitely.

Call `Shutdown` from the component that owns the pool. A task that waits for its
own pool to shut down would wait for itself. Similarly, blocking submission to
the same saturated pool can deadlock; use `TrySubmit`, a deadline, or submit from
outside the pool.

## Logging and statistics

`WithLogger(logger)` injects a Zap logger into a worker;
`WithMasterLogger(logger)` configures workers created by `AddWorkers`.
Manually added workers retain their own logger, and replacements inherit it.
The caller owns the final `Sync` of injected loggers. Nil loggers are rejected.

`Worker.Stats()` and `Master.Stats()` return value snapshots with `Started`,
`Succeeded`, `Failed`, `Panicked`, and `InFlight` counters. Each snapshot obeys:

```text
Started = Succeeded + Failed + Panicked + InFlight
```

Pre-execution rejections do not increment these counters. Master counters include
direct tasks started after worker registration and survive worker replacement.
Master snapshots also expose registered worker count, pending queue count,
capacity, and shutdown state. Execution counters and pool/queue fields are sampled
separately; they are not a global atomic snapshot.

Use a worker in one master at a time. Configure exported fields before work starts;
do not mutate channels, names, or pool membership concurrently with the library.
The direct `Worker.Task` channel remains supported, but it bypasses master admission
and the bounded asynchronous queue. Prefer methods when you need those guarantees.

## Code layout

| File | Responsibility |
| --- | --- |
| `worker.go` | Worker configuration, lifecycle, panic recovery, and waiting |
| `worker_task.go` | Task execution, cancellation before handoff, per-call results |
| `task.go`, `result.go` | Task contract and structured outcomes |
| `master.go`, `master_pool.go` | Synchronous dispatch and pool membership |
| `master_recovery.go`, `recovery.go` | Replacement workers, restart limits, failure snapshots |
| `master_lifecycle.go` | Admission shutdown, draining, and waiting |
| `master_queue.go`, `future.go` | Bounded submission and reusable outcomes |
| `stats.go` | Execution counters and snapshots |

The local codebase-memory-mcp graph is stored in `.codebase-memory/`, which is
ignored by Git. Refresh it after substantial source changes.

The Go upgrade keeps existing public method signatures and task-phase sentinel
comparisons. Worker errors use standard-library `errors` and `fmt.Errorf`; the old
`pkg/errors` stack formatting and `Cause` behavior are not retained. UUID and Zap
versions are unchanged. `panic(nil)` and a task calling `runtime.Goexit()` produce
`ErrWorkerPanic` and stop that worker, so accepted requests still obtain a result.

See [feature status and validation](docs/feature-evaluation.md) and the runnable
[examples](example_test.go).
