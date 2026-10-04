# Durable tasks

The `durable` package stores reconstructible jobs in a local **bbolt v1.5.0**
database and dispatches them through the existing `worker.Master`. It requires
**Go 1.27.1 or newer**. Import `"yuki-tang.github.com/durable"` alongside
`worker "yuki-tang.github.com"`.

See the [storage architecture review](../docs/durable-storage-design.md) for the
transaction boundaries, recovery rules, backend coupling, and open design decisions.

Only jobs are persisted. Master/Worker state, live Task objects, goroutines, and
contexts remain in memory. Worker panic removal and replacement keep their
existing behavior. Each Task remains responsible for its own work and has no
new context method or parent-context binding.

## Acceptance and reconstruction

`Open(path)` creates or opens a database; its parent directory must already
exist. `Enqueue` commits a pending job without needing a Master:

```go
q, err := durable.Open("jobs.db")
if err != nil {
	return err
}
// The owner must cancel and await any Run before calling q.Close().
job, err := q.Enqueue(ctx, durable.Spec{
	ID:      "payment-request-123",
	Kind:    "capture-payment",
	Version: 1,
	Payload: payload,
})
if err != nil {
	return err
}
// job.ID is now durably accepted, even if no Master can dispatch it yet.
```

The stored specification contains a **JobID**, task kind, data version, opaque
payload bytes, and immutable retry policy. It does not serialize `Task`. A
caller-provided `Builder` handles kind/version validation and payload decoding:

```go
build := func(job durable.Job) (worker.Task, error) {
	if job.Kind != "capture-payment" || job.Version != 1 {
		return nil, fmt.Errorf("unsupported job: %s/%d", job.Kind, job.Version)
	}
	return rebuildPaymentTask(job.ID, job.Payload)
}
// m must have started workers and WithQueueCapacity(n), where n > 0.
err = q.Run(runnerCtx, m, build, 4)
```

The application supplies `rebuildPaymentTask` and its business deduplication.
See the [complete runnable example](example_test.go), which reconstructs a file
writing Task and waits for its persisted success.

Each execution attempt calls Builder again to create a fresh Task. Builder runs
inside the Worker's `Init` phase, so reconstruction errors, panics, and Goexit
follow the existing Task failure boundary. Builder should reconstruct data;
required business effects belong in Task phases. The wrapper uses **JobID** for
Worker logs and result correlation; it does not call the rebuilt Task's `ID()`.
The application can still carry a separate domain identity in the payload.

`Enqueue` returns success only after a synchronous database commit. It checks
context cancellation before writing and does not report cancellation after a
successful commit. Contexts cannot interrupt an ongoing database commit.

Use a stable caller-generated JobID when requests can be repeated after a lost
reply. Re-enqueueing the same ID and exact normalized specification returns the
existing record, even if it has already completed. Changed kind, version,
payload bytes, or retry policy returns `ErrConflict`. An empty ID generates a
new random ID each time and cannot deduplicate repeated requests. Payload JSON
is opaque: equivalent JSON with different bytes is a different specification.

A lost reply or commit I/O error can leave the caller uncertain whether a write
committed. Inspect the original JobID after reopening, then repeat the original
specification if necessary. Do not generate a different ID to resolve an
uncertain acceptance.

## Persisted states and completion

| State | Meaning |
| --- | --- |
| `pending` | Waiting for dispatch or a permitted retry |
| `running` | An attempt has been durably claimed; it may still be waiting for Master admission or handoff |
| `succeeded` | Init, Run, and Done returned successfully, and that outcome committed |
| `failed` | A Task phase returned an error and no permitted automatic retry remains |
| `unknown` | Panic, Goexit, or interruption left the outcome uncertain and no permitted automatic retry remains |

`Attempts` includes the initial attempt. Claims increment it conservatively
before dispatch. A known pre-execution rejection, including a queued job aborted
by Master.Stop, returns the job to pending and refunds that attempt. If the
process disappears after a claim, recovery cannot prove whether execution began;
that claim still consumes an attempt.

**Task success means all required work has completed.** Wait for any required
child goroutines and propagate their errors before returning success. The runner
cannot detect work detached into background goroutines. `Done` is a success-path
phase, not unconditional cleanup; use resource-owner defers for cleanup.

`Job(ctx, id)` returns a detached persisted snapshot. `Jobs(ctx, state)` lists
snapshots in JobID order; empty state includes all records. A failed/unknown
record does not mean external effects were rolled back. Outcome stores phase,
error/cause text, panic text/stack, and an interruption flag; it does not retain
live error values or arbitrary Task return values.

If Task execution finished but its completion commit failed, Run returns an
error. Only a committed `succeeded` record establishes durable completion. A
later Open/Run reconciles any remaining running records as interrupted. A failed
commit does not always imply the disk record stayed unchanged; inspect the
persisted state before deciding what to do.

## Recovery and independent retry budgets

Default retry policy allows **one attempt**. Panicked or interrupted work becomes
unknown; an ordinary Task error becomes failed. Neither is automatically replayed.

Enable retries only after the application makes repeated execution safe:

```go
Retry: durable.RetryPolicy{
	Safe:        true,
	MaxAttempts: 3, // Includes the first attempt, across process restarts.
	Delay:       time.Second,
},
```

`Safe` is an explicit application assertion of idempotency or deduplication;
it does not implement either. More than one attempt requires this assertion.
Use JobID or a stable business key when coordinating deduplication with external
systems. The effect and its deduplication record must have the atomicity needed
by that system. Recording a separate marker before or after an effect can leave
its own crash gap.

For safe jobs with remaining budget, phase errors, panics, and interrupted
running records become pending with a persisted retry delay. Outcome and retry
decision commit together. A successful retry replaces LastOutcome with success;
this version stores the latest outcome, not a full attempt history.

The Task budget is separate from `worker.RecoveryPolicy`. Worker replacement
restores pool capacity; the durable runner decides whether to rebuild a Task.
If replacement capacity is exhausted, a job not yet executed stays pending
without spending another Task attempt. Restore capacity and call Run again.
Recovery starts the next Task from Init; it cannot resume the middle of Run.

Retries provide bounded repeated attempts, **not exactly-once external effects**.
An interrupted attempt may already have performed all or part of its work. The
package retains unknown jobs for application investigation and does not expose
an automatic override of their immutable retry policy.

## Runner ownership and shutdown

A database file has one owner at a time. Open takes bbolt's exclusive OS file
lock and waits at most one second; another owner gets a lock timeout. Each Queue
allows one active Run. Multiple processes coordinating a shared queue are outside
this package's scope.

Run permits at most its `concurrency` argument of claimed/in-flight jobs. A
positive Master queue capacity is required. Run does not create, start, stop, or
replace Master/Workers. A dispatch/storage error stops new admissions, drains
already accepted Tasks, and returns an error. Durable acceptance remains valid
when Master cannot dispatch; jobs remain pending where execution was rejected.

Canceling runnerCtx stops new admissions. The runner waits for accepted Tasks'
actual results and saves them before returning; it never passes runnerCtx into
the Task. The owner should:

1. Cancel the runner and wait for Run to return.
2. Shut down the Master it owns.
3. Close the Queue and handle any error.

Close returns `ErrRunning` while Run is active. A blocked Task or caller-supplied
callback can delay runner shutdown, just as it can delay Worker shutdown.

## Storage and verification limits

The database uses normal bbolt synchronous commits; this package does not expose
NoSync. Durability depends on the filesystem and storage device honoring sync.
See the [bbolt documentation](https://pkg.go.dev/go.etcd.io/bbolt) for transaction
and locking behavior. Do not replace or copy a live database file to create a
second owner of the same logical work.

This first version scans persisted records to find eligible work, retains
terminal records, and makes no FIFO dispatch guarantee. It is intended for
local queues with modest record counts; it has no queue compaction or retention
API. Unknown schema/corrupt records fail opening rather than resetting work.

Tests kill a child process without graceful cleanup after acceptance, during
execution, after Task completion but before completion commit, and after a
committed retry decision. They verify reopen behavior, explicit retry safety,
attempt limits, and a completed external-effect deduplication fixture. These
process-crash tests do not simulate power loss or prove deduplication for every
possible business operation.

Run the repository checks with:

```sh
go build ./...
go vet ./...
go test -race -shuffle=on ./...
```
