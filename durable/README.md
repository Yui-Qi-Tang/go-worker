# Durable tasks

Package `durable` persists jobs in a local bbolt database before dispatching
them through a `worker.Master`, so accepted work survives a process restart.
Import it as `"github.com/Yui-Qi-Tang/go-worker/durable"`.

A job is stored as data (ID, kind, version, payload, retry policy), never as a
`Task`. Each attempt asks your `Builder` to rebuild a fresh Task from that data.
See the [design](../docs/durable-storage-design.md) for transactions, recovery,
and trade-offs, and the [runnable example](example_test.go).

## Usage

```go
q, err := durable.Open("jobs.db") // the parent directory must exist
if err != nil {
	return err
}
defer q.Close()

// Enqueue returns only after the job is committed. Use a stable ID so a
// retried request finds the existing job instead of creating a new one.
_, err = q.Enqueue(ctx, durable.Spec{
	ID:      "payment-request-123",
	Kind:    "capture-payment",
	Version: 1,
	Payload: payload,
})
if err != nil {
	return err
}

build := func(ctx context.Context, job durable.Job) (worker.Task, error) {
	if job.Kind != "capture-payment" || job.Version != 1 {
		return nil, fmt.Errorf("unsupported job: %s/%d", job.Kind, job.Version)
	}
	return newPaymentTask(job.ID, job.Payload)
}

// m was created with WithQueueCapacity(n > 0). Run blocks until
// runnerCtx is canceled or an error stops it.
err = q.Run(runnerCtx, m, build, 4)
```

To stop: cancel `runnerCtx` and wait for `Run`, then `m.Shutdown`, then
`q.Close`. `Run` waits for accepted Tasks and saves their results before it
returns; canceling it never cancels a Task.

## Contracts

| State | Meaning |
| --- | --- |
| `pending` | Waiting for dispatch or a retry |
| `running` | Claimed; may still be waiting for the Master |
| `succeeded` | All phases succeeded and that result was committed |
| `failed` | A phase returned an error and no retry remains |
| `unknown` | A panic, Goexit, or crash left the outcome uncertain and no retry remains |

- **Return success only when the work is done.** Only a committed `succeeded`
  record means the job completed. A crash between your side effect and that
  commit can run the job again, so make effects idempotent or deduplicate them.
- **One attempt by default.** For more, set
  `Retry: durable.RetryPolicy{Safe: true, MaxAttempts: 3, Delay: time.Second}`.
  `Safe` is your promise that repeating the job is harmless; it is required
  when `MaxAttempts > 1`. Attempts survive restarts and are separate from the
  Master's worker recovery budget.
- **Rejections before execution are free.** If the Master rejects a job before
  running it, the job returns to `pending` and the attempt is refunded. `Run`
  then returns the error, unless you pass `durable.WithDispatchRetry(delay)`,
  which keeps waiting while the master has no worker because its restart
  limit was reached or a worker could not be created.
- **Same ID, same job.** Re-enqueueing an ID with the same spec returns the
  existing record; a different spec returns `ErrConflict`.
- **Bounded history.** `q.Purge(ctx, cutoff, durable.Succeeded, durable.Failed)`
  deletes terminal jobs last updated before `cutoff`. A purged ID can be
  enqueued again as a new job.
- **One owner.** A database file has one owner process (an exclusive file lock)
  and one `Run` at a time. Jobs are claimed in the order they became ready.
- Opening a file written by an earlier unreleased version migrates it to schema
  version 2; older code cannot open it afterwards.

Delivery is bounded attempts, not exactly-once: an `unknown` job may or may not
have run.
