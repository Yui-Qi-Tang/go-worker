# Worker pool benchmarks

This separate module pins comparison libraries without adding them to the
production module. It imports the local `go-worker` checkout through `replace`.

## Frozen baseline protocol — 2026-10-04

Question: what is the time and allocation cost per completed task for each
pool's native API under the same successful workload and concurrency limit?
This establishes a baseline; it does not test a proposed optimization.

- Production baseline: commit `cb271c92cd139234037833562ea94ca2b49e521a`.
- Go 1.27.1; `GOMAXPROCS=8`; one submitting goroutine; 1 or 8 workers.
- Engines: a minimal channel pool, go-worker, ants v2.12.1, pond v2.7.2.
- Pending queue capacity: 64 for channel/go-worker/pond. ants uses direct
  blocking handoff without a pending-task queue. Idle purging is disabled for
  ants. go-worker uses a no-op logger and its default disabled replacement policy.
- No task errors, panics, cancellation, retries, or nested submissions.
- Workloads and fixed task counts per sample:

  | Workload | Task body | Tasks/sample | Samples/case |
  | --- | --- | ---: | ---: |
  | Noop | Return a constant | 100,000 | 5 |
  | SHA2564KiB | SHA-256 of the same deterministic 4 KiB byte array | 10,000 | 5 |
  | Wait100us | `time.Sleep(100*time.Microsecond)` | 1,000 | 5 |

- Each task contributes to the same atomic completion checksum. SHA-256 results
  contribute to this checksum so the compiler cannot discard the work. Every
  sample verifies the checksum after all workers finish.
- Timing and allocation counters include construction, startup, native
  submission, task execution, and draining/closing one pool per sample.
  Benchmark harness fixture creation and checksum validation are excluded.
- Submitters use the native API: go-worker `Submit`/`Shutdown`, ants
  `Submit`/`ReleaseContext`, pond `Submit`/`StopAndWait`, channel send/close/wait.
  Returned per-task futures are not individually observed; the batch is drained.
- Primary measurements: `ns/op`, `tasks/s`, `B/op`, and `allocs/op`. One operation
  is one completed task. `ns/op` is amortized batch time, not task latency or p99.
- Completion criterion: all 24 cases complete their fixed task counts, all
  checksum checks pass, and all five raw samples are retained. There is no
  performance pass/fail threshold.

The native APIs provide different guarantees. go-worker executes Task phases,
builds structured results, logs to a no-op logger, records metrics, and returns
futures; the channel baseline supplies none of these. This comparison measures
the cost of those native configurations, not identical internal implementations.
`Wait100us` is simulated waiting, not real I/O; timer resolution affects it.
Fixed engine order and host scheduling/thermal conditions can affect samples.
The shared atomic checksum can create contention, especially for Noop with eight
workers. Its cost depends on scheduling and cannot be subtracted as a constant
to infer pure pool overhead. Pool startup and shutdown are amortized over each
fixed batch; these figures do not describe a long-lived pool's steady state.
Different batch sizes also limit comparisons across workloads.
No claim about production reliability, panic recovery, peak retained memory, or
individual task latency follows from these measurements.

## Run

From this directory, install the pinned dependencies with `go mod tidy`, then:

```sh
go test -run '^$' -bench '^BenchmarkPool/Noop/' -benchmem -benchtime=100000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/SHA2564KiB/' -benchmem -benchtime=10000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/Wait100us/' -benchmem -benchtime=1000x -count=5 -cpu=8 -timeout=10m
```

For a small correctness/race check of the harness:

```sh
go test -race -run '^$' -bench '^BenchmarkPool$' -benchtime=100x -cpu=8 -timeout=2m
```

Keep raw output and environment metadata in `results/` before interpreting
median measurements. See each recorded run for its source hashes and host.

## Recorded baseline

See the [2026-10-04 measurements](results/2026-10-04-summary.md), including all
five samples per case and the environment/source hashes.
