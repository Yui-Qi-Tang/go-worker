# Worker pool benchmarks

This separate module compares go-worker with other pools without adding them to
the production module. It imports the local checkout through `replace`.

## What it measures

Time and allocations per completed task for each pool's native API, under the
same successful workload and worker count. One operation is one completed task;
`ns/op` is batch time per task, including pool construction and draining, not
task latency.

| Setting | Value |
| --- | --- |
| Engines | minimal channel pool, go-worker, ants, pond (versions in `go.mod`) |
| Workers | 1 or 8, one submitting goroutine, `GOMAXPROCS=8` |
| Pending queue | 64 for channel, go-worker, and pond; ants hands off directly |
| go-worker setup | fixed pool (`WithWorkers(n, n)`), `Submit`/`Shutdown`, no-op logger |

| Workload | Task body | Tasks per sample |
| --- | --- | ---: |
| Noop | Return a constant | 100,000 |
| SHA2564KiB | SHA-256 of a fixed 4 KiB array | 10,000 |
| Wait100us | `time.Sleep(100*time.Microsecond)` | 1,000 |

Every task adds to a shared atomic checksum, which each sample verifies. The
engines offer different guarantees: go-worker runs Task phases, builds results,
records metrics, and returns futures, while the channel baseline does none of
these. The figures compare those native configurations, not equivalent
implementations.

## Load scenarios

Two more benchmarks cover what `BenchmarkPool` does not:

| Benchmark | Measures |
| --- | --- |
| `BenchmarkConcurrentSubmit` | `BenchmarkPool` with 8 goroutines submitting at once; Noop and SHA2564KiB, 1 or 8 workers |
| `BenchmarkLatency` | A pool that stays up, 8 workers, 1 or 32 callers each submitting one Noop task and waiting for it in a loop; reports p50, p99, and p99.9 round-trip latency |

In `BenchmarkLatency` each engine uses its own submit-and-wait path: go-worker
`Schedule`, ants `Submit` with a done channel, pond `Submit(...).Wait()`, and
the channel baseline a done channel. Its ns/op is the inverse of closed-loop
throughput, not latency.

## Methodology

- **Fixed work per sample.** The commands below set `-benchtime` to a task
  count, so every sample of a case runs the same number of tasks; a record's
  environment file notes when it used the default benchtime instead.
  `BenchmarkPool` and `BenchmarkConcurrentSubmit` build and drain a pool in each
  sample; `BenchmarkLatency` keeps one pool per sample and times each round trip
  with `time.Now`.
- **Native configurations.** Each engine uses its own API with the settings in
  the tables above. Worker count and `GOMAXPROCS` are the same for all engines,
  and so is the pending-queue size for the engines that have one.
- **Interleaving.** Since the 2026-10-24 dispatcher-removal record,
  comparisons run several rounds and change the order of engines or trees
  between rounds, giving 10 or 20 samples per case, so slow periods on the host
  affect every engine. The earlier summaries ran five samples per case back to
  back in a fixed engine order.
- **Reporting.** Records give medians across rounds with the 10th–90th
  percentile range (the earlier summaries give the observed range), or
  benchstat medians with Mann–Whitney U p-values when two trees are compared.
  They state which differences the noise allows.
- **Recording.** Each record keeps the raw `go test` output and an environment
  file with the Go version, module versions, host, and the commits or patches
  measured; records made on a loaded host also keep the load averages sampled
  during the run. Ablation patches are kept next to their results. Profiles
  and execution traces used for diagnosis are summarized in the record text.

## Limitations

- All records come from one machine: an Apple M5 Max running macOS on arm64,
  with `GOMAXPROCS=8` on 18 cores. There are no Linux or x86 results, and
  scheduling on other systems may rank the engines differently.
- The host was not isolated. Other processes ran during the later records,
  whose 1-minute load averages ranged from about 2 to 16; the earlier
  summaries did not sample load. Results with overlapping percentile ranges
  are not distinguished.
- The workloads are synthetic: an empty task, SHA-256 of 4 KiB, and a 100 µs
  sleep. Tasks that allocate, perform I/O, or contend on shared state may
  behave differently.
- The engines provide different features. go-worker runs task phases, builds
  results, records metrics, and supervises workers, so the figures compare
  native configurations rather than equivalent implementations. Noop results
  mostly measure handoff overhead, and the shared atomic checksum adds
  contention between workers to every workload.
- Only 1 and 8 workers and 1, 8, or 32 concurrent callers are covered.
  `BenchmarkLatency` is a closed loop: each caller waits for its result before
  submitting again, so behavior under a fixed arrival rate or overload is not
  measured. A p99.9 from 100,000 round trips rests on about 100 samples.
- This project wrote and runs the harness; it has not been reviewed
  independently.

## Run

From this directory:

```sh
go test -run '^$' -bench '^BenchmarkPool/Noop/' -benchmem -benchtime=100000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/SHA2564KiB/' -benchmem -benchtime=10000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/Wait100us/' -benchmem -benchtime=1000x -count=5 -cpu=8 -timeout=10m
```

The load scenarios use fixed counts too:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentSubmit/Noop/' -benchmem -benchtime=100000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkConcurrentSubmit/SHA2564KiB/' -benchmem -benchtime=10000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkLatency/' -benchmem -benchtime=100000x -count=5 -cpu=8 -timeout=10m
```

`-count` runs each case's samples back to back. To interleave engines, repeat
`-count=1` instead.

Check the harness itself under the race detector:

```sh
go test -race -run '^$' -bench '^Benchmark(Pool|ConcurrentSubmit|Latency)$' -benchtime=100x -cpu=8 -timeout=5m
```

Summarize a record's raw runs:

```sh
python3 summarize.py --date 2026-10-23 --go "Go 1.27.3" --commit cc99888 \
  results/2026-10-23-Noop.txt results/2026-10-23-SHA2564KiB.txt results/2026-10-23-Wait100us.txt
```

## Records

Each record in `results/` keeps its raw samples and an environment file with
source hashes and host details. The branch history was regrouped after the
2026-10-24 records were made; their commit hashes refer to commits with the
same production and benchmark code as the trees measured.

- [2026-10-04](results/2026-10-04-summary.md): Go 1.27.1, pond v2.7.2, commit `cb271c9`.
- [2026-10-23](results/2026-10-23-summary.md): Go 1.27.3, pond v2.7.3, commit `cc99888`.
- [2026-10-23 dispatch path](results/2026-10-23-hotpath.md): go-worker only,
  one step per dispatch change.
- [2026-10-24](results/2026-10-24-summary.md): Go 1.27.3, pond v2.7.3, commit `c769d67`,
  workers taking tasks directly from the master.
- [2026-10-24 dispatcher removal](results/2026-10-24-dispatch.md): go-worker only,
  develop against each step and against an autoscaling pool.
- [2026-10-24 actor workers](results/2026-10-24-actor.md): ablation of the shared
  channel, CPU profiles, and all engines at `38b3d04`; recorded on a loaded host.
- [2026-10-24 load scenarios](results/2026-10-24-load.md): concurrent submitters,
  round-trip latency, and a tail-latency ablation; recorded on a loaded host.
- [2026-10-24 ready queue](results/2026-10-24-ready-queue.md): the cause of the
  32-caller tail, the ready queue against channel handoffs, and all engines at
  `d56f8b3`; recorded on a loaded host.
