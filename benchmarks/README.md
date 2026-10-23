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

## Run

From this directory:

```sh
go test -run '^$' -bench '^BenchmarkPool/Noop/' -benchmem -benchtime=100000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/SHA2564KiB/' -benchmem -benchtime=10000x -count=5 -cpu=8 -timeout=10m
go test -run '^$' -bench '^BenchmarkPool/Wait100us/' -benchmem -benchtime=1000x -count=5 -cpu=8 -timeout=10m
```

Check the harness itself under the race detector:

```sh
go test -race -run '^$' -bench '^BenchmarkPool$' -benchtime=100x -cpu=8 -timeout=2m
```

Summarize a record's raw runs:

```sh
python3 summarize.py --date 2026-10-23 --go "Go 1.27.3" --commit cc99888 \
  results/2026-10-23-Noop.txt results/2026-10-23-SHA2564KiB.txt results/2026-10-23-Wait100us.txt
```

## Records

Each record in `results/` keeps all raw samples and an environment file with
source hashes and host details.

- [2026-10-04](results/2026-10-04-summary.md): Go 1.27.1, pond v2.7.2, commit `cb271c9`.
- [2026-10-23](results/2026-10-23-summary.md): Go 1.27.3, pond v2.7.3, commit `cc99888`.
- [2026-10-23 dispatch path](results/2026-10-23-hotpath.md): go-worker only,
  one step per dispatch change.
- [2026-10-24](results/2026-10-24-summary.md): Go 1.27.3, pond v2.7.3, commit `3f00498`,
  workers taking tasks directly from the master.
- [2026-10-24 dispatcher removal](results/2026-10-24-dispatch.md): go-worker only,
  develop against each step and against an autoscaling pool.
