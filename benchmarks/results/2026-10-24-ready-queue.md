# Ready queue — 2026-10-24

Go 1.27.3, darwin/arm64, Apple M5 Max, GOMAXPROCS=8. Before: `5a19088`
(workers take tasks over channels). After: `d56f8b3` (workers take tasks from
a shared ready queue). The benchmark code is the same in both. Details are in
the [environment record](2026-10-24-ready-queue-environment.json).

**Noise.** Other processes loaded the host. The 1-minute load average was
2.61–6.33 during the before/after runs and 5.73–15.50 during the engine
comparison (5-minute: 3.26–4.63 and 4.49–12.08). Each comparison ran 20 rounds
of `-count=1` with the trees or engines alternating. Differences whose
10th–90th percentile bands overlap are not distinguished here.

## Cause of the 32-caller tail

[The load record](2026-10-24-load.md) found a p99.9 of about 7 ms with 32
callers. Execution traces (`go test -trace`, analyzed with
`golang.org/x/exp/trace`) of that benchmark on both trees, three runs each:

| Measure, per run of 100,000 round trips | Before | After |
| --- | ---: | ---: |
| Worker parks waiting for work | 93,686–98,903 | 11,421–11,752 |
| Longest run of goroutines on one processor, each woken by the previous one | 1,687–2,381 | 55–57 |
| Such runs longer than 1 ms | 342–381 | 3–4 |
| Callers run on another processor than the one that woke them | 0.7–4.2% | 36.9–37.4% |
| Processor idle time | 1.7–2.1% | 6.2–8.3% |

Before, each task was a channel handoff to a parked worker. The runtime places
a goroutine woken by a channel operation in the waking processor's next-run
slot, so each processor kept alternating one worker and one caller, and
callers queued behind them waited until the scheduler preempted that pair,
about every 10 ms. After, a worker takes queued tasks without parking and a
producer wakes a worker only when one is idle; callers that block wake no one,
so processors reach their run queues or go idle and take work from others.

Diagnostic runs, summarized only (their raw data is not kept here):

- `GOGC=off` left the tail in place (median p99.9 6.6 ms against 7.0 ms).
- `-cpu=16` lowered p99.9 to 1.4–1.9 ms and raised p99 to about 0.75 ms.
- Routing go-worker through `Submit` and `Future.Wait` instead of `Schedule`
  kept the tail in 7 of 20 rounds.
- Calling `runtime.Gosched` after each reply removed the tail but lowered
  `BenchmarkPool` and `BenchmarkConcurrentSubmit` throughput by 37% (geometric
  mean); it was not adopted.

## Before and after

Medians across 20 rounds; p-values are benchstat Mann–Whitney U tests, n=20.

`BenchmarkLatency`, 8 workers, Noop:

| Callers | Measure | Before | After | p |
| ---: | --- | ---: | ---: | ---: |
| 1 | p50 | 333 ns | 292 ns | 0.000 |
| 1 | p99 | 3.06 µs | 2.75 µs | 0.000 |
| 1 | p99.9 | 8.77 µs | 8.73 µs | 0.920 |
| 1 | ns/op | 551 | 529 | 0.000 |
| 32 | p50 | 1.38 µs | 16.92 µs | 0.000 |
| 32 | p99 | 271 µs | 113 µs | 0.000 |
| 32 | p99.9 | 6.50 ms | 0.21 ms | 0.000 |
| 32 | ns/op | 1,453 | 741 | 0.000 |

With 32 callers, p99.9 was 5 ms or more in 18 of 20 rounds before and in none
after; the highest value after was 1.06 ms.

Throughput, tasks/s:

| Case | Before | After | Change | p |
| --- | ---: | ---: | ---: | ---: |
| Pool Noop, 1 worker | 4.235M | 2.928M | −30.9% | 0.000 |
| Pool Noop, 8 workers | 2.206M | 1.300M | −41.1% | 0.000 |
| Pool SHA2564KiB, 1 worker | 628.3k | 629.3k | ~ | 0.121 |
| Pool SHA2564KiB, 8 workers | 1.350M | 1.308M | −3.1% | 0.000 |
| Pool Wait100us, 1 worker | 7.585k | 7.575k | ~ | 0.330 |
| Pool Wait100us, 8 workers | 60.91k | 60.91k | ~ | 0.995 |
| ConcurrentSubmit Noop, 1 worker | 2.795M | 1.618M | −42.1% | 0.001 |
| ConcurrentSubmit Noop, 8 workers | 2.367M | 1.658M | −30.0% | 0.000 |
| ConcurrentSubmit SHA2564KiB, 1 worker | 585.5k | 579.0k | ~ | 0.121 |
| ConcurrentSubmit SHA2564KiB, 8 workers | 1.353M | 1.295M | −4.3% | 0.000 |

The ready queue takes a mutex and, for `Submit`, a capacity slot per task.
That cost shows mostly in Noop, where the task body takes almost no time.

Raw runs: [latency before](2026-10-24-ready-queue-latency-before.txt),
[latency after](2026-10-24-ready-queue-latency-after.txt),
[throughput before](2026-10-24-ready-queue-throughput-before.txt),
[throughput after](2026-10-24-ready-queue-throughput-after.txt),
[load](2026-10-24-ready-queue-load.txt).

## Engines at d56f8b3

ns/task, median (10th–90th percentile) across 20 rounds:

| Case | channel | go-worker | ants | pond |
| --- | ---: | ---: | ---: | ---: |
| Pool Noop, 1 worker | 22 (20–25) | 329 (258–443) | 220 (209–279) | 404 (328–528) |
| Pool Noop, 8 workers | 80 (64–92) | 856 (763–920) | 426 (386–526) | 724 (542–795) |
| Pool SHA2564KiB, 1 worker | 1,482 (1,396–1,613) | 1,590 (1,532–1,667) | 2,056 (1,851–2,425) | 1,710 (1,590–2,006) |
| Pool SHA2564KiB, 8 workers | 583 (473–615) | 1,026 (794–1,067) | 1,045 (770–1,081) | 950 (633–1,018) |
| Pool Wait100us, 1 worker | 119,448 | 119,971 | 121,279 | 120,761 |
| Pool Wait100us, 8 workers | 14,884 | 15,018 | 14,940 | 15,088 |
| ConcurrentSubmit Noop, 1 worker | 76 (67–82) | 652 (600–668) | 236 (214–320) | 1,018 (872–1,069) |
| ConcurrentSubmit Noop, 8 workers | 38 (30–44) | 667 (603–714) | 925 (848–1,028) | 755 (628–861) |
| ConcurrentSubmit SHA2564KiB, 1 worker | 1,578 (1,424–1,793) | 2,106 (1,740–2,496) | 2,016 (1,862–2,445) | 2,372 (1,907–2,793) |
| ConcurrentSubmit SHA2564KiB, 8 workers | 544 (451–582) | 962 (789–1,068) | 1,896 (1,740–2,230) | 1,124 (839–1,188) |

`BenchmarkLatency`, 8 workers, Noop, medians across rounds:

| Callers | Engine | p50 | p99 | p99.9 (10th–90th) | ns/op | allocs/op |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | channel | 167 ns | 2.1 µs | 11.3 µs (7.6–14.0) | 292 | 1 |
| 1 | go-worker | 333 ns | 3.6 µs | 13.6 µs (9.0–18.1) | 615 | 3 |
| 1 | ants | 167 ns | 2.3 µs | 11.5 µs (7.6–14.3) | 321 | 2 |
| 1 | pond | 1,041 ns | 6.9 µs | 16.1 µs (10.5–22.8) | 1,640 | 7 |
| 32 | channel | 480 ns | 54 µs | 146 µs (79–168) | 417 | 1 |
| 32 | go-worker | 19.1 µs | 153 µs | 261 µs (166–302) | 906 | 3 |
| 32 | ants | 16.4 µs | 393 µs | 771 µs (729–903) | 1,276 | 2 |
| 32 | pond | 18.8 µs | 157 µs | 270 µs (187–323) | 919 | 7 |

Among the three pools, go-worker has the lowest median in three throughput
cases (Pool SHA2564KiB with one worker, ConcurrentSubmit with eight workers),
ants in four (the Noop cases with one worker, Pool Noop with eight, and
ConcurrentSubmit SHA2564KiB with one worker), and pond in Pool SHA2564KiB with
eight workers, where the bands of all three overlap. The
Wait100us cases differ by less than 1.5%. With 32 callers, go-worker and pond
have similar latency percentiles and ants a higher p99 and p99.9; with one
caller, ants has the lowest latency of the three. This run had the highest
host load of the records listed here.

Raw runs: [all engines](2026-10-24-ready-queue-engines.txt),
[load](2026-10-24-ready-queue-engines-load.txt).
