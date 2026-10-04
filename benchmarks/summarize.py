#!/usr/bin/env python3
"""Summarize the frozen baseline's five samples without significance claims."""

import argparse
import re
import statistics
from collections import defaultdict
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    args = parser.parse_args()
    samples = defaultdict(list)
    task_counts = {"Noop": 100000, "SHA2564KiB": 10000, "Wait100us": 1000}
    pattern = re.compile(
        r"^BenchmarkPool/(Noop|SHA2564KiB|Wait100us)/workers=(1|8)/"
        r"(channel|go-worker|ants|pond)-8\s+(\d+)\s+(.*)$"
    )
    for path in args.runs:
        text = path.read_text()
        if not re.search(r"^PASS$", text, re.MULTILINE):
            raise ValueError(f"run did not pass: {path}")
        for line in text.splitlines():
            if not line.startswith("BenchmarkPool/"):
                continue
            match = pattern.fullmatch(line)
            if not match:
                raise ValueError(f"unexpected benchmark row: {line}")
            workload, workers, engine, tasks, metrics = match.groups()
            if int(tasks) != task_counts[workload]:
                raise ValueError(f"unexpected task count: {line}")
            fields = metrics.split()
            values = {fields[i + 1]: float(fields[i]) for i in range(0, len(fields), 2)}
            samples[(workload, int(workers), engine)].append(values)

    print("# Baseline measurements — 2026-10-04\n")
    print("Go 1.27.1, darwin/arm64, Apple M5 Max, GOMAXPROCS=8. Production commit: `cb271c9`.\n")
    print("Five samples per case. Times are batch time per completed task, including pool construction and draining; they are not individual task latency. Ranges are observed min–max, not confidence intervals.\n")
    print("Native APIs have different guarantees; see [the frozen protocol](../README.md). [Environment and source hashes](2026-10-04-environment.json) accompany the raw runs.\n")
    print("| Workload | Workers | Engine | Median ns/task | Observed range | Median tasks/s | B/task | Allocs/task |")
    print("| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |")
    for workload in task_counts:
        for workers in (1, 8):
            for engine in ("channel", "go-worker", "ants", "pond"):
                rows = samples[(workload, workers, engine)]
                if len(rows) != 5:
                    raise ValueError(f"expected five samples: {(workload, workers, engine)}")
                times = [row["ns/op"] for row in rows]
                median = lambda metric: statistics.median(row[metric] for row in rows)
                print(f"| {workload} | {workers} | {engine} | {median('ns/op'):,.1f} | {min(times):,.1f}–{max(times):,.1f} | {median('tasks/s'):,.0f} | {median('B/op'):,.0f} | {median('allocs/op'):g} |")
    print("\nRaw runs: [Noop](2026-10-04-Noop.txt), [SHA2564KiB](2026-10-04-SHA2564KiB.txt), [Wait100us](2026-10-04-Wait100us.txt).\n")
    print("Allocation figures are cumulative allocated bytes/objects per task, not peak or retained heap. Wait100us uses a timer to simulate waiting; it does not measure real I/O.\n")
    print("The shared atomic checksum can contend between workers, so these measurements include its scheduling-dependent cost. Startup and shutdown are amortized over finite batches, not measurements of a long-lived pool's steady state. Different batch sizes limit comparisons across workloads. Fixed engine order and host conditions may affect the observed differences.\n")


if __name__ == "__main__":
    main()
