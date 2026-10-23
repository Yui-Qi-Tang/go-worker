package benchmarks_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	worker "github.com/Yui-Qi-Tang/go-worker"
	"github.com/alitto/pond/v2"
	"github.com/panjf2000/ants/v2"
)

const submitters = 8

// BenchmarkConcurrentSubmit is BenchmarkPool with several goroutines
// submitting at once: each sample builds a pool, splits b.N tasks among the
// submitters, and drains the pool. One operation is one completed task.
func BenchmarkConcurrentSubmit(b *testing.B) {
	var payload [4096]byte
	for i := range payload {
		payload[i] = byte(i)
	}
	workloads := []struct {
		name string
		work func()
	}{
		{"Noop", func() {}},
		{"SHA2564KiB", func() { _ = sha256.Sum256(payload[:]) }},
	}
	runners := []struct {
		name string
		run  func(b *testing.B, workers int, submit func(func(func()))) // submit runs each submitter
	}{
		{"channel", concurrentChannel},
		{"go-worker", concurrentGoWorker},
		{"ants", concurrentAnts},
		{"pond", concurrentPond},
	}
	for _, workload := range workloads {
		for _, workers := range []int{1, 8} {
			for _, runner := range runners {
				b.Run(fmt.Sprintf("%s/workers=%d/submitters=%d/%s", workload.name, workers, submitters, runner.name), func(b *testing.B) {
					var completed atomic.Int64
					job := func() { workload.work(); completed.Add(1) }
					b.ReportAllocs()
					b.ResetTimer()
					runner.run(b, workers, func(send func(func())) { splitAmongSubmitters(b.N, func() { send(job) }) })
					b.StopTimer()
					if got := completed.Load(); got != int64(b.N) {
						b.Fatalf("completed %d tasks, want %d", got, b.N)
					}
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/s")
				})
			}
		}
	}
}

// splitAmongSubmitters calls send tasks times in total from submitters
// goroutines and returns when all calls have returned.
func splitAmongSubmitters(tasks int, send func()) {
	var wg sync.WaitGroup
	for i := range submitters {
		n := tasks / submitters
		if i < tasks%submitters {
			n++
		}
		wg.Go(func() {
			for range n {
				send()
			}
		})
	}
	wg.Wait()
}

func concurrentChannel(_ *testing.B, workers int, submit func(func(func()))) {
	jobs := make(chan func(), queueCapacity)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for job := range jobs {
				job()
			}
		})
	}
	submit(func(job func()) { jobs <- job })
	close(jobs)
	wg.Wait()
}

func concurrentGoWorker(b *testing.B, workers int, submit func(func(func()))) {
	m, err := worker.NewMaster(worker.WithWorkers(workers, workers), worker.WithQueueCapacity(queueCapacity), worker.WithMasterLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		b.Fatal(err)
	}
	defer m.Stop()
	submit(func(job func()) {
		if _, err := m.Submit(context.Background(), benchmarkTask{run: job}); err != nil {
			b.Error(err)
		}
	})
	if err := m.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func concurrentAnts(b *testing.B, workers int, submit func(func(func()))) {
	p, err := ants.NewPool(workers, ants.WithDisablePurge(true))
	if err != nil {
		b.Fatal(err)
	}
	defer p.Release()
	submit(func(job func()) {
		if err := p.Submit(job); err != nil {
			b.Error(err)
		}
	})
	if err := p.ReleaseContext(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func concurrentPond(_ *testing.B, workers int, submit func(func(func()))) {
	p := pond.NewPool(workers, pond.WithQueueSize(queueCapacity))
	submit(func(job func()) { p.Submit(job) })
	p.StopAndWait()
}

// BenchmarkLatency measures round trips on a pool that stays up for the whole
// benchmark: callers goroutines each submit one Noop task and wait for it,
// repeatedly. One operation is one round trip. It reports p50, p99, and p99.9
// round-trip latency; ns/op is the inverse of the closed-loop throughput.
func BenchmarkLatency(b *testing.B) {
	const workers = 8
	runners := []struct {
		name string
		// start returns a function that runs one task and waits for it, and
		// a function that shuts the pool down.
		start func(b *testing.B) (roundTrip func(), stop func())
	}{
		{"channel", latencyChannel},
		{"go-worker", latencyGoWorker},
		{"ants", latencyAnts},
		{"pond", latencyPond},
	}
	for _, callers := range []int{1, 32} {
		for _, runner := range runners {
			b.Run(fmt.Sprintf("Noop/workers=%d/callers=%d/%s", workers, callers, runner.name), func(b *testing.B) {
				roundTrip, stop := runner.start(b)
				defer stop()
				samples := make([][]time.Duration, callers)
				var wg sync.WaitGroup
				b.ReportAllocs()
				b.ResetTimer()
				for i := range callers {
					n := b.N / callers
					if i < b.N%callers {
						n++
					}
					samples[i] = make([]time.Duration, 0, n)
					wg.Go(func() {
						for range n {
							start := time.Now()
							roundTrip()
							samples[i] = append(samples[i], time.Since(start))
						}
					})
				}
				wg.Wait()
				b.StopTimer()
				all := slices.Concat(samples...)
				slices.Sort(all)
				for _, q := range []struct {
					name string
					at   float64
				}{{"p50-ns", 0.50}, {"p99-ns", 0.99}, {"p99.9-ns", 0.999}} {
					b.ReportMetric(float64(all[int(q.at*float64(len(all)-1))]), q.name)
				}
			})
		}
	}
}

func latencyChannel(_ *testing.B) (func(), func()) {
	jobs := make(chan chan struct{}, queueCapacity)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for done := range jobs {
				close(done)
			}
		})
	}
	roundTrip := func() {
		done := make(chan struct{})
		jobs <- done
		<-done
	}
	return roundTrip, func() { close(jobs); wg.Wait() }
}

func latencyGoWorker(b *testing.B) (func(), func()) {
	m, err := worker.NewMaster(worker.WithWorkers(8, 8), worker.WithQueueCapacity(queueCapacity), worker.WithMasterLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		b.Fatal(err)
	}
	task := benchmarkTask{run: func() {}}
	roundTrip := func() {
		if err := m.Schedule(task); err != nil {
			b.Error(err)
		}
	}
	stop := func() {
		if err := m.Shutdown(context.Background()); err != nil {
			b.Error(err)
		}
	}
	return roundTrip, stop
}

func latencyAnts(b *testing.B) (func(), func()) {
	p, err := ants.NewPool(8, ants.WithDisablePurge(true))
	if err != nil {
		b.Fatal(err)
	}
	roundTrip := func() {
		done := make(chan struct{})
		if err := p.Submit(func() { close(done) }); err != nil {
			b.Error(err)
			return
		}
		<-done
	}
	return roundTrip, p.Release
}

func latencyPond(_ *testing.B) (func(), func()) {
	p := pond.NewPool(8, pond.WithQueueSize(queueCapacity))
	roundTrip := func() {
		// Wait returns the task's error; the Noop task has none.
		_ = p.Submit(func() {}).Wait()
	}
	return roundTrip, p.StopAndWait
}
