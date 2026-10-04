package benchmarks_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
	worker "yuki-tang.github.com"
)

const queueCapacity = 64

type benchmarkTask struct{ run func() }

func (benchmarkTask) ID() string  { return "benchmark" }
func (benchmarkTask) Init() error { return nil }
func (t benchmarkTask) Run() error {
	t.run()
	return nil
}
func (benchmarkTask) Done() error { return nil }

// BenchmarkPool measures a finite batch, including pool construction, submission,
// execution, and draining. One operation is one completed task, not a latency
// sample for an individual task. See README.md for the fixed run protocol.
func BenchmarkPool(b *testing.B) {
	var payload [4096]byte
	for i := range payload {
		payload[i] = byte(i)
	}
	digest := sha256.Sum256(payload[:])
	workloads := []struct {
		name   string
		work   func() uint64
		weight uint64
	}{
		{"Noop", func() uint64 { return 1 }, 1},
		{"SHA2564KiB", func() uint64 {
			digest := sha256.Sum256(payload[:])
			return uint64(digest[0]) + 1
		}, uint64(digest[0]) + 1},
		{"Wait100us", func() uint64 { time.Sleep(100 * time.Microsecond); return 1 }, 1},
	}
	runners := []struct {
		name string
		run  func(*testing.B, int, int, func())
	}{
		{"channel", runChannel},
		{"go-worker", runGoWorker},
		{"ants", runAnts},
		{"pond", runPond},
	}
	for _, workload := range workloads {
		for _, workers := range []int{1, 8} {
			for _, runner := range runners {
				b.Run(fmt.Sprintf("%s/workers=%d/%s", workload.name, workers, runner.name), func(b *testing.B) {
					var completed atomic.Uint64
					job := func() { completed.Add(workload.work()) }
					b.ReportAllocs()
					b.ResetTimer()
					runner.run(b, workers, b.N, job)
					b.StopTimer()
					if got, want := completed.Load(), uint64(b.N)*workload.weight; got != want {
						b.Fatalf("completed work checksum = %d, want %d", got, want)
					}
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tasks/s")
				})
			}
		}
	}
}

func runChannel(_ *testing.B, workers, tasks int, job func()) {
	jobs := make(chan func(), queueCapacity)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for job := range jobs {
				job()
			}
		})
	}
	for range tasks {
		jobs <- job
	}
	close(jobs)
	wg.Wait()
}

func runGoWorker(b *testing.B, workers, tasks int, job func()) {
	m, err := worker.NewMaster(worker.WithQueueCapacity(queueCapacity), worker.WithMasterLogger(zap.NewNop()))
	if err != nil {
		b.Fatal(err)
	}
	defer m.Stop()
	if err := m.AddWorkers(workers); err != nil {
		b.Fatal(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		b.Fatal(err)
	}
	task := benchmarkTask{run: job}
	for range tasks {
		if _, err := m.Submit(context.Background(), task); err != nil {
			b.Fatal(err)
		}
	}
	if err := m.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func runAnts(b *testing.B, workers, tasks int, job func()) {
	p, err := ants.NewPool(workers, ants.WithDisablePurge(true))
	if err != nil {
		b.Fatal(err)
	}
	defer p.Release()
	for range tasks {
		if err := p.Submit(job); err != nil {
			b.Fatal(err)
		}
	}
	if err := p.ReleaseContext(context.Background()); err != nil {
		b.Fatal(err)
	}
}

func runPond(_ *testing.B, workers, tasks int, job func()) {
	p := pond.NewPool(workers, pond.WithQueueSize(queueCapacity))
	for range tasks {
		p.Submit(job)
	}
	p.StopAndWait()
}
