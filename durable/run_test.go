package durable

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	worker "yuki-tang.github.com"
)

type testTask struct {
	init func() error
	run  func() error
	done func() error
}

func (*testTask) ID() string { return "application-task-ID" }
func (t *testTask) Init() error {
	if t.init != nil {
		return t.init()
	}
	return nil
}
func (t *testTask) Run() error {
	if t.run != nil {
		return t.run()
	}
	return nil
}
func (t *testTask) Done() error {
	if t.done != nil {
		return t.done()
	}
	return nil
}

func testMaster(t *testing.T, workers int, options ...worker.MasterOption) *worker.Master {
	t.Helper()
	options = append(options, worker.WithQueueCapacity(workers), worker.WithMasterLogger(zap.NewNop()))
	m, err := worker.NewMaster(options...)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddWorkers(workers); err != nil {
		t.Fatal(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Stop)
	return m
}

func startRunner(t *testing.T, q *Queue, m *worker.Master, build Builder, concurrency int) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Run(ctx, m, build, concurrency) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("runner did not stop")
		}
	})
	return cancel, done
}

func waitJob(t *testing.T, q *Queue, id string, state State) Job {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		q.mu.Lock()
		changed := q.changed
		q.mu.Unlock()
		job, err := q.Job(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == state {
			return job
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("job %q = %+v, want %s", id, job, state)
		}
	}
}

func TestMasterUnavailablePreservesDurableAcceptanceAndRefundsAttempt(t *testing.T) {
	q := openQueue(t)
	accepted, err := q.Enqueue(context.Background(), spec("accepted"))
	if err != nil {
		t.Fatal(err)
	}
	empty, err := worker.NewMaster(worker.WithQueueCapacity(1), worker.WithMasterLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Stop()
	var builds atomic.Int64
	build := func(job Job) (worker.Task, error) {
		builds.Add(1)
		if job.Kind != "test" || job.Version != 1 || string(job.Payload) != "payload" {
			t.Errorf("builder input = %+v", job)
		}
		return &testTask{}, nil
	}
	if err := q.Run(context.Background(), empty, build, 1); !errors.Is(err, worker.ErrMasterWorkerPoolIsEmpty) {
		t.Fatalf("unavailable master = %v", err)
	}
	pending, err := q.Job(context.Background(), accepted.ID)
	if err != nil || pending.State != Pending || pending.Attempts != 0 || builds.Load() != 0 {
		t.Fatalf("accepted job lost or attempt spent: %+v, %v, builds=%d", pending, err, builds.Load())
	}
	m := testMaster(t, 1)
	startRunner(t, q, m, build, 1)
	finished := waitJob(t, q, accepted.ID, Succeeded)
	if finished.Attempts != 1 || builds.Load() != 1 {
		t.Fatalf("finished = %+v, builds=%d", finished, builds.Load())
	}
}

func TestRunnerCancellationDrainsAcceptedWorkAndGuardsOwnership(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 1)
	if _, err := q.Enqueue(context.Background(), spec("blocked")); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	build := func(Job) (worker.Task, error) {
		return &testTask{run: func() error { close(entered); <-release; return nil }}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Run(ctx, m, build, 1) }()
	t.Cleanup(func() { cancel(); releaseOnce.Do(func() { close(release) }) })
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start")
	}
	if err := q.Close(); !errors.Is(err, ErrRunning) {
		t.Fatalf("close active = %v", err)
	}
	if err := q.Run(context.Background(), m, build, 1); !errors.Is(err, ErrRunning) {
		t.Fatalf("second runner = %v", err)
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("runner abandoned Task: %v", err)
	default:
	}
	job, err := q.Job(context.Background(), "blocked")
	if err != nil || job.State != Running {
		t.Fatalf("premature completion: %+v, %v", job, err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not drain")
	}
	if job := waitJob(t, q, "blocked", Succeeded); job.Attempts != 1 {
		t.Fatal(job)
	}
}

func TestConcurrentRunnerClaimsEachJobOnceAndRunsAllPhases(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 4)
	for i := range 32 {
		if _, err := q.Enqueue(context.Background(), spec(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	var init, run, done atomic.Int64
	build := func(job Job) (worker.Task, error) {
		if job.Attempts != 1 {
			t.Errorf("duplicate claim: %+v", job)
		}
		return &testTask{
			init: func() error { init.Add(1); return nil },
			run:  func() error { run.Add(1); return nil },
			done: func() error { done.Add(1); return nil },
		}, nil
	}
	startRunner(t, q, m, build, 4)
	for i := range 32 {
		waitJob(t, q, fmt.Sprint(i), Succeeded)
	}
	if init.Load() != 32 || run.Load() != 32 || done.Load() != 32 {
		t.Fatalf("phases = %d/%d/%d", init.Load(), run.Load(), done.Load())
	}
	if jobs, err := q.Jobs(context.Background(), Succeeded); err != nil || len(jobs) != 32 {
		t.Fatalf("completed jobs = %d, %v", len(jobs), err)
	}
}

func TestSafeRetryHasIndependentBudgetAndBackoff(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 1)
	input := spec("retry")
	input.Retry = RetryPolicy{Safe: true, MaxAttempts: 3, Delay: 60 * time.Millisecond}
	if _, err := q.Enqueue(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int64
	var previous time.Time
	build := func(job Job) (worker.Task, error) {
		now := time.Now()
		if !previous.IsZero() && now.Sub(previous) < input.Retry.Delay {
			t.Errorf("retry preceded backoff: %v", now.Sub(previous))
		}
		previous = now
		return &testTask{run: func() error { attempts.Add(1); return errors.New("business failure") }}, nil
	}
	startRunner(t, q, m, build, 1)
	job := waitJob(t, q, input.ID, Failed)
	if job.Attempts != 3 || attempts.Load() != 3 || job.LastOutcome.Phase != worker.PhaseRun || job.LastOutcome.Cause != "business failure" {
		t.Fatalf("retry budget = %+v, attempts=%d", job, attempts.Load())
	}
	if stats := m.RecoveryStats(); stats.Attempts != 0 {
		t.Fatalf("task retries spent worker recovery budget: %+v", stats)
	}
}

func TestPanicAndGoexitRemainUnknownWithoutTaskRetryOptIn(t *testing.T) {
	for _, mode := range []string{"panic", "goexit", "builder-panic"} {
		t.Run(mode, func(t *testing.T) {
			q := openQueue(t)
			m := testMaster(t, 1, worker.WithWorkerRecovery(true))
			if _, err := q.Enqueue(context.Background(), spec(mode)); err != nil {
				t.Fatal(err)
			}
			var builds atomic.Int64
			startRunner(t, q, m, func(Job) (worker.Task, error) {
				builds.Add(1)
				if mode == "builder-panic" {
					panic("reconstruction failed")
				}
				return &testTask{run: func() error {
					if mode == "panic" {
						panic("unexpected")
					}
					runtime.Goexit()
					return nil
				}}, nil
			}, 1)
			job := waitJob(t, q, mode, Unknown)
			if job.Attempts != 1 || !job.LastOutcome.Panicked || builds.Load() != 1 || job.LastOutcome.PanicStack == "" {
				t.Fatalf("panic policy changed task retry: %+v, builds=%d", job, builds.Load())
			}
		})
	}
}

type exitingDiagnostic struct{}

func (exitingDiagnostic) Error() string  { runtime.Goexit(); return "" }
func (exitingDiagnostic) String() string { runtime.Goexit(); return "" }

func TestCallerDiagnosticGoexitDoesNotAbandonProcessor(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			q := openQueue(t)
			m := testMaster(t, 1, worker.WithWorkerRecovery(true))
			if _, err := q.Enqueue(context.Background(), spec(mode)); err != nil {
				t.Fatal(err)
			}
			startRunner(t, q, m, func(Job) (worker.Task, error) {
				return &testTask{run: func() error {
					if mode == "panic" {
						panic(exitingDiagnostic{})
					}
					return exitingDiagnostic{}
				}}, nil
			}, 1)
			want := Failed
			if mode == "panic" {
				want = Unknown
			}
			job := waitJob(t, q, mode, want)
			if mode == "error" && job.LastOutcome.Cause != "<diagnostic unavailable>" {
				t.Fatal(job.LastOutcome)
			}
			if mode == "panic" && job.LastOutcome.PanicValue != "<diagnostic unavailable>" {
				t.Fatal(job.LastOutcome)
			}
		})
	}
}

func TestReconstructionErrorsAndNilTaskArePersisted(t *testing.T) {
	for _, mode := range []string{"error", "nil", "typed-nil"} {
		t.Run(mode, func(t *testing.T) {
			q := openQueue(t)
			m := testMaster(t, 1)
			if _, err := q.Enqueue(context.Background(), spec(mode)); err != nil {
				t.Fatal(err)
			}
			startRunner(t, q, m, func(Job) (worker.Task, error) {
				switch mode {
				case "error":
					return nil, errors.New("unsupported payload version")
				case "typed-nil":
					var task *testTask
					return task, nil
				default:
					return nil, nil
				}
			}, 1)
			job := waitJob(t, q, mode, Failed)
			if job.Attempts != 1 || job.LastOutcome.Phase != worker.PhaseInit || job.LastOutcome.Error == "" {
				t.Fatal(job)
			}
		})
	}
}

func TestSafePanicRetryRebuildsTasksAndPreservesBudgetWhenWorkerRecoveryExhausts(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 1, worker.WithWorkerRecovery(true), worker.WithRecoveryPolicy(worker.RecoveryPolicy{
		MaxRestarts: 1, Window: time.Minute,
	}))
	input := spec("panic-retry")
	input.Retry = RetryPolicy{Safe: true, MaxAttempts: 3}
	if _, err := q.Enqueue(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var tasks []*testTask
	build := func(job Job) (worker.Task, error) {
		task := &testTask{run: func() error {
			if job.Attempts < 3 {
				panic("retryable with application deduplication")
			}
			return nil
		}}
		tasks = append(tasks, task)
		return task, nil
	}
	if err := q.Run(context.Background(), m, build, 1); !errors.Is(err, worker.ErrRecoveryExhausted) {
		t.Fatalf("worker recovery exhaustion = %v", err)
	}
	job, err := q.Job(context.Background(), input.ID)
	if err != nil || job.State != Pending || job.Attempts != 2 || !job.LastOutcome.Panicked {
		t.Fatalf("Task attempt spent without capacity: %+v, %v", job, err)
	}
	stats := m.RecoveryStats()
	if stats.Attempts != 1 || stats.Started != 1 || stats.Exhausted != 1 {
		t.Fatalf("worker replacement budget = %+v", stats)
	}
	if len(tasks) != 2 || tasks[0] == tasks[1] {
		t.Fatal("retry reused a failed Task")
	}
	fresh := testMaster(t, 1)
	startRunner(t, q, fresh, build, 1)
	job = waitJob(t, q, input.ID, Succeeded)
	if job.Attempts != 3 {
		t.Fatal(job)
	}
}

func waitPendingCount(t *testing.T, m *worker.Master, want int) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		if m.Stats().Queued == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("master pending = %d, want %d", m.Stats().Queued, want)
		}
	}
}

func occupyWorker(t *testing.T, m *worker.Master) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	future, err := m.Submit(context.Background(), &testTask{run: func() error {
		close(entered)
		<-release
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if result, err := future.Wait(ctx); err != nil || result.Err != nil {
			t.Errorf("occupied task = %+v, %v", result, err)
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker not occupied")
	}
}

func TestCanceledMasterAdmissionRefundsTaskAttempt(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 1)
	occupyWorker(t, m)
	if _, err := m.Submit(context.Background(), &testTask{}); err != nil {
		t.Fatal(err)
	}
	waitPendingCount(t, m, 1)
	if _, err := q.Enqueue(context.Background(), spec("waiting")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	var builds atomic.Int64
	go func() {
		done <- q.Run(ctx, m, func(Job) (worker.Task, error) {
			builds.Add(1)
			return &testTask{}, nil
		}, 1)
	}()
	waitJob(t, q, "waiting", Running)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not cancel")
	}
	job := waitJob(t, q, "waiting", Pending)
	if job.Attempts != 0 || builds.Load() != 0 {
		t.Fatalf("canceled claim spent attempt: %+v, builds=%d", job, builds.Load())
	}
}

func TestMasterStopAfterQueueAcceptancePreservesPendingJob(t *testing.T) {
	q := openQueue(t)
	m := testMaster(t, 1)
	occupyWorker(t, m)
	if _, err := q.Enqueue(context.Background(), spec("queued")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var builds atomic.Int64
	build := func(Job) (worker.Task, error) {
		builds.Add(1)
		return &testTask{}, nil
	}
	go func() { done <- q.Run(context.Background(), m, build, 1) }()
	waitPendingCount(t, m, 1)
	m.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, worker.ErrMasterStopped) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stopped master did not resolve queued job")
	}
	job := waitJob(t, q, "queued", Pending)
	if job.Attempts != 0 || builds.Load() != 0 {
		t.Fatalf("pre-execution rejection spent attempt: %+v, builds=%d", job, builds.Load())
	}
	fresh := testMaster(t, 1)
	startRunner(t, q, fresh, build, 1)
	if job := waitJob(t, q, "queued", Succeeded); job.Attempts != 1 {
		t.Fatal(job)
	}
}

func TestCompletionPersistenceFailureIsReturnedAndReconciledOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m := testMaster(t, 1)
	if _, err := q.Enqueue(context.Background(), spec("unrecorded")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); releaseOnce.Do(func() { close(release) }) })
	done := make(chan error, 1)
	go func() {
		done <- q.Run(ctx, m, func(Job) (worker.Task, error) {
			return &testTask{run: func() error { close(entered); <-release; return nil }}, nil
		}, 1)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not start")
	}
	// Simulate a storage failure without a public fault-injection extension point.
	// No completion transaction can begin after this close.
	if err := q.db.Close(); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("completion write failure = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("storage failure silently abandoned runner")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	job, err := reopened.Job(context.Background(), "unrecorded")
	if err != nil || job.State != Unknown || job.Attempts != 1 || !job.LastOutcome.Interrupted {
		t.Fatalf("uncommitted success must stay uncertain: %+v, %v", job, err)
	}
}
