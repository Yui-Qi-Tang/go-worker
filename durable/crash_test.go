package durable

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	worker "yuki-tang.github.com"
)

// TestCrashHelper is executed in a subprocess and deliberately killed without
// Close or deferred cleanup. READY is sent only after the tested boundary.
func TestCrashHelper(t *testing.T) {
	mode := os.Getenv("GO_WORKER_DURABLE_CRASH_MODE")
	if mode == "" {
		return
	}
	q, err := Open(os.Getenv("GO_WORKER_DURABLE_CRASH_DB"))
	if err != nil {
		t.Fatal(err)
	}
	input := spec("crash-job")
	input.Payload = []byte(os.Getenv("GO_WORKER_DURABLE_CRASH_EFFECT"))
	if mode == "safe-running" || mode == "completion-gap" || mode == "retry-scheduled" {
		input.Retry = RetryPolicy{Safe: true, MaxAttempts: 2}
	}
	if mode == "exhausted-running" {
		input.Retry.Safe = true
	}
	if mode == "retry-scheduled" {
		input.Retry.Delay = time.Hour
	}
	if _, err := q.Enqueue(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if mode == "accepted" {
		fmt.Println("READY")
		<-time.After(time.Hour)
		return
	}
	m, err := worker.NewMaster(worker.WithQueueCapacity(1), worker.WithMasterLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddWorkers(1); err != nil {
		t.Fatal(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}
	phasesFinished := make(chan struct{})
	build := func(job Job) (worker.Task, error) {
		return &testTask{
			run: func() error {
				if mode == "retry-scheduled" {
					return errors.New("retryable failure")
				}
				if mode == "completion-gap" {
					if err := deduplicatedEffect(job); err != nil {
						return err
					}
					locked := make(chan struct{})
					go func() {
						// Block the completion writer after the external effect.
						// No durable job data is changed in this uncommitted tx.
						if err := q.db.Update(func(*bolt.Tx) error {
							close(locked)
							<-time.After(time.Hour)
							return nil
						}); err != nil {
							t.Error(err)
						}
					}()
					<-locked
					return nil
				}
				fmt.Println("READY")
				<-time.After(time.Hour)
				return nil
			},
			done: func() error { close(phasesFinished); return nil },
		}, nil
	}
	go func() {
		if err := q.Run(context.Background(), m, build, 1); err != nil {
			t.Error(err)
		}
	}()
	if mode == "completion-gap" {
		<-phasesFinished
		// This proves the native Future/Task really completed, while the durable
		// completion transaction is still blocked by the lock held above.
		if err := m.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		fmt.Println("READY")
	} else if mode == "retry-scheduled" {
		var job Job
		// Enqueue itself starts Pending, so wait for the committed failed attempt.
		for {
			q.mu.Lock()
			changed := q.changed
			q.mu.Unlock()
			job, err = q.Job(context.Background(), input.ID)
			if err != nil {
				t.Fatal(err)
			}
			if job.State == Pending && job.Attempts == 1 {
				break
			}
			select {
			case <-changed:
			case <-time.After(5 * time.Second):
				t.Fatal("retry was not committed")
			}
		}
		if job.State != Pending || job.LastOutcome.Error == "" || job.NextAttemptAt.IsZero() {
			t.Fatalf("retry = %+v", job)
		}
		fmt.Println("READY")
	}
	<-time.After(time.Hour)
}

func crashAtBoundary(t *testing.T, mode, path, effect string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(),
		"GO_WORKER_DURABLE_CRASH_MODE="+mode,
		"GO_WORKER_DURABLE_CRASH_DB="+path,
		"GO_WORKER_DURABLE_CRASH_EFFECT="+effect,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err == nil && strings.TrimSpace(line) != "READY" {
			err = fmt.Errorf("unexpected child output: %q", line)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("child did not reach crash boundary")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
}

func deduplicatedEffect(job Job) error {
	file, err := os.OpenFile(string(job.Payload), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(job.ID)
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func TestProcessCrashAfterAcceptanceBeforeDispatch(t *testing.T) {
	dir := t.TempDir()
	path, effect := filepath.Join(dir, "jobs.db"), filepath.Join(dir, "effect")
	crashAtBoundary(t, "accepted", path, effect)
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	job, err := q.Job(context.Background(), "crash-job")
	if err != nil || job.State != Pending || job.Attempts != 0 {
		t.Fatalf("accepted job = %+v, %v", job, err)
	}
	m := testMaster(t, 1)
	startRunner(t, q, m, func(job Job) (worker.Task, error) {
		return &testTask{run: func() error { return deduplicatedEffect(job) }}, nil
	}, 1)
	finished := waitJob(t, q, job.ID, Succeeded)
	if finished.Attempts != 1 {
		t.Fatal(finished)
	}
}

func TestProcessCrashDuringTaskUsesSafetyDeclarationAndAttemptBudget(t *testing.T) {
	for _, mode := range []string{"unsafe-running", "safe-running", "exhausted-running"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			path, effect := filepath.Join(dir, "jobs.db"), filepath.Join(dir, "effect")
			crashAtBoundary(t, mode, path, effect)
			q, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := q.Close(); err != nil {
					t.Error(err)
				}
			})
			job, err := q.Job(context.Background(), "crash-job")
			want := Unknown
			if mode == "safe-running" {
				want = Pending
			}
			if err != nil || job.State != want || job.Attempts != 1 || !job.LastOutcome.Interrupted {
				t.Fatalf("recovered = %+v, %v", job, err)
			}
			m := testMaster(t, 1)
			var builds atomic.Int64
			startRunner(t, q, m, func(job Job) (worker.Task, error) {
				if job.ID == "crash-job" {
					builds.Add(1)
				}
				return &testTask{}, nil
			}, 1)
			if mode == "safe-running" {
				finished := waitJob(t, q, job.ID, Succeeded)
				if finished.Attempts != 2 || builds.Load() != 1 {
					t.Fatalf("retry = %+v, builds=%d", finished, builds.Load())
				}
				return
			}
			// A later job proves the runner continued scanning without dispatching
			// the quarantined job; no arbitrary sleep is used to infer no retry.
			if _, err := q.Enqueue(context.Background(), spec("sentinel")); err != nil {
				t.Fatal(err)
			}
			waitJob(t, q, "sentinel", Succeeded)
			unchanged, err := q.Job(context.Background(), job.ID)
			if err != nil || unchanged.State != Unknown || unchanged.Attempts != 1 || builds.Load() != 0 {
				t.Fatalf("unsafe replay: %+v, %v", unchanged, err)
			}
		})
	}
}

func TestProcessCrashAfterTaskCompletionBeforeDurableCompletionAllowsDeduplication(t *testing.T) {
	dir := t.TempDir()
	path, effect := filepath.Join(dir, "jobs.db"), filepath.Join(dir, "effect")
	crashAtBoundary(t, "completion-gap", path, effect)
	before, err := os.ReadFile(effect)
	if err != nil || string(before) != "crash-job" {
		t.Fatalf("first external effect = %q, %v", before, err)
	}
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	job, err := q.Job(context.Background(), "crash-job")
	if err != nil || job.State != Pending || !job.LastOutcome.Interrupted {
		t.Fatalf("completion gap = %+v, %v", job, err)
	}
	m := testMaster(t, 1)
	var builds atomic.Int64
	startRunner(t, q, m, func(job Job) (worker.Task, error) {
		builds.Add(1)
		return &testTask{run: func() error { return deduplicatedEffect(job) }}, nil
	}, 1)
	finished := waitJob(t, q, job.ID, Succeeded)
	if finished.Attempts != 2 || builds.Load() != 1 {
		t.Fatalf("replayed job = %+v", finished)
	}
	after, err := os.ReadFile(effect)
	if err != nil || string(after) != string(before) {
		t.Fatalf("duplicate changed effect = %q, %v", after, err)
	}
	// A lost enqueue reply is also resolved with the original stable ID. This
	// returns the terminal record rather than creating a third execution.
	again, err := q.Enqueue(context.Background(), job.Spec)
	if err != nil || again.State != Succeeded || again.Attempts != 2 {
		t.Fatalf("duplicate enqueue = %+v, %v", again, err)
	}
}

func TestProcessCrashPreservesAtomicRetryDecisionAndDelay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.db")
	crashAtBoundary(t, "retry-scheduled", path, filepath.Join(dir, "effect"))
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	job, err := q.Job(context.Background(), "crash-job")
	if err != nil || job.State != Pending || job.Attempts != 1 || job.LastOutcome.Error == "" ||
		job.NextAttemptAt.Sub(job.UpdatedAt) != time.Hour {
		t.Fatalf("retry decision was lost: %+v, %v", job, err)
	}
}
