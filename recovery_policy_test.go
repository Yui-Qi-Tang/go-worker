package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func recoveryLimit(limit int) MasterOption {
	return WithRecoveryPolicy(RecoveryPolicy{MaxRestarts: limit, Window: time.Minute})
}

// waitFor polls a condition that becomes true asynchronously, such as a
// replacement started by a timer.
func waitFor(t *testing.T, what string, ready func() bool) {
	t.Helper()
	ctx := testContext(t)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func waitForRecoveryStats(t *testing.T, m *Master, ready func(RecoveryStats) bool) RecoveryStats {
	t.Helper()
	var stats RecoveryStats
	waitFor(t, "recovery stats", func() bool { stats = m.RecoveryStats(); return ready(stats) })
	return stats
}

var panicTask = functionTask{run: func() error { panic("task failure") }}

func TestRecoveryLimitIsPoolWide(t *testing.T) {
	m := newFeatureMaster(t, 2, recoveryLimit(2))
	for range 2 {
		if err := m.Schedule(panicTask); err != ErrWorkerPanic {
			t.Fatal(err)
		}
		waitFor(t, "replacement", func() bool { return m.Workers() == 2 })
	}
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	stats := waitForRecoveryStats(t, m, func(s RecoveryStats) bool { return s.Exhausted == 1 })
	if stats.Attempts != 2 || stats.Started != 2 || stats.Failed != 0 ||
		stats.LastFailure.Stage != "limit" || stats.LastFailure.Err != ErrRecoveryExhausted || stats.LastFailure.WorkerID == "" {
		t.Fatalf("stats=%+v", stats)
	}
	// The remaining worker keeps serving; the pool is not down.
	if got := m.Workers(); got != 1 {
		t.Fatalf("workers=%d", got)
	}
	if err := m.Schedule(phaseErrorTask{}); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryHealsAfterWindow(t *testing.T) {
	m := newFeatureMaster(t, 1, WithRecoveryPolicy(RecoveryPolicy{MaxRestarts: 1, Window: 50 * time.Millisecond}))
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	waitFor(t, "replacement", func() bool { return m.Workers() == 1 })
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	waitForRecoveryStats(t, m, func(s RecoveryStats) bool { return s.Exhausted == 1 })
	if err := m.Schedule(phaseErrorTask{}); err != ErrRecoveryExhausted && err != nil {
		t.Fatalf("while down=%v", err)
	}
	waitFor(t, "healed worker", func() bool { return m.Workers() == 1 })
	if err := m.Schedule(phaseErrorTask{}); err != nil {
		t.Fatalf("after window=%v", err)
	}
	if stats := m.RecoveryStats(); stats.Started != 2 || stats.Exhausted != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestRecoveryCreateFailureRecordsCauseAndRetries(t *testing.T) {
	cause := errors.New("cannot create")
	var failing atomic.Bool
	m := newFeatureMaster(t, 1, WithQueueCapacity(1),
		WithRecoveryPolicy(RecoveryPolicy{MaxRestarts: 1, Window: 50 * time.Millisecond}),
		func(m *Master) {
			create := m.newWorker
			m.newWorker = func(m *Master) (*Worker, error) {
				if failing.Load() {
					return nil, cause
				}
				return create(m)
			}
		})
	failing.Store(true)
	gate := newGatedTask(t)
	last := submitTask(t, m, functionTask{run: func() error { _ = gate.Run(context.Background()); panic("last worker") }})
	awaitSignal(t, gate.entered)
	queued := submitTask(t, m, phaseErrorTask{})
	gate.unblock()
	if r := awaitResult(t, last); r.Err != ErrWorkerPanic {
		t.Fatalf("last=%+v", r)
	}
	if r := awaitResult(t, queued); r.Err != ErrRecoveryFailed {
		t.Fatalf("queued=%+v", r)
	}
	stats := m.RecoveryStats()
	if stats.Attempts != 1 || stats.Failed != 1 || stats.LastFailure.Stage != "create" || !errors.Is(stats.LastFailure.Err, cause) {
		t.Fatalf("stats=%+v", stats)
	}
	failing.Store(false)
	waitFor(t, "retried replacement", func() bool { return m.Workers() == 1 })
	if err := m.Schedule(phaseErrorTask{}); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownDrainsQueueWhenRestartLimitLeavesNoWorker(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(2), recoveryLimit(1))
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	gate := newGatedTask(t)
	last := submitTask(t, m, functionTask{run: func() error { _ = gate.Run(context.Background()); panic("last worker") }})
	awaitSignal(t, gate.entered)
	queued := submitTask(t, m, phaseErrorTask{})
	shutdown := make(chan error, 1)
	go func() { shutdown <- m.Shutdown(testContext(t)) }()
	gate.unblock()
	if r := awaitResult(t, last); r.Err != ErrWorkerPanic {
		t.Fatalf("last=%+v", r)
	}
	if r := awaitResult(t, queued); r.Err != ErrRecoveryExhausted {
		t.Fatalf("queued=%+v", r)
	}
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestNoReplacementAfterStop(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(1))
	gate := newGatedTask(t)
	f := submitTask(t, m, functionTask{run: func() error { _ = gate.Run(context.Background()); panic("after stop") }})
	awaitSignal(t, gate.entered)
	m.Stop()
	gate.unblock()
	if r := awaitResult(t, f); r.Err != ErrWorkerPanic {
		t.Fatalf("result=%+v", r)
	}
	if err := m.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if stats := m.RecoveryStats(); stats != (RecoveryStats{}) {
		t.Fatalf("replaced after Stop: %+v", stats)
	}
}

type failureLogCore struct {
	zapcore.Core
	write func(zapcore.Entry)
}

func (c failureLogCore) Enabled(zapcore.Level) bool { return true }
func (c failureLogCore) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(e, c)
}
func (c failureLogCore) Write(e zapcore.Entry, _ []zapcore.Field) error { c.write(e); return nil }

func TestStartupPanicsAreBoundedEvenWhenDiagnosticLoggerPanics(t *testing.T) {
	logger := zap.New(failureLogCore{Core: zapcore.NewNopCore(), write: func(zapcore.Entry) { panic("logger failure") }})
	m := newFeatureMaster(t, 1, recoveryLimit(3), WithMasterLogger(logger))
	stats := waitForRecoveryStats(t, m, func(s RecoveryStats) bool { return s.Exhausted == 1 })
	if stats.Attempts != 3 || stats.Started != 3 || stats.Failed != 0 {
		t.Fatalf("startup panic loop: %+v", stats)
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != ErrRecoveryExhausted {
		t.Fatalf("startup failure terminal error: %v", err)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPanicResultPrecedesBlockingDiagnosticLog(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	logger := zap.New(failureLogCore{Core: zapcore.NewNopCore(), write: func(e zapcore.Entry) {
		if e.Message == workerPanic {
			close(entered)
			<-release
		}
	}})
	w, err := NewWorker(WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- w.Do(functionTask{run: func() error { panic("task failure") }}) }()
	awaitSignal(t, entered)
	if got := awaitError(t, result); got != ErrWorkerPanic {
		t.Fatalf("task error: %v", got)
	}
	unblock()
	if err := w.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestRestartBudgetRollingWindow(t *testing.T) {
	policy := RecoveryPolicy{MaxRestarts: 2, Window: time.Minute}
	now := time.Unix(1_000, 0)
	var budget restartBudget
	if !budget.allow(now, policy) || !budget.allow(now.Add(10*time.Second), policy) {
		t.Fatal("initial two restarts rejected")
	}
	if budget.allow(now.Add(59*time.Second), policy) {
		t.Fatal("third restart accepted within the window")
	}
	if !budget.allow(now.Add(time.Minute), policy) {
		t.Fatal("attempt at the window boundary did not expire")
	}
	if budget.allow(now.Add(69*time.Second), policy) {
		t.Fatal("non-expired attempts did not count toward the limit")
	}
	if !budget.allow(now.Add(70*time.Second), policy) {
		t.Fatal("second old attempt did not expire")
	}
}

func TestRestartBudgetNextAllowed(t *testing.T) {
	policy := RecoveryPolicy{MaxRestarts: 1, Window: time.Minute}
	var budget restartBudget
	now := time.Now()
	if got := budget.nextAllowed(policy); got.After(time.Now()) {
		t.Fatalf("empty budget waits until %v", got)
	}
	budget.allow(now, policy)
	next := budget.nextAllowed(policy)
	if !next.After(now.Add(policy.Window)) {
		t.Fatalf("next=%v, want after %v", next, now.Add(policy.Window))
	}
	if !budget.allow(next, policy) {
		t.Fatal("attempt at nextAllowed was denied")
	}
}

func TestRecoveryPolicyValidation(t *testing.T) {
	for _, policy := range []RecoveryPolicy{{}, {MaxRestarts: -1, Window: time.Minute}, {MaxRestarts: 1}, {MaxRestarts: 1, Window: -time.Second}} {
		if m, err := NewMaster(WithRecoveryPolicy(policy)); m != nil || err != ErrInvalidRecoveryPolicy {
			t.Fatalf("policy=%+v master=%v err=%v", policy, m, err)
		}
	}
	for _, m := range []*Master{nil, {}} {
		if got := m.RecoveryStats(); got != (RecoveryStats{}) {
			t.Fatalf("uninitialized stats=%+v", got)
		}
	}
}
