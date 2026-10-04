package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func recoveryLimit(limit int) MasterOption {
	return WithRecoveryPolicy(RecoveryPolicy{MaxRestarts: limit, Window: time.Minute})
}

func waitForRecoveryStats(t *testing.T, m *Master, ready func(RecoveryStats) bool) RecoveryStats {
	t.Helper()
	ctx := testContext(t)
	for {
		m.RLock()
		stats, changed := m.recoveryStats, m.poolChanged
		m.RUnlock()
		if ready(stats) {
			return stats
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("recovery did not finish: %+v", stats)
		}
	}
}

func TestRecoveryLimitSurvivesReplacementAndSuccessfulTasks(t *testing.T) {
	m := newFeatureMaster(t, 1, WithWorkerRecovery(true), recoveryLimit(2))
	original := currentWorker(t, m)
	for attempt := range 3 {
		if err := m.ScheduleContext(testContext(t), functionTask{run: func() error { panic("broken task") }}); err != ErrWorkerPanic {
			t.Fatalf("panic %d: %v", attempt, err)
		}
		if attempt < 2 {
			if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
				t.Fatalf("successful task %d: %v", attempt, err)
			}
		}
	}
	stats := m.RecoveryStats()
	if stats.Attempts != 2 || stats.Started != 2 || stats.Failed != 0 || stats.Exhausted != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if stats.LastFailure.WorkerID != original.identity() || stats.LastFailure.Stage != "limit" || stats.LastFailure.Err != ErrRecoveryExhausted || stats.LastFailure.Time.IsZero() {
		t.Fatalf("last failure=%+v", stats.LastFailure)
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != ErrRecoveryExhausted {
		t.Fatalf("exhausted pool: %v", err)
	}
	if _, err := m.TrySubmit(phaseErrorTask{}); err != ErrRecoveryExhausted {
		t.Fatalf("admission to exhausted pool: %v", err)
	}
	if m.GetWorkers() != 0 {
		t.Fatal("exhausted worker remained registered")
	}
	// Manual replacement starts a new slot budget and preserves failure history.
	w, err := NewWorker(WithName(original.identity()), WithLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddWorker(w); err != nil {
		t.Fatal(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		t.Fatal(err)
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatalf("manual repair: %v", err)
	}
	if err := m.ScheduleContext(testContext(t), functionTask{run: func() error { panic("new slot") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	if got := m.RecoveryStats(); got.Attempts != 3 || got.Started != 3 || got.LastFailure != stats.LastFailure {
		t.Fatalf("stats after repair=%+v", got)
	}
	if stats.Attempts != 2 {
		t.Fatal("previous snapshot was mutated")
	}
}

func TestRecoveryLimitIsPerSlot(t *testing.T) {
	m := newFeatureMaster(t, 2, WithWorkerRecovery(true), recoveryLimit(1))
	m.RLock()
	first, second := m.Pool[0], m.Pool[1]
	m.RUnlock()
	panicTask := functionTask{run: func() error { panic("one slot") }}
	if err := first.Do(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	m.recoverWorker(first.recoveryIdentity())
	var replacement *Worker
	m.RLock()
	for _, w := range m.Pool {
		if w.identity() == first.identity() {
			replacement = w
		}
	}
	m.RUnlock()
	if replacement == nil {
		t.Fatal("missing replacement")
	}
	if err := replacement.Do(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	m.recoverWorker(replacement.recoveryIdentity())
	if err := second.Do(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	m.recoverWorker(second.recoveryIdentity())
	if got := m.RecoveryStats(); got.Attempts != 2 || got.Started != 2 || got.Exhausted != 1 {
		t.Fatalf("stats=%+v", got)
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatalf("remaining healthy slot: %v", err)
	}
}

func TestRecoveryFailureRecordsCauseAndCompletesPendingWork(t *testing.T) {
	cause := errors.New("cannot allocate replacement")
	cases := []struct {
		name  string
		stage string
		err   error
		build func(...Option) (*Worker, error)
	}{
		{"create", "create", cause, func(...Option) (*Worker, error) { return nil, cause }},
		{"start", "start", ErrWorkerStopped, func(opts ...Option) (*Worker, error) {
			w, err := NewWorker(opts...)
			if err == nil {
				w.Stop()
			}
			return w, err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newFeatureMaster(t, 1, WithWorkerRecovery(true), WithQueueCapacity(2), func(m *Master) { m.newRecoveryWorker = tc.build })
			old := currentWorker(t, m)
			gate := newGatedTask(t)
			failed := submitTask(t, m, functionTask{run: func() error { _ = gate.Run(); panic("task failure") }})
			awaitSignal(t, gate.entered)
			ctx := &observedContext{Context: testContext(t), observed: make(chan struct{})}
			waiting := make(chan error, 1)
			go func() { waiting <- m.ScheduleContext(ctx, phaseErrorTask{}) }()
			awaitSignal(t, ctx.observed)
			queued := []*Future{submitTask(t, m, phaseErrorTask{}), submitTask(t, m, phaseErrorTask{})}
			blockedCtx := &observedContext{Context: testContext(t), observed: make(chan struct{})}
			blocked := make(chan error, 1)
			go func() { _, err := m.Submit(blockedCtx, phaseErrorTask{}); blocked <- err }()
			awaitSignal(t, blockedCtx.observed)
			gate.unblock()
			if got := awaitResult(t, failed).Err; got != ErrWorkerPanic {
				t.Fatalf("original task: %v", got)
			}
			if got := awaitError(t, waiting); got != ErrRecoveryFailed {
				t.Fatalf("waiting schedule: %v", got)
			}
			for _, f := range queued {
				if got := awaitResult(t, f).Err; got != ErrRecoveryFailed {
					t.Fatalf("queued task: %v", got)
				}
			}
			if got := awaitError(t, blocked); got != ErrRecoveryFailed {
				t.Fatalf("blocked submit: %v", got)
			}
			stats := m.RecoveryStats()
			if stats.Attempts != 1 || stats.Started != 0 || stats.Failed != 1 || stats.Exhausted != 0 {
				t.Fatalf("stats=%+v", stats)
			}
			failure := stats.LastFailure
			if failure.WorkerID != old.identity() || failure.RecoveryID != old.recoveryIdentity() || failure.Stage != tc.stage || !errors.Is(failure.Err, tc.err) || failure.Time.IsZero() {
				t.Fatalf("failure=%+v", failure)
			}
			if err := m.Shutdown(testContext(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoveryFailureKeepsHealthySlotsAvailable(t *testing.T) {
	cause := errors.New("replacement unavailable")
	m := newFeatureMaster(t, 2, WithWorkerRecovery(true), func(m *Master) {
		m.newRecoveryWorker = func(...Option) (*Worker, error) { return nil, cause }
	})
	old := currentWorker(t, m)
	if err := old.Do(functionTask{run: func() error { panic("one failed slot") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	m.recoverWorker(old.recoveryIdentity())
	if m.GetWorkers() != 1 {
		t.Fatal("healthy slot was removed")
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatalf("healthy slot rejected work: %v", err)
	}
	if got := m.RecoveryStats(); got.Failed != 1 || !errors.Is(got.LastFailure.Err, cause) {
		t.Fatalf("stats=%+v", got)
	}
}

func TestShutdownDrainsQueueWhenRecoveryBudgetExhausts(t *testing.T) {
	m := newFeatureMaster(t, 1, WithWorkerRecovery(true), WithQueueCapacity(2), recoveryLimit(1))
	if err := m.ScheduleContext(testContext(t), functionTask{run: func() error { panic("first") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	gate := newGatedTask(t)
	active := submitTask(t, m, functionTask{run: func() error { _ = gate.Run(); panic("second") }})
	awaitSignal(t, gate.entered)
	queued := []*Future{submitTask(t, m, phaseErrorTask{}), submitTask(t, m, phaseErrorTask{})}
	shutdown := make(chan error, 1)
	go func() { shutdown <- m.Shutdown(testContext(t)) }()
	awaitSignal(t, m.acceptDone)
	gate.unblock()
	if got := awaitResult(t, active).Err; got != ErrWorkerPanic {
		t.Fatalf("active task: %v", got)
	}
	for _, f := range queued {
		if got := awaitResult(t, f).Err; got != ErrRecoveryExhausted {
			t.Fatalf("queued task: %v", got)
		}
	}
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestDroppedRecoverySignalAndConcurrentObserversDoNotLoseOrDuplicateRecovery(t *testing.T) {
	m := newFeatureMaster(t, 1, withBufferedRecoverySignalOnly())
	old := currentWorker(t, m)
	m.workerPanic <- "occupied"
	if err := old.Do(functionTask{run: func() error { panic("direct task") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	// There is no channel consumer or Master task completion in this test.
	// The registered done observer must perform the replacement independently.
	waitForRecoveryStats(t, m, func(s RecoveryStats) bool { return s.Started == 1 })
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { m.recoverWorker(old.recoveryIdentity()) })
	}
	wg.Wait()
	if got := m.RecoveryStats(); got.Attempts != 1 || got.Started != 1 || got.Failed != 0 || got.Exhausted != 0 {
		t.Fatalf("duplicate notifications changed stats: %+v", got)
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatalf("replacement unavailable: %v", err)
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
	m := newFeatureMaster(t, 1, WithWorkerRecovery(true), recoveryLimit(3), WithMasterLogger(logger))
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

func TestDoneObserverDoesNotRestartAfterStop(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	logger := zap.New(failureLogCore{Core: zapcore.NewNopCore(), write: func(e zapcore.Entry) {
		if e.Message == workerPanic {
			close(entered)
			<-release
		}
	}})
	m := newFeatureMaster(t, 1, withBufferedRecoverySignalOnly(), WithMasterLogger(logger))
	t.Cleanup(unblock)
	m.workerPanic <- "occupied"
	old := currentWorker(t, m)
	if err := old.Do(functionTask{run: func() error { panic("task") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	awaitSignal(t, entered)
	m.Stop()
	unblock()
	if err := m.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := m.RecoveryStats(); got.Attempts != 0 || got.Started != 0 {
		t.Fatalf("observer restarted during shutdown: %+v", got)
	}
}

func TestStalePanickedWorkerReportsRecoveryFailure(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	logger := zap.New(failureLogCore{Core: zapcore.NewNopCore(), write: func(e zapcore.Entry) {
		if e.Message == workerPanic {
			close(entered)
			<-release
		}
	}})
	cause := errors.New("replacement unavailable")
	m := newFeatureMaster(t, 1, withBufferedRecoverySignalOnly(), WithMasterLogger(logger), func(m *Master) {
		m.newRecoveryWorker = func(...Option) (*Worker, error) { return nil, cause }
	})
	t.Cleanup(unblock)
	old := currentWorker(t, m)
	m.workerPanic <- "occupied"
	if err := old.Do(functionTask{run: func() error { panic("direct task") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	awaitSignal(t, entered)
	// The queued token still names the panicked worker; neither the dropped
	// notification nor the blocked done observer has recovered it yet.
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != ErrRecoveryFailed {
		t.Fatalf("stale token: %v", err)
	}
	if got := m.RecoveryStats(); got.Failed != 1 || !errors.Is(got.LastFailure.Err, cause) {
		t.Fatalf("stats=%+v", got)
	}
	unblock()
	if err := m.Shutdown(testContext(t)); err != nil {
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

func TestRecoveryPolicyValidationAndDisabledRecovery(t *testing.T) {
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
	m := newFeatureMaster(t, 1, recoveryLimit(1))
	if err := m.ScheduleContext(testContext(t), functionTask{run: func() error { panic("disabled recovery") }}); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	if got := m.RecoveryStats(); got != (RecoveryStats{}) {
		t.Fatalf("policy enabled recovery: %+v", got)
	}
	if err := m.ScheduleContext(context.Background(), phaseErrorTask{}); err != ErrMasterWorkerPoolIsEmpty {
		t.Fatal(err)
	}
}
