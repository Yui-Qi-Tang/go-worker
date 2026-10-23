package worker

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func newScalingMaster(t *testing.T, minWorkers, maxWorkers int, opts ...MasterOption) *Master {
	t.Helper()
	m, err := NewMaster(append([]MasterOption{
		WithMasterLogger(zap.NewNop()), WithWorkers(minWorkers, maxWorkers), WithIdleTimeout(20 * time.Millisecond),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Stop()
		if err := m.Wait(testContext(t)); err != nil {
			t.Errorf("master cleanup: %v", err)
		}
	})
	return m
}

func TestPoolGrowsToMaxUnderLoadAndShrinksToMin(t *testing.T) {
	m := newScalingMaster(t, 1, 4, WithQueueCapacity(8))
	if got := m.Workers(); got != 1 {
		t.Fatalf("initial workers=%d, want min", got)
	}
	gates := make([]*gatedTask, 4)
	futures := make([]*Future, 4)
	for i := range gates {
		gates[i] = newGatedTask(t)
		futures[i] = submitTask(t, m, gates[i])
	}
	// All four run at once, so the pool grew to its maximum.
	for _, gate := range gates {
		awaitSignal(t, gate.entered)
	}
	extra := submitTask(t, m, phaseErrorTask{id: "queued"})
	if got := m.Workers(); got != 4 {
		t.Fatalf("workers under load=%d, want max", got)
	}
	for _, gate := range gates {
		gate.unblock()
	}
	for _, f := range append(futures, extra) {
		if r := awaitResult(t, f); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	waitFor(t, "idle workers to retire", func() bool { return m.Workers() == 1 })
}

func TestPoolWithZeroMinStartsWorkersOnDemand(t *testing.T) {
	m := newScalingMaster(t, 0, 2, WithQueueCapacity(1))
	if got := m.Workers(); got != 0 {
		t.Fatalf("initial workers=%d", got)
	}
	if err := m.Schedule(phaseErrorTask{}); err != nil {
		t.Fatal(err)
	}
	if r := awaitResult(t, submitTask(t, m, phaseErrorTask{id: "async"})); r.Err != nil {
		t.Fatal(r.Err)
	}
	waitFor(t, "pool to empty", func() bool { return m.Workers() == 0 })
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestFixedPoolDoesNotRetireIdleWorkers(t *testing.T) {
	m := newScalingMaster(t, 2, 2)
	time.Sleep(60 * time.Millisecond) // three idle timeouts
	if got := m.Workers(); got != 2 {
		t.Fatalf("workers=%d", got)
	}
	m.mu.Lock()
	idle := m.idle.Load()
	m.mu.Unlock()
	if idle != 0 {
		t.Fatalf("fixed pool tracked idle workers: %d", idle)
	}
}

func TestScaleUpAfterPanicsSpendsRestartBudget(t *testing.T) {
	m := newScalingMaster(t, 0, 2, WithRecoveryPolicy(RecoveryPolicy{MaxRestarts: 1, Window: 100 * time.Millisecond}))
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	// The next worker replaces the panicked one and spends the only restart.
	if err := m.Schedule(panicTask); err != ErrWorkerPanic {
		t.Fatal(err)
	}
	if err := m.Schedule(phaseErrorTask{}); err != ErrRecoveryExhausted {
		t.Fatalf("over the restart limit=%v", err)
	}
	stats := m.RecoveryStats()
	if stats.Attempts != 1 || stats.Started != 1 || stats.Exhausted != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	waitFor(t, "restart window", func() bool { return m.Schedule(phaseErrorTask{}) == nil })
}

func TestScalingPoolDrainsOnShutdown(t *testing.T) {
	m := newScalingMaster(t, 0, 3, WithQueueCapacity(6))
	gate := newGatedTask(t)
	first := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	var rest []*Future
	for range 5 {
		rest = append(rest, submitTask(t, m, phaseErrorTask{}))
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- m.Shutdown(context.Background()) }()
	gate.unblock()
	for _, f := range append(rest, first) {
		if r := awaitResult(t, f); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
	if got := m.Workers(); got != 0 {
		t.Fatalf("workers after shutdown=%d", got)
	}
}

func TestIdleTimeoutValidation(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if m, err := NewMaster(WithIdleTimeout(d)); m != nil || err != ErrInvalidIdleTimeout {
			t.Fatalf("WithIdleTimeout(%v)=%v,%v", d, m, err)
		}
	}
}
