package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// awaitQueued waits until n tasks wait in m's queue.
func awaitQueued(t *testing.T, m *Master, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for int(m.ready.waiting.Load()) != n {
		if time.Now().After(deadline) {
			t.Fatalf("queued = %d, want %d", m.ready.waiting.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIdleWorkerTakesTaskWhileAnotherRuns(t *testing.T) {
	m := newFeatureMaster(t, 2)
	gate := newGatedTask(t)
	blocked := make(chan error, 1)
	go func() { blocked <- m.Schedule(gate) }()
	awaitSignal(t, gate.entered)

	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatal(err)
	}
	gate.unblock()
	if err := awaitError(t, blocked); err != nil {
		t.Fatal(err)
	}
}

func TestScheduleAndSubmitRunInArrivalOrder(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(4))
	gate := newGatedTask(t)
	occupied := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)

	var mu sync.Mutex
	var order []string
	record := func(name string) Task {
		return functionTask{run: func() error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}}
	}
	first := submitTask(t, m, record("first"))
	second := make(chan error, 1)
	go func() { second <- m.Schedule(record("second")) }()
	awaitQueued(t, m, 2)
	third := submitTask(t, m, record("third"))

	gate.unblock()
	for _, f := range []*Future{occupied, first, third} {
		if r := awaitResult(t, f); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if err := awaitError(t, second); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "first" || order[1] != "second" || order[2] != "third" {
		t.Fatalf("order = %v", order)
	}
}

func TestStopAnswersQueuedScheduleWithoutWaitingForWorker(t *testing.T) {
	m := newFeatureMaster(t, 1)
	gate := newGatedTask(t)
	go func() { _ = m.Schedule(gate) }()
	awaitSignal(t, gate.entered)
	queued := make(chan error, 1)
	go func() { queued <- m.Schedule(phaseErrorTask{}) }()
	awaitQueued(t, m, 1)

	m.Stop()
	if err := awaitError(t, queued); err != ErrMasterStopped {
		t.Fatalf("queued Schedule = %v, want %v", err, ErrMasterStopped)
	}
}

func TestShutdownRejectsQueuedScheduleButRunsSubmitted(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(2))
	gate := newGatedTask(t)
	occupied := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	var runs int32
	submitted := submitTask(t, m, countingTask{count: &runs})
	scheduled := make(chan error, 1)
	go func() { scheduled <- m.Schedule(countingTask{count: &runs}) }()
	awaitQueued(t, m, 2)

	shutdown := make(chan error, 1)
	go func() { shutdown <- m.Shutdown(testContext(t)) }()
	if err := awaitError(t, scheduled); err != ErrMasterStopped {
		t.Fatalf("queued Schedule = %v, want %v", err, ErrMasterStopped)
	}
	gate.unblock()
	for _, f := range []*Future{occupied, submitted} {
		if r := awaitResult(t, f); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if err := awaitError(t, shutdown); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
}

func TestReadyQueueWakeTokens(t *testing.T) {
	var q readyQueue
	q.init(0)
	q.addActive(1)
	if request, _ := q.take(); request != nil {
		t.Fatal("took a task from an empty queue")
	}
	if q.active != 0 || q.parked != 1 {
		t.Fatalf("after parking: active=%d parked=%d", q.active, q.parked)
	}

	request := newTaskRequest(phaseErrorTask{})
	if err := q.push(request); err != nil {
		t.Fatal(err)
	}
	if len(q.wake) != 1 || q.active != 1 || q.parked != 0 {
		t.Fatalf("after push: tokens=%d active=%d parked=%d", len(q.wake), q.active, q.parked)
	}
	// The parked worker leaves for another reason and takes the token
	// reserved for it, so no token outlives the parked workers.
	q.unpark()
	if len(q.wake) != 0 || q.active != 1 {
		t.Fatalf("after unpark: tokens=%d active=%d", len(q.wake), q.active)
	}
	if got := q.pop(); got != request {
		t.Fatal("queued request was not returned")
	}
}

func TestConcurrentProducersNeverStrandTasks(t *testing.T) {
	m, err := NewMaster(WithMasterLogger(nopLogger()), WithWorkers(1, 8), WithIdleTimeout(time.Millisecond), WithQueueCapacity(16))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	const producers, perProducer = 16, 200
	var runs int32
	var wg sync.WaitGroup
	ctx := testContext(t)
	for i := range producers {
		wg.Go(func() {
			for j := range perProducer {
				if (i+j)%2 == 0 {
					if err := m.ScheduleContext(ctx, countingTask{count: &runs}); err != nil {
						t.Error(err)
						return
					}
					continue
				}
				f, err := m.Submit(ctx, countingTask{count: &runs})
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := f.Wait(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&runs); got != producers*perProducer {
		t.Fatalf("runs = %d, want %d", got, producers*perProducer)
	}
}
