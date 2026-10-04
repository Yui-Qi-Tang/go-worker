package worker

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type functionTask struct {
	phaseErrorTask
	run func() error
}

func (t functionTask) Run() error { return t.run() }

type gatedTask struct {
	phaseErrorTask
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedTask(t *testing.T) *gatedTask {
	t.Helper()
	task := &gatedTask{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(task.unblock)
	return task
}

func (t *gatedTask) Run() error { close(t.entered); <-t.release; return nil }
func (t *gatedTask) unblock()   { t.once.Do(func() { close(t.release) }) }

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newFeatureMaster(t *testing.T, count int, opts ...MasterOption) *Master {
	t.Helper()
	m, err := NewMaster(append([]MasterOption{WithMasterLogger(zap.NewNop())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Wait(ctx); err != nil {
			t.Errorf("master cleanup: %v", err)
		}
	})
	if err := m.AddWorkers(count); err != nil {
		t.Fatal(err)
	}
	if count > 0 {
		if err := m.WakeAllWorkersUp(); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func awaitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

func awaitResult(t *testing.T, f *Future) Result {
	t.Helper()
	result, err := f.Wait(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func submitTask(t *testing.T, m *Master, task Task) *Future {
	t.Helper()
	f, err := m.Submit(testContext(t), task)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestScheduleContextCanceledBeforeHandoffReturnsWorker(t *testing.T) {
	m := newFeatureMaster(t, 1)
	w := currentWorker(t, m)
	gate := newGatedTask(t)
	first := make(chan error, 1)
	// Direct Do occupies the worker while it is still available in WorkerQueue.
	go func() { first <- w.Do(gate) }()
	awaitSignal(t, gate.entered)

	var runs int32
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if got := m.ScheduleContext(ctx, countingTask{count: &runs}); got != context.DeadlineExceeded {
		t.Fatalf("waiting handoff = %v", got)
	}
	gate.unblock()
	if got := awaitError(t, first); got != nil {
		t.Fatal(got)
	}
	if got := m.ScheduleContext(testContext(t), countingTask{count: &runs}); got != nil {
		t.Fatal(got)
	}
	if got := atomic.LoadInt32(&runs); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
}

func TestScheduleContextAfterHandoffRetainsWorkerUntilCompletion(t *testing.T) {
	m := newFeatureMaster(t, 1)
	gate := newGatedTask(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- m.ScheduleContext(ctx, gate) }()
	awaitSignal(t, gate.entered)
	cancel()
	select {
	case err := <-first:
		t.Fatalf("returned before task finished: %v", err)
	default:
	}

	secondCtx, secondCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer secondCancel()
	if got := m.ScheduleContext(secondCtx, phaseErrorTask{}); got != context.DeadlineExceeded {
		t.Fatalf("second = %v", got)
	}
	if got := m.Stats(); got.InFlight != 1 || got.Started != 1 {
		t.Fatalf("stats = %+v", got)
	}
	gate.unblock()
	if got := awaitError(t, first); got != nil {
		t.Fatal(got)
	}
	if got := m.ScheduleContext(testContext(t), phaseErrorTask{}); got != nil {
		t.Fatal(got)
	}
}

func TestScheduleCancellationRaceHasExactlyOneOutcome(t *testing.T) {
	m := newFeatureMaster(t, 1)
	for range 100 {
		var runs int32
		ctx, cancel := context.WithCancel(context.Background())
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() { <-start; done <- m.ScheduleContext(ctx, countingTask{count: &runs}) }()
		close(start)
		cancel()
		err := awaitError(t, done)
		want := int32(0)
		if err == nil {
			want = 1
		} else if err != context.Canceled {
			t.Fatal(err)
		}
		if got := atomic.LoadInt32(&runs); got != want {
			t.Fatalf("error=%v runs=%d want=%d", err, got, want)
		}
	}
	if err := m.ScheduleContext(testContext(t), phaseErrorTask{}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerShutdownWaitsForActiveTaskAndHandlesUnstartedWorker(t *testing.T) {
	w, err := NewWorker(WithLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := w.Wait(ctx); got != context.Canceled {
		t.Fatalf("unstarted Wait = %v", got)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	gate := newGatedTask(t)
	done := make(chan error, 1)
	go func() { done <- w.Do(gate) }()
	awaitSignal(t, gate.entered)
	if got := w.Shutdown(ctx); got != context.Canceled {
		t.Fatalf("Shutdown = %v", got)
	}
	if got := w.Stats(); !got.Stopped || got.InFlight != 1 {
		t.Fatalf("stats=%+v", got)
	}
	if got := w.Do(phaseErrorTask{}); got != ErrWorkerStopped {
		t.Fatal(got)
	}
	gate.unblock()
	if got := awaitError(t, done); got != nil {
		t.Fatal(got)
	}
	if err := w.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := w.Stats(); got.InFlight != 0 || got.Succeeded != 1 {
		t.Fatalf("stats=%+v", got)
	}

	unstarted, err := NewWorker(WithLogger(zap.NewNop()))
	if err != nil {
		t.Fatal(err)
	}
	if err := unstarted.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestMasterShutdownRejectsWaitingHandoffAndWaitsForDirectTask(t *testing.T) {
	m := newFeatureMaster(t, 1)
	gate := newGatedTask(t)
	direct := make(chan error, 1)
	go func() { direct <- currentWorker(t, m).Do(gate) }()
	awaitSignal(t, gate.entered)
	waiting := make(chan error, 1)
	go func() { waiting <- m.Schedule(phaseErrorTask{}) }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := m.Shutdown(ctx); got != context.Canceled {
		t.Fatalf("Shutdown=%v", got)
	}
	if got := awaitError(t, waiting); got != ErrMasterStopped {
		t.Fatalf("pending handoff=%v", got)
	}
	if got := m.Wait(ctx); got != context.Canceled {
		t.Fatalf("Wait before direct task returns=%v", got)
	}
	gate.unblock()
	if got := awaitError(t, direct); got != nil {
		t.Fatal(got)
	}
	if err := m.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestResultsPreservePhaseCausesAndPanicDetails(t *testing.T) {
	cause := errors.New("original failure")
	tests := []struct {
		name       string
		task       Task
		want       error
		phase      Phase
		cause      error
		panicValue any
	}{
		{"success", phaseErrorTask{id: "success"}, nil, "", nil, nil},
		{"init", phaseErrorTask{id: "init", initErr: cause}, ErrWorkerTaskInit, PhaseInit, cause, nil},
		{"run", phaseErrorTask{id: "run", runErr: cause}, ErrWorkerTaskRun, PhaseRun, cause, nil},
		{"done", phaseErrorTask{id: "done", doneErr: cause}, ErrWorkerTaskDone, PhaseDone, cause, nil},
		{"panic", functionTask{id: "panic", run: func() error { panic(cause) }}, ErrWorkerPanic, PhaseRun, cause, cause},
		{"id panic", &executionTraceTask{panicIDCall: 1}, ErrWorkerPanic, PhaseID, nil, "ID panic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWorker(WithLogger(zap.NewNop()))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = w.Shutdown(testContext(t)) })
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			result := w.DoResult(testContext(t), tt.task)
			if result.Err != tt.want || result.Phase != tt.phase || result.Cause != tt.cause || result.PanicValue != tt.panicValue {
				t.Fatalf("result=%+v", result)
			}
			if tt.phase != PhaseID && result.TaskID != tt.name {
				t.Fatalf("task ID=%q want=%q", result.TaskID, tt.name)
			}
			if tt.want == ErrWorkerPanic && !strings.Contains(result.PanicStack, "worker") {
				t.Fatalf("missing panic stack: %+v", result)
			}
		})
	}
}

func TestGoexitStillCompletesFutureAndAllowsRecovery(t *testing.T) {
	m := newFeatureMaster(t, 1, WithWorkerRecovery(true), WithQueueCapacity(2))
	f := submitTask(t, m, functionTask{run: func() error { runtime.Goexit(); return nil }})
	if result := awaitResult(t, f); result.Err != ErrWorkerPanic || result.Phase != PhaseRun || result.PanicStack == "" {
		t.Fatalf("result=%+v", result)
	}
	if result := m.ScheduleResult(testContext(t), phaseErrorTask{id: "next"}); result.Err != nil {
		t.Fatal(result.Err)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestLoggerAndCumulativeStatsSurviveRecovery(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	m := newFeatureMaster(t, 1, WithWorkerRecovery(true), WithMasterLogger(logger))
	old := currentWorker(t, m)
	cause := errors.New("failure")
	for _, task := range []Task{phaseErrorTask{}, phaseErrorTask{runErr: cause}, functionTask{run: func() error { panic("failed") }}, phaseErrorTask{}} {
		m.ScheduleResult(testContext(t), task)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := m.Stats().TaskStats; got != (TaskStats{Started: 4, Succeeded: 2, Failed: 1, Panicked: 1}) {
		t.Fatalf("stats=%+v", got)
	}
	replacement := currentWorker(t, m)
	if replacement == old || replacement.logger != logger {
		t.Fatal("replacement did not inherit logger")
	}
	if got := logs.FilterMessage(workerEventStart).Len(); got != 2 {
		t.Fatalf("start log count=%d", got)
	}
	if got := replacement.Stats().Succeeded; got != 1 {
		t.Fatalf("replacement successes=%d", got)
	}
}

func TestQueueBoundsAdmissionAndFutureWaitIsReusable(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(2))
	gate := newGatedTask(t)
	active := make(chan error, 1)
	go func() { active <- m.Schedule(gate) }()
	awaitSignal(t, gate.entered)
	admissionCtx, cancelAdmission := context.WithCancel(context.Background())
	first, err := m.Submit(admissionCtx, phaseErrorTask{id: "first"})
	if err != nil {
		t.Fatal(err)
	}
	cancelAdmission()
	second := submitTask(t, m, phaseErrorTask{id: "second"})
	if _, err := m.TrySubmit(phaseErrorTask{}); err != ErrQueueFull {
		t.Fatalf("TrySubmit=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := m.Submit(ctx, phaseErrorTask{}); err != context.DeadlineExceeded {
		t.Fatalf("Submit=%v", err)
	}
	if got := m.Stats().Queued; got != 2 {
		t.Fatalf("queued=%d", got)
	}
	if _, err := first.Wait(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Wait=%v", err)
	}
	select {
	case <-first.Done():
		t.Fatal("waiting canceled the task")
	default:
	}
	gate.unblock()
	if err := awaitError(t, active); err != nil {
		t.Fatal(err)
	}
	const readers = 8
	results := make(chan Result, readers)
	for range readers {
		go func() { r, _ := first.Wait(context.Background()); results <- r }()
	}
	for range readers {
		select {
		case r := <-results:
			if r.Err != nil || r.TaskID != "first" {
				t.Fatalf("result=%+v", r)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("future reader blocked")
		}
	}
	if r := awaitResult(t, second); r.Err != nil || r.TaskID != "second" {
		t.Fatalf("result=%+v", r)
	}
	if r, err := first.Wait(ctx); err != nil || r.TaskID != "first" {
		t.Fatalf("completed future=%+v,%v", r, err)
	}
}

func TestAsyncDispatcherUsesMultipleWorkers(t *testing.T) {
	m := newFeatureMaster(t, 2, WithQueueCapacity(2))
	first, second := newGatedTask(t), newGatedTask(t)
	f1, f2 := submitTask(t, m, first), submitTask(t, m, second)
	awaitSignal(t, first.entered)
	awaitSignal(t, second.entered)
	if got := m.Stats().InFlight; got != 2 {
		t.Fatalf("inflight=%d", got)
	}
	first.unblock()
	second.unblock()
	if r := awaitResult(t, f1); r.Err != nil {
		t.Fatal(r.Err)
	}
	if r := awaitResult(t, f2); r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestShutdownDrainsAcceptedQueueAcrossPanic(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(3), WithWorkerRecovery(true))
	gate := newGatedTask(t)
	first := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	cause := errors.New("failure")
	failed := submitTask(t, m, phaseErrorTask{runErr: cause})
	panicked := submitTask(t, m, functionTask{run: func() error { panic("panic") }})
	last := submitTask(t, m, phaseErrorTask{id: "last"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Shutdown(ctx); err != context.Canceled {
		t.Fatalf("Shutdown=%v", err)
	}
	if _, err := m.TrySubmit(phaseErrorTask{}); err != ErrMasterStopped {
		t.Fatalf("new Submit=%v", err)
	}
	if err := m.Schedule(phaseErrorTask{}); err != ErrMasterStopped {
		t.Fatalf("new Schedule=%v", err)
	}
	gate.unblock()
	if err := m.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		f    *Future
		want error
	}{{first, nil}, {failed, ErrWorkerTaskRun}, {panicked, ErrWorkerPanic}, {last, nil}} {
		if got := awaitResult(t, tt.f).Err; got != tt.want {
			t.Fatalf("result=%v want=%v", got, tt.want)
		}
	}
	if got := m.Stats(); got.Queued != 0 || got.InFlight != 0 || got.Started != 4 || !got.Stopped {
		t.Fatalf("stats=%+v", got)
	}
}

func TestStopAbortsQueuedTasksAndBlockedSubmit(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(1))
	gate := newGatedTask(t)
	active := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	queued := submitTask(t, m, phaseErrorTask{})
	blocked := make(chan error, 1)
	go func() { _, err := m.Submit(context.Background(), phaseErrorTask{}); blocked <- err }()
	m.Stop()
	if err := awaitError(t, blocked); err != ErrMasterStopped {
		t.Fatalf("blocked Submit=%v", err)
	}
	if got := awaitResult(t, queued).Err; got != ErrMasterStopped {
		t.Fatalf("queued=%v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Wait(ctx); err != context.Canceled {
		t.Fatalf("premature Wait=%v", err)
	}
	gate.unblock()
	if got := awaitResult(t, active).Err; got != nil {
		t.Fatal(got)
	}
	if err := m.Wait(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := m.Stats().Started; got != 1 {
		t.Fatalf("started=%d", got)
	}
}

func TestQueueHandsOffInAdmissionOrder(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(8))
	var mu sync.Mutex
	var order []int
	var futures []*Future
	for i := range 8 {
		futures = append(futures, submitTask(t, m, functionTask{run: func() error { mu.Lock(); order = append(order, i); mu.Unlock(); return nil }}))
	}
	for _, f := range futures {
		if r := awaitResult(t, f); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	for i, got := range order {
		if got != i {
			t.Fatalf("order=%v", order)
		}
	}
}

func TestNewAPIValidation(t *testing.T) {
	if w, err := NewWorker(WithLogger(nil)); w != nil || err != ErrInvalidLogger {
		t.Fatalf("nil logger=%v,%v", w, err)
	}
	if m, err := NewMaster(WithWorkerRecovery(true), WithMasterLogger(nil)); m != nil || err != ErrInvalidLogger {
		t.Fatalf("nil master logger=%v,%v", m, err)
	}
	if m, err := NewMaster(WithQueueCapacity(-1)); m != nil || err != ErrInvalidQueueCapacity {
		t.Fatalf("negative capacity=%v,%v", m, err)
	}
	m := newFeatureMaster(t, 1)
	if _, err := m.TrySubmit(phaseErrorTask{}); err != ErrQueueDisabled {
		t.Fatalf("disabled=%v", err)
	}
	var zero Master
	if _, err := zero.Submit(context.Background(), phaseErrorTask{}); err != ErrMasterNotInitialized {
		t.Fatal(err)
	}
	if err := zero.Shutdown(context.Background()); err != ErrMasterNotInitialized {
		t.Fatal(err)
	}
	var w *Worker
	if r := w.DoResult(context.Background(), phaseErrorTask{}); r.Err != ErrWorkerNotInitialized {
		t.Fatal(r.Err)
	}
	if err := w.Wait(context.Background()); err != ErrWorkerNotInitialized {
		t.Fatal(err)
	}
	var f Future
	if _, err := f.Wait(context.Background()); err != ErrFutureNotInitialized {
		t.Fatal(err)
	}
}

func TestConcurrentAdmissionAndShutdown(t *testing.T) {
	m := newFeatureMaster(t, 4, WithQueueCapacity(8), WithWorkerRecovery(true))
	const callers = 80
	start := make(chan struct{})
	results := make(chan error, callers)
	ctx := testContext(t)
	firstRun := make(chan struct{})
	var firstOnce sync.Once
	task := functionTask{run: func() error { firstOnce.Do(func() { close(firstRun) }); runtime.Gosched(); return nil }}
	for i := range callers {
		go func() {
			<-start
			if i%2 == 0 {
				results <- m.ScheduleContext(ctx, task)
				return
			}
			f, err := m.Submit(ctx, task)
			if err != nil {
				results <- err
				return
			}
			r, err := f.Wait(ctx)
			if err == nil {
				err = r.Err
			}
			results <- err
		}()
	}
	close(start)
	awaitSignal(t, firstRun)
	if err := m.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for range callers {
		if err := awaitError(t, results); err != nil && err != ErrMasterStopped {
			t.Fatal(err)
		}
	}
	if got := m.Stats(); got.Queued != 0 || got.InFlight != 0 {
		t.Fatalf("stats=%+v", got)
	}
}

type observedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestWaitingTasksFinishWhenLastWorkerPanicsWithoutRecovery(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(1))
	gate := newGatedTask(t)
	first := make(chan error, 1)
	go func() { first <- m.Schedule(functionTask{run: func() error { _ = gate.Run(); panic("last worker") }}) }()
	awaitSignal(t, gate.entered)
	base, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx := &observedContext{Context: base, observed: make(chan struct{})}
	waiting := make(chan error, 1)
	go func() { waiting <- m.ScheduleContext(ctx, phaseErrorTask{}) }()
	awaitSignal(t, ctx.observed)
	queued := submitTask(t, m, phaseErrorTask{})
	gate.unblock()
	if got := awaitError(t, first); got != ErrWorkerPanic {
		t.Fatalf("first=%v", got)
	}
	if got := awaitError(t, waiting); got != ErrMasterWorkerPoolIsEmpty {
		t.Fatalf("waiting=%v", got)
	}
	if got := awaitResult(t, queued).Err; got != ErrMasterWorkerPoolIsEmpty {
		t.Fatalf("queued=%v", got)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedSubmitResumesWhenQueueHasSpace(t *testing.T) {
	m := newFeatureMaster(t, 1, WithQueueCapacity(1))
	gate := newGatedTask(t)
	active := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	queued := submitTask(t, m, phaseErrorTask{id: "queued"})
	ctx := &observedContext{Context: testContext(t), observed: make(chan struct{})}
	type accepted struct {
		future *Future
		err    error
	}
	done := make(chan accepted, 1)
	go func() { f, err := m.Submit(ctx, phaseErrorTask{id: "blocked"}); done <- accepted{f, err} }()
	awaitSignal(t, ctx.observed)
	gate.unblock()
	select {
	case a := <-done:
		if a.err != nil {
			t.Fatal(a.err)
		}
		if r := awaitResult(t, a.future); r.TaskID != "blocked" || r.Err != nil {
			t.Fatalf("result=%+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Submit did not resume")
	}
	if r := awaitResult(t, active); r.Err != nil {
		t.Fatal(r.Err)
	}
	if r := awaitResult(t, queued); r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestConcurrentStopAndShutdownCompleteOnce(t *testing.T) {
	m := newFeatureMaster(t, 2, WithQueueCapacity(2), WithWorkerRecovery(true))
	gate := newGatedTask(t)
	active := submitTask(t, m, gate)
	awaitSignal(t, gate.entered)
	const callers = 16
	called := make(chan struct{}, callers)
	done := make(chan error, callers)
	ctx := testContext(t)
	for i := range callers {
		go func() {
			called <- struct{}{}
			if i%2 == 0 {
				m.Stop()
				done <- m.Wait(ctx)
			} else {
				done <- m.Shutdown(ctx)
			}
		}()
	}
	for range callers {
		select {
		case <-called:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	gate.unblock()
	for range callers {
		if err := awaitError(t, done); err != nil {
			t.Fatal(err)
		}
	}
	if r := awaitResult(t, active); r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestShutdownUnstartedPool(t *testing.T) {
	m, err := NewMaster(WithMasterLogger(zap.NewNop()), WithWorkerRecovery(true), WithQueueCapacity(1))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Stop()
	if err := m.AddWorkers(2); err != nil {
		t.Fatal(err)
	}
	f := submitTask(t, m, phaseErrorTask{})
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if got := awaitResult(t, f).Err; got != ErrWorkerNotStarted {
		t.Fatalf("result=%v", got)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyNilPanicStillCompletesTask(t *testing.T) {
	t.Setenv("GODEBUG", "panicnil=1")
	m := newFeatureMaster(t, 1, WithQueueCapacity(1), WithWorkerRecovery(true))
	f := submitTask(t, m, nilPanicTask{})
	if r := awaitResult(t, f); r.Err != ErrWorkerPanic || r.Phase != PhaseRun {
		t.Fatalf("result=%+v", r)
	}
	if err := m.Shutdown(testContext(t)); err != nil {
		t.Fatal(err)
	}
}
