package worker_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	worker "yuki-tang.github.com"
)

type exampleTask struct {
	id  string
	err error
}

func (t exampleTask) ID() string  { return t.id }
func (t exampleTask) Init() error { return nil }
func (t exampleTask) Run() error  { return t.err }
func (t exampleTask) Done() error { return nil }

func ExampleMaster_Submit() {
	m, err := worker.NewMaster(worker.WithQueueCapacity(4), worker.WithMasterLogger(zap.NewNop()))
	if err != nil {
		panic(err)
	}
	defer m.Stop()
	if err := m.AddWorkers(2); err != nil {
		panic(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		panic(err)
	}

	future, err := m.Submit(context.Background(), exampleTask{id: "job-1"})
	if err != nil {
		panic(err)
	}
	result, err := future.Wait(context.Background())
	if err != nil {
		panic(err)
	}
	if err := m.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(result.TaskID, result.Err == nil, m.Stats().Succeeded)
	// Output: job-1 true 1
}

func ExampleWorker_DoResult() {
	w, err := worker.NewWorker(worker.WithLogger(zap.NewNop()))
	if err != nil {
		panic(err)
	}
	if err := w.Start(); err != nil {
		panic(err)
	}
	cause := errors.New("storage unavailable")
	result := w.DoResult(context.Background(), exampleTask{id: "job-2", err: cause})
	if err := w.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println(result.TaskID, result.Phase, result.Err == worker.ErrWorkerTaskRun, errors.Is(result.Cause, cause))
	// Output: job-2 run true true
}

type examplePanicTask struct{ exampleTask }

func (t examplePanicTask) Run() error { panic("job failed") }

func ExampleMaster_RecoveryStats() {
	m, err := worker.NewMaster(
		worker.WithWorkerRecovery(true),
		worker.WithRecoveryPolicy(worker.RecoveryPolicy{MaxRestarts: 1, Window: time.Minute}),
		worker.WithMasterLogger(zap.NewNop()),
	)
	if err != nil {
		panic(err)
	}
	defer m.Stop()
	if err := m.AddWorkers(1); err != nil {
		panic(err)
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		panic(err)
	}
	for range 2 {
		if err := m.Schedule(examplePanicTask{}); err != worker.ErrWorkerPanic {
			panic(err)
		}
	}
	stats := m.RecoveryStats()
	err = m.Schedule(exampleTask{})
	fmt.Println(stats.Attempts, stats.Started, stats.Exhausted, stats.LastFailure.Stage, errors.Is(err, worker.ErrRecoveryExhausted))
	if err := m.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	// Output: 1 1 1 limit true
}
