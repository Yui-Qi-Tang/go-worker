package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	worker "yuki-tang.github.com"
	"yuki-tang.github.com/durable"
)

// writeTask completes and syncs the required file write before returning. Writing
// the same contents to this dedicated path again is safe for this example.
type writeTask struct {
	path string
	text string
}

func (t *writeTask) ID() string { return t.path }
func (*writeTask) Init() error  { return nil }
func (*writeTask) Done() error  { return nil }
func (t *writeTask) Run() error {
	file, err := os.OpenFile(t.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(t.text)
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func buildWrite(job durable.Job) (worker.Task, error) {
	if job.Kind != "write-file" || job.Version != 1 {
		return nil, fmt.Errorf("unsupported job kind/version: %s/%d", job.Kind, job.Version)
	}
	var data struct{ Path, Text string }
	if err := json.Unmarshal(job.Payload, &data); err != nil {
		return nil, err
	}
	return &writeTask{path: data.Path, text: data.Text}, nil
}

func runExample(dir string) (err error) {
	q, err := durable.Open(filepath.Join(dir, "jobs.db"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()
	payload, err := json.Marshal(struct{ Path, Text string }{filepath.Join(dir, "output"), "hello"})
	if err != nil {
		return err
	}
	accepted, err := q.Enqueue(context.Background(), durable.Spec{
		ID: "request-123", Kind: "write-file", Version: 1, Payload: payload,
		Retry: durable.RetryPolicy{Safe: true, MaxAttempts: 2},
	})
	if err != nil {
		return err
	}
	// This job is already durable, even though the Master does not exist yet.
	fmt.Println(accepted.State)

	m, err := worker.NewMaster(worker.WithQueueCapacity(1), worker.WithMasterLogger(zap.NewNop()))
	if err != nil {
		return err
	}
	defer m.Stop()
	if err := m.AddWorkers(1); err != nil {
		return err
	}
	if err := m.WakeAllWorkersUp(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	finished := make(chan error, 1)
	go func() { finished <- q.Run(ctx, m, buildWrite, 1) }()
	defer func() {
		cancel()
		if runErr := <-finished; runErr != nil && !errors.Is(runErr, context.Canceled) {
			err = errors.Join(err, runErr)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		job, err := q.Job(context.Background(), accepted.ID)
		if err != nil {
			return err
		}
		if job.State == durable.Succeeded {
			fmt.Println(job.State)
			return nil
		}
		if job.State == durable.Failed || job.State == durable.Unknown {
			return fmt.Errorf("job %s: %s", job.ID, job.LastOutcome.Error)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func ExampleQueue() {
	dir, err := os.MkdirTemp("", "go-worker-durable-example-")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}()
	if err := runExample(dir); err != nil {
		panic(err)
	}
	// Output:
	// pending
	// succeeded
}
