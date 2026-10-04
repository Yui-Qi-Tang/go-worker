package durable

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openQueue(t *testing.T) *Queue {
	t.Helper()
	q, err := Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	return q
}

func spec(id string) Spec {
	return Spec{ID: id, Kind: "test", Version: 1, Payload: []byte("payload")}
}

func TestEnqueueSurvivesReopenAndOwnsPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	input := spec("stable-job")
	job, err := q.Enqueue(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	input.Payload[0] = 'x'
	job.Payload[1] = 'x'
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	stored, err := q.Job(context.Background(), "stable-job")
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != Pending || stored.Attempts != 0 || string(stored.Payload) != "payload" || stored.Retry.MaxAttempts != 1 {
		t.Fatalf("stored job = %+v", stored)
	}
	stored.Payload[0] = 'x'
	again, err := q.Job(context.Background(), stored.ID)
	if err != nil || string(again.Payload) != "payload" {
		t.Fatalf("snapshot aliases storage: %+v, %v", again, err)
	}
}

func TestEnqueueDeduplicatesConcurrentRequestsAndRejectsConflicts(t *testing.T) {
	q := openQueue(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			job, err := q.Enqueue(context.Background(), spec("same"))
			if err != nil || job.ID != "same" {
				t.Errorf("enqueue = %+v, %v", job, err)
			}
		})
	}
	wg.Wait()
	jobs, err := q.Jobs(context.Background(), "")
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs = %+v, %v", jobs, err)
	}
	for _, conflicting := range []Spec{
		{ID: "same", Kind: "other", Version: 1, Payload: []byte("payload")},
		{ID: "same", Kind: "test", Version: 2, Payload: []byte("payload")},
		{ID: "same", Kind: "test", Version: 1, Payload: []byte("other")},
		{ID: "same", Kind: "test", Version: 1, Payload: []byte("payload"), Retry: RetryPolicy{Safe: true, MaxAttempts: 2}},
	} {
		if _, err := q.Enqueue(context.Background(), conflicting); !errors.Is(err, ErrConflict) {
			t.Fatalf("conflicting enqueue = %v", err)
		}
	}
	unchanged, err := q.Job(context.Background(), "same")
	if err != nil || !bytes.Equal(unchanged.Payload, []byte("payload")) || unchanged.Retry.MaxAttempts != 1 {
		t.Fatalf("conflict changed job: %+v, %v", unchanged, err)
	}
}

func TestEnqueueValidationCancellationAndGeneratedIDs(t *testing.T) {
	q := openQueue(t)
	for _, invalid := range []Spec{
		{Kind: "test"},
		{Version: 1},
		{Kind: "test", Version: 1, Retry: RetryPolicy{MaxAttempts: -1}},
		{Kind: "test", Version: 1, Retry: RetryPolicy{MaxAttempts: 2}},
		{Kind: "test", Version: 1, Retry: RetryPolicy{Delay: -1}},
	} {
		if _, err := q.Enqueue(context.Background(), invalid); !errors.Is(err, ErrInvalidJob) {
			t.Fatalf("invalid enqueue = %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := q.Enqueue(ctx, spec("canceled")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := q.Job(context.Background(), "canceled"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := q.Jobs(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	a, err := q.Enqueue(context.Background(), spec(""))
	if err != nil {
		t.Fatal(err)
	}
	b, err := q.Enqueue(context.Background(), spec(""))
	if err != nil || a.ID == "" || b.ID == "" || a.ID == b.ID {
		t.Fatalf("generated IDs = %q, %q; %v", a.ID, b.ID, err)
	}
}

func TestFileOwnershipAndClosedEnqueue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	q, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(path); !errors.Is(err, bolt.ErrTimeout) {
		if other != nil {
			if closeErr := other.Close(); closeErr != nil {
				t.Error(closeErr)
			}
		}
		t.Fatalf("second owner = %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(context.Background(), spec("not-accepted")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed enqueue = %v", err)
	}
	q, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := q.Job(context.Background(), "not-accepted"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed enqueue persisted a job: %v", err)
	}
}

func TestUnsupportedSchemaAndCorruptRecordDoNotResetQueue(t *testing.T) {
	for _, corruption := range []string{"schema", "record"} {
		t.Run(corruption, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			q, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := q.Enqueue(context.Background(), spec("retained")); err != nil {
				t.Fatal(err)
			}
			if err := q.db.Update(func(tx *bolt.Tx) error {
				if corruption == "schema" {
					return tx.Bucket(metadataBucket).Put(versionKey, []byte("999"))
				}
				return tx.Bucket(jobsBucket).Put([]byte("corrupt"), []byte("null"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(path); !errors.Is(err, ErrSchema) {
				if reopened != nil {
					if closeErr := reopened.Close(); closeErr != nil {
						t.Error(closeErr)
					}
				}
				t.Fatalf("corrupt open = %v", err)
			}
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := db.View(func(tx *bolt.Tx) error {
				if tx.Bucket(jobsBucket).Get([]byte("retained")) == nil {
					t.Error("original job was lost")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
