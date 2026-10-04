package durable

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	metadataBucket = []byte("go-worker-durable-meta")
	jobsBucket     = []byte("go-worker-durable-jobs")
	versionKey     = []byte("schema-version")
	errNoReadyJob  = errors.New("no ready durable job")
)

// Queue owns a synchronous bbolt database and permits one active runner.
// The OS file lock also prevents another process from owning the same file.
type Queue struct {
	db      *bolt.DB
	mu      sync.Mutex
	running bool
	closed  bool
	changed chan struct{}
}

// Open creates or opens a queue and reconciles interrupted Running attempts.
// It waits at most one second for the exclusive database file lock. The parent
// directory must exist. Sync-on-commit is enabled and is not configurable here.
func Open(path string) (*Queue, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	q := &Queue{db: db, changed: make(chan struct{})}
	err = q.update(func(tx *bolt.Tx) error {
		meta, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		if version := meta.Get(versionKey); version != nil && string(version) != "1" {
			return ErrSchema
		}
		if err := meta.Put(versionKey, []byte("1")); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(jobsBucket)
		return err
	})
	if err == nil {
		err = q.recoverInterrupted()
	}
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return q, nil
}

// Close releases the file lock. An active runner must first be canceled and
// awaited; Close refuses to abandon its accepted Tasks.
func (q *Queue) Close() error {
	if q == nil || q.db == nil {
		return ErrClosed
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.running {
		return ErrRunning
	}
	if q.closed {
		return nil
	}
	q.closed = true
	close(q.changed)
	return q.db.Close()
}

// Enqueue accepts a job only after a synchronous transaction commits. It does
// not consult a Master. Repeating the same ID and specification returns the
// existing record, including its current outcome; differing data returns ErrConflict.
// Cancellation is checked before writing, not after a successful commit.
func (q *Queue) Enqueue(ctx context.Context, spec Spec) (Job, error) {
	spec, err := normalizeSpec(spec)
	if err != nil {
		return Job{}, err
	}
	if spec.ID == "" {
		spec.ID = rand.Text()
	}
	now := time.Now().UTC()
	job := Job{Spec: spec, State: Pending, CreatedAt: now, UpdatedAt: now}
	err = q.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket := tx.Bucket(jobsBucket)
		if data := bucket.Get([]byte(spec.ID)); data != nil {
			existing, err := decodeJob(data)
			if err != nil {
				return err
			}
			if !sameSpec(existing.Spec, spec) {
				return ErrConflict
			}
			job = existing
			return nil
		}
		return putJob(bucket, job)
	})
	if err != nil {
		return Job{}, err
	}
	q.signal()
	return job, nil
}

// Job returns the current persisted snapshot for an ID.
func (q *Queue) Job(ctx context.Context, id string) (Job, error) {
	var job Job
	err := q.view(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		data := tx.Bucket(jobsBucket).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		var err error
		job, err = decodeJob(data)
		return err
	})
	return job, err
}

// Jobs lists detached snapshots in JobID order. Empty state includes all jobs.
// This small-queue API does not imply FIFO dispatch or load the Tasks themselves.
func (q *Queue) Jobs(ctx context.Context, state State) ([]Job, error) {
	var jobs []Job
	err := q.view(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return tx.Bucket(jobsBucket).ForEach(func(_, data []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			job, err := decodeJob(data)
			if err != nil {
				return err
			}
			if state == "" || job.State == state {
				jobs = append(jobs, job)
			}
			return nil
		})
	})
	return jobs, err
}

func (q *Queue) recoverInterrupted() error {
	return q.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(jobsBucket)
		var interrupted []Job
		// Collect first: changing a bucket during ForEach invalidates its cursor.
		if err := bucket.ForEach(func(_, data []byte) error {
			job, err := decodeJob(data)
			if err != nil {
				return err
			}
			if job.State == Running {
				interrupted = append(interrupted, job)
			}
			return nil
		}); err != nil {
			return err
		}
		now := time.Now().UTC()
		for _, job := range interrupted {
			job.unsuccessful(Outcome{Interrupted: true, Error: "interrupted before durable completion"}, now)
			if err := putJob(bucket, job); err != nil {
				return err
			}
		}
		return nil
	})
}

func (q *Queue) claim(ctx context.Context) (Job, bool, error) {
	var claimed Job
	now := time.Now().UTC()
	err := q.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket := tx.Bucket(jobsBucket)
		cursor := bucket.Cursor()
		for key, data := cursor.First(); key != nil; key, data = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			job, err := decodeJob(data)
			if err != nil {
				return err
			}
			if job.State != Pending || job.NextAttemptAt.After(now) {
				continue
			}
			job.State = Running
			job.Attempts++
			job.UpdatedAt = now
			job.NextAttemptAt = time.Time{}
			claimed = job
			return putJob(bucket, job)
		}
		// Roll back an empty scan instead of fsyncing an idle transaction.
		return errNoReadyJob
	})
	if errors.Is(err, errNoReadyJob) {
		return Job{}, false, nil
	}
	if err == nil {
		q.signal()
	}
	return claimed, err == nil, err
}

func (q *Queue) changeJob(id string, change func(*Job)) error {
	err := q.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(jobsBucket)
		data := bucket.Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		job, err := decodeJob(data)
		if err != nil {
			return err
		}
		change(&job)
		return putJob(bucket, job)
	})
	if err == nil {
		q.signal()
	}
	return err
}

func (q *Queue) signal() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		close(q.changed)
		q.changed = make(chan struct{})
	}
}

func (q *Queue) update(fn func(*bolt.Tx) error) error {
	if q == nil || q.db == nil {
		return ErrClosed
	}
	return databaseError(q.db.Update(fn))
}

func (q *Queue) view(fn func(*bolt.Tx) error) error {
	if q == nil || q.db == nil {
		return ErrClosed
	}
	return databaseError(q.db.View(fn))
}

func databaseError(err error) error {
	if errors.Is(err, bolt.ErrDatabaseNotOpen) {
		return ErrClosed
	}
	return err
}

func decodeJob(data []byte) (Job, error) {
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return Job{}, fmt.Errorf("reading durable job: %w", err)
	}
	if job.ID == "" || job.Kind == "" || job.Version == 0 || job.Retry.MaxAttempts < 1 ||
		job.Attempts < 0 || job.Attempts > job.Retry.MaxAttempts || job.Retry.Delay < 0 ||
		(job.Retry.MaxAttempts > 1 && !job.Retry.Safe) {
		return Job{}, fmt.Errorf("%w: invalid job record", ErrSchema)
	}
	switch job.State {
	case Pending, Running, Succeeded, Failed, Unknown:
	default:
		return Job{}, fmt.Errorf("%w: invalid job state", ErrSchema)
	}
	return job, nil
}

func putJob(bucket *bolt.Bucket, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(job.ID), data)
}
