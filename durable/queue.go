package durable

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	metadataBucket = []byte("go-worker-durable-meta")
	jobsBucket     = []byte("go-worker-durable-jobs")
	// readyBucket indexes pending jobs by readyKey; each value is the JobID.
	readyBucket   = []byte("go-worker-durable-ready")
	versionKey    = []byte("schema-version")
	errNoReadyJob = errors.New("no ready durable job")

	// ErrInvalidPurge means Purge was given no state or a non-terminal state.
	ErrInvalidPurge = errors.New("invalid durable purge request")
)

// schemaVersion 2 adds readyBucket. Opening a version 1 file migrates it in
// place; binaries that only support version 1 then reject the file.
const schemaVersion = "2"

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
// A schema version 1 file is migrated to version 2 while opening, in the same
// transaction that validates the schema.
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
		if _, err := tx.CreateBucketIfNotExists(jobsBucket); err != nil {
			return err
		}
		switch version := meta.Get(versionKey); {
		case version == nil:
			if _, err := tx.CreateBucket(readyBucket); err != nil {
				return err
			}
		case string(version) == "1":
			if err := migrateV1(tx); err != nil {
				return err
			}
		case string(version) == schemaVersion:
			if tx.Bucket(readyBucket) == nil {
				return fmt.Errorf("%w: missing ready index", ErrSchema)
			}
		default:
			return ErrSchema
		}
		return meta.Put(versionKey, []byte(schemaVersion))
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
		return saveJob(tx, nil, job)
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

// Jobs lists detached snapshots in JobID order, which is not dispatch order.
// Empty state includes all jobs. It reads every record and does not build Tasks.
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
			// A Running job has no ready index entry to remove.
			job.unsuccessful(Outcome{Interrupted: true, Error: "interrupted before durable completion"}, now)
			if err := saveJob(tx, nil, job); err != nil {
				return err
			}
		}
		return nil
	})
}

// migrateV1 builds the ready index for an existing version 1 file.
func migrateV1(tx *bolt.Tx) error {
	ready, err := tx.CreateBucket(readyBucket)
	if err != nil {
		return err
	}
	return tx.Bucket(jobsBucket).ForEach(func(_, data []byte) error {
		job, err := decodeJob(data)
		if err != nil || job.State != Pending {
			return err
		}
		return ready.Put(readyKey(job), []byte(job.ID))
	})
}

// Purge deletes jobs in the given terminal states (Succeeded, Failed, or
// Unknown) whose last update is before cutoff, and returns how many it
// deleted. At least one state is required, so Unknown jobs kept for
// investigation are removed only when requested. A purged JobID is forgotten:
// a later Enqueue with that ID creates a new job. Purge scans every record and
// may run while Run is active, because Run never changes terminal jobs.
func (q *Queue) Purge(ctx context.Context, cutoff time.Time, states ...State) (int, error) {
	if len(states) == 0 {
		return 0, ErrInvalidPurge
	}
	for _, state := range states {
		switch state {
		case Succeeded, Failed, Unknown:
		default:
			return 0, ErrInvalidPurge
		}
	}
	var purged int
	err := q.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		bucket := tx.Bucket(jobsBucket)
		var ids [][]byte
		// Collect first: changing a bucket during ForEach invalidates its cursor.
		if err := bucket.ForEach(func(key, data []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			job, err := decodeJob(data)
			if err != nil {
				return err
			}
			if job.UpdatedAt.Before(cutoff) && slices.Contains(states, job.State) {
				ids = append(ids, bytes.Clone(key))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, id := range ids {
			if err := bucket.Delete(id); err != nil {
				return err
			}
		}
		purged = len(ids)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return purged, nil
}

func (q *Queue) claim(ctx context.Context) (Job, bool, error) {
	var claimed Job
	now := time.Now().UTC()
	err := q.update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// The earliest index entry is the next job to become ready.
		key, id := tx.Bucket(readyBucket).Cursor().First()
		if key != nil && len(key) < 8 {
			return fmt.Errorf("%w: invalid ready index key", ErrSchema)
		}
		if key == nil || readyTime(key).After(now) {
			// Roll back an empty claim instead of fsyncing an idle transaction.
			return errNoReadyJob
		}
		data := tx.Bucket(jobsBucket).Get(id)
		if data == nil {
			return fmt.Errorf("%w: ready index references a missing job", ErrSchema)
		}
		job, err := decodeJob(data)
		if err != nil {
			return err
		}
		if job.State != Pending {
			return fmt.Errorf("%w: ready index references a %s job", ErrSchema, job.State)
		}
		previous := job
		job.State = Running
		job.Attempts++
		job.UpdatedAt = now
		job.NextAttemptAt = time.Time{}
		claimed = job
		return saveJob(tx, &previous, job)
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
		previous := job
		change(&job)
		return saveJob(tx, &previous, job)
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

// readyKey orders pending jobs by the time they became eligible, then JobID:
// NextAttemptAt when delayed, otherwise the update that made them pending.
func readyKey(job Job) []byte {
	at := job.NextAttemptAt
	if at.IsZero() {
		at = job.UpdatedAt
	}
	key := binary.BigEndian.AppendUint64(make([]byte, 0, 8+len(job.ID)), uint64(at.UnixNano()))
	return append(key, job.ID...)
}

func readyTime(key []byte) time.Time {
	return time.Unix(0, int64(binary.BigEndian.Uint64(key[:8]))).UTC()
}

// saveJob writes job and keeps the ready index equal to the set of pending
// jobs. previous is the stored record being replaced, or nil for none or a
// record that was not pending.
func saveJob(tx *bolt.Tx, previous *Job, job Job) error {
	ready := tx.Bucket(readyBucket)
	if previous != nil && previous.State == Pending {
		if err := ready.Delete(readyKey(*previous)); err != nil {
			return err
		}
	}
	if job.State == Pending {
		if err := ready.Put(readyKey(job), []byte(job.ID)); err != nil {
			return err
		}
	}
	return putJob(tx.Bucket(jobsBucket), job)
}

func putJob(bucket *bolt.Bucket, job Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(job.ID), data)
}
