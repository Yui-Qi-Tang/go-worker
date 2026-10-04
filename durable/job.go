package durable

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	worker "yuki-tang.github.com"
)

// State describes a persisted job, rather than a Worker's health.
type State string

const (
	// Pending means the job is waiting for a dispatch attempt.
	Pending State = "pending"
	// Running means an attempt has been claimed, possibly awaiting admission.
	Running State = "running"
	// Succeeded means all Task phases returned successfully and this was committed.
	Succeeded State = "succeeded"
	// Failed means a Task returned an error and no automatic retry remains.
	Failed State = "failed"
	// Unknown means an interrupted or panicked attempt has an uncertain outcome.
	Unknown State = "unknown"
)

var (
	// ErrInvalidJob identifies an invalid job specification.
	ErrInvalidJob = errors.New("invalid durable job")
	// ErrConflict means an existing JobID has a different immutable specification.
	ErrConflict = errors.New("durable job ID conflict")
	// ErrNotFound means the JobID does not exist.
	ErrNotFound = errors.New("durable job not found")
	// ErrClosed means the queue's database is closed or uninitialized.
	ErrClosed = errors.New("durable queue closed")
	// ErrRunning means the queue already has a runner, or cannot yet be closed.
	ErrRunning = errors.New("durable queue running")
	// ErrSchema means the file uses an unsupported durable schema.
	ErrSchema = errors.New("unsupported durable schema")
	// ErrInvalidRunner identifies an invalid Master, Builder, or concurrency.
	ErrInvalidRunner = errors.New("invalid durable runner")
)

// RetryPolicy bounds Task attempts independently of Worker replacement budgets.
type RetryPolicy struct {
	// MaxAttempts includes the initial attempt; zero defaults to one.
	MaxAttempts int
	// Safe declares that repeat execution is safe through idempotency or deduplication.
	// This declaration does not itself provide either guarantee.
	Safe bool
	// Delay is the minimum wait before automatically retrying an unsuccessful attempt.
	Delay time.Duration
}

// Spec contains immutable data from which the caller can reconstruct a Task.
type Spec struct {
	// ID is a JobID independent of Task.ID. Empty generates a new JobID.
	// Supply a stable ID when an enqueue request may be retried after a lost reply.
	ID      string
	Kind    string
	Version uint
	Payload []byte
	Retry   RetryPolicy
}

// Outcome stores diagnostic data without serializing live error or panic objects.
// A failed or unknown outcome does not imply rollback of external effects.
type Outcome struct {
	Phase       worker.Phase
	Error       string
	Cause       string
	Panicked    bool
	PanicValue  string
	PanicStack  string
	Interrupted bool
}

// Job is a detached snapshot of a persisted job.
type Job struct {
	Spec
	State         State
	Attempts      int
	CreatedAt     time.Time
	UpdatedAt     time.Time
	NextAttemptAt time.Time
	LastOutcome   Outcome
}

func normalizeSpec(spec Spec) (Spec, error) {
	if spec.Kind == "" || spec.Version == 0 || spec.Retry.MaxAttempts < 0 || spec.Retry.Delay < 0 {
		return Spec{}, ErrInvalidJob
	}
	if spec.Retry.MaxAttempts == 0 {
		spec.Retry.MaxAttempts = 1
	}
	if spec.Retry.MaxAttempts > 1 && !spec.Retry.Safe {
		return Spec{}, fmt.Errorf("%w: retries require an explicit safety declaration", ErrInvalidJob)
	}
	spec.Payload = bytes.Clone(spec.Payload)
	return spec, nil
}

func sameSpec(a, b Spec) bool {
	return a.ID == b.ID && a.Kind == b.Kind && a.Version == b.Version &&
		bytes.Equal(a.Payload, b.Payload) && a.Retry == b.Retry
}

func (j *Job) unsuccessful(outcome Outcome, now time.Time) {
	j.LastOutcome = outcome
	j.UpdatedAt = now
	j.NextAttemptAt = time.Time{}
	j.State = Failed
	if outcome.Interrupted || outcome.Panicked {
		j.State = Unknown
	}
	if j.Retry.Safe && j.Attempts < j.Retry.MaxAttempts {
		j.State = Pending
		j.NextAttemptAt = now.Add(j.Retry.Delay)
	}
}
