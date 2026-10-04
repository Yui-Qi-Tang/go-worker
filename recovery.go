package worker

import (
	"errors"
	"time"
)

var (
	// ErrInvalidRecoveryPolicy means a restart limit or window is not positive.
	ErrInvalidRecoveryPolicy = errors.New("recovery limit and window must be positive")
	// ErrRecoveryFailed means the last worker could not be replaced. The original
	// failure is available in Master.RecoveryStats().LastFailure.
	ErrRecoveryFailed = errors.New("worker recovery failed")
	// ErrRecoveryExhausted means the last worker exceeded its restart budget.
	ErrRecoveryExhausted = errors.New("worker recovery restart limit reached")
)

// RecoveryPolicy bounds replacement attempts for each registered worker slot.
// Attempts are counted across replacement instances in a rolling Window.
// Both fields must be positive. The default is 5 attempts per minute.
type RecoveryPolicy struct {
	MaxRestarts int
	Window      time.Duration
}

// WithRecoveryPolicy configures the restart limit; WithWorkerRecovery(true)
// enables recovery. Exhausted slots require a newly added worker to resume.
func WithRecoveryPolicy(policy RecoveryPolicy) MasterOption {
	return func(m *Master) { m.recoveryPolicy = policy }
}

// RecoveryFailure retains the latest unsuccessful worker replacement.
// Stage is "create", "start", "capacity", or "limit". Err is the original
// error, or ErrRecoveryExhausted for a denied attempt. Error values are shared
// with the caller and should not be mutated.
type RecoveryFailure struct {
	WorkerID   string
	RecoveryID string
	Stage      string
	Err        error
	Time       time.Time
}

// RecoveryStats counts replacements across all slots and instances.
// Attempts equals Started + Failed; Exhausted counts attempts denied by the
// limit. Started counts replacements started and registered successfully, not
// workers that remain healthy.
// LastFailure is zero until a failure and is retained after later successes.
type RecoveryStats struct {
	Attempts    uint64
	Started     uint64
	Failed      uint64
	Exhausted   uint64
	LastFailure RecoveryFailure
}

// RecoveryStats returns a consistent snapshot, including the original error
// when replacement failed. It returns zero for an uninitialized master.
func (m *Master) RecoveryStats() RecoveryStats {
	if !m.isInitialized() {
		return RecoveryStats{}
	}
	m.RLock()
	defer m.RUnlock()
	return m.recoveryStats
}

// restartBudget belongs to a logical slot and is guarded by its master's lock.
type restartBudget struct {
	attempts []time.Time
}

func (b *restartBudget) allow(now time.Time, policy RecoveryPolicy) bool {
	cutoff := now.Add(-policy.Window)
	first := 0
	for first < len(b.attempts) && !b.attempts[first].After(cutoff) {
		first++
	}
	b.attempts = b.attempts[first:]
	if len(b.attempts) >= policy.MaxRestarts {
		return false
	}
	b.attempts = append(b.attempts, now)
	return true
}
