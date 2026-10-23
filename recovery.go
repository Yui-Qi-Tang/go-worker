package worker

import (
	"errors"
	"time"
)

var (
	// ErrInvalidRecoveryPolicy means a restart limit or window is not positive.
	ErrInvalidRecoveryPolicy = errors.New("recovery limit and window must be positive")
	// ErrRecoveryFailed means no worker is running because a replacement could
	// not be created. The master retries; the original failure is in
	// Master.RecoveryStats().LastFailure.
	ErrRecoveryFailed = errors.New("worker recovery failed")
	// ErrRecoveryExhausted means no worker is running because the restart limit
	// was reached. The master restores workers when the window allows.
	ErrRecoveryExhausted = errors.New("worker recovery restart limit reached")
)

// RecoveryPolicy limits how often the master replaces workers that panic:
// at most MaxRestarts replacements across the pool in any rolling Window.
// When the limit is reached, replacement waits until the window allows it.
// Both fields must be positive. The default is 5 replacements per minute.
type RecoveryPolicy struct {
	MaxRestarts int
	Window      time.Duration
}

// WithRecoveryPolicy sets the restart limit for workers that panic.
func WithRecoveryPolicy(policy RecoveryPolicy) MasterOption {
	return func(m *Master) { m.recoveryPolicy = policy }
}

// RecoveryFailure retains the latest unsuccessful worker replacement.
// Stage is "create" or "limit". Err is the original error, or
// ErrRecoveryExhausted for a denied attempt. WorkerID names the worker that
// panicked, or is empty for a delayed retry. Error values are shared with the
// caller and should not be mutated.
type RecoveryFailure struct {
	WorkerID string
	Stage    string
	Err      error
	Time     time.Time
}

// RecoveryStats counts replacements across the pool. Attempts equals
// Started + Failed; Exhausted counts attempts denied by the limit. Started
// counts replacements that began running, not workers that remain healthy.
// LastFailure is zero until a failure and is retained after later successes.
type RecoveryStats struct {
	Attempts    uint64
	Started     uint64
	Failed      uint64
	Exhausted   uint64
	LastFailure RecoveryFailure
}

// RecoveryStats returns a consistent snapshot. It returns zero for an
// uninitialized master.
func (m *Master) RecoveryStats() RecoveryStats {
	if !m.isInitialized() {
		return RecoveryStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recoveryStats
}

// restartBudget records replacement times in the rolling window. The master's
// mutex guards it.
type restartBudget struct {
	attempts []time.Time
}

func (b *restartBudget) allow(now time.Time, policy RecoveryPolicy) bool {
	b.expire(now, policy)
	if len(b.attempts) >= policy.MaxRestarts {
		return false
	}
	b.attempts = append(b.attempts, now)
	return true
}

// nextAllowed returns when allow can next succeed.
func (b *restartBudget) nextAllowed(policy RecoveryPolicy) time.Time {
	if len(b.attempts) < policy.MaxRestarts {
		return time.Now()
	}
	// The oldest attempt leaves the window just after Window has passed.
	return b.attempts[0].Add(policy.Window + time.Nanosecond)
}

func (b *restartBudget) expire(now time.Time, policy RecoveryPolicy) {
	cutoff := now.Add(-policy.Window)
	first := 0
	for first < len(b.attempts) && !b.attempts[first].After(cutoff) {
		first++
	}
	b.attempts = b.attempts[first:]
}
