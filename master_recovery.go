package worker

import "time"

func (m *Master) recoverWorker(id string) bool {
	if !m.isInitialized() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, oldWorker := range m.pool {
		if oldWorker.recoveryIdentity() == id {
			m.removeWorkerAtLocked(i, true)

			if m.stopped {
				m.markWorkerPoolEmptyLocked()
				return true
			}

			now := time.Now()
			if !oldWorker.recoveryBudget.allow(now, m.recoveryPolicy) {
				m.recordRecoveryFailureLocked(oldWorker, "limit", ErrRecoveryExhausted, now)
				return true
			}
			m.recoveryStats.Attempts++
			worker, err := m.newRecoveryWorker(WithName(oldWorker.identity()), withRecovery(true), WithLogger(oldWorker.logger))
			if err != nil {
				m.recordRecoveryFailureLocked(oldWorker, "create", err, now)
				return true
			}

			worker.loggerConfigured = oldWorker.loggerConfigured
			worker.recoveryBudget = oldWorker.recoveryBudget
			if err := worker.Start(); err != nil {
				worker.Stop()
				m.recordRecoveryFailureLocked(oldWorker, "start", err, now)
				return true
			}

			if uint(len(m.pool)+1) > maxPoolSize {
				worker.Stop()
				m.recordRecoveryFailureLocked(oldWorker, "capacity", ErrMasterWorkerPoolIsFull, now)
				return true
			}

			m.addWorkerLocked(worker)
			m.recoveryStats.Started++
			return true
		}
	}

	return false
}

func (m *Master) recordRecoveryFailureLocked(worker *Worker, stage string, err error, now time.Time) {
	poolErr := ErrRecoveryFailed
	if stage == "limit" {
		m.recoveryStats.Exhausted++
		poolErr = ErrRecoveryExhausted
	} else {
		m.recoveryStats.Failed++
	}
	m.recoveryStats.LastFailure = RecoveryFailure{
		WorkerID: worker.identity(), RecoveryID: worker.recoveryIdentity(),
		Stage: stage, Err: err, Time: now,
	}
	m.markWorkerPoolEmptyLocked()
	if len(m.pool) == 0 {
		m.poolErr = poolErr
	}
	// State publication and notification share the lock used by dispatchState.
	m.signalPoolChangedLocked()
}

// recoveryLoop re-creates a new worker when receives worker panic
func (m *Master) recoveryLoop() {
	if !m.isRecoveryInitialized() {
		return
	}

	for {
		select {
		case id, ok := <-m.workerPanic:
			if !ok {
				return
			}
			m.recoverWorker(id)
		case <-m.stopRecoveryRoutine:
			return
		}
	}
}

func (m *Master) isRecoveryInitialized() bool {
	return m != nil && m.workerPanic != nil && m.stopRecoveryRoutine != nil
}

func (m *Master) stopWorkerRecovery() {
	if m.stopRecoveryRoutine != nil {
		close(m.stopRecoveryRoutine)
	}
}
