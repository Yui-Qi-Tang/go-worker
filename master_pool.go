package worker

import "slices"

// AddWorker adds worker to pool
func (m *Master) AddWorker(worker *Worker) error {
	if worker == nil {
		return ErrMasterAddNilWorker
	}
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if !worker.isInitialized() {
		return ErrWorkerNotInitialized
	}

	if worker.isStopped() {
		return ErrWorkerStopped
	}

	if worker.name == "" {
		return ErrWorkerInvalidName
	}

	if m.hasWorkerIdentityLocked(worker.identity()) {
		return ErrMasterDuplicateWorkerName
	}

	if uint(len(m.pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	m.addWorkerLocked(worker)

	return nil
}

func (m *Master) hasWorkerIdentityLocked(id string) bool {
	for _, worker := range m.pool {
		if worker.identity() == id {
			return true
		}
	}
	return false
}

func (m *Master) addWorkerLocked(worker *Worker) {
	// Master owns recovery signaling after a worker enters the pool.
	worker.mu.Lock()
	worker.recovery = m.workerPanic
	worker.ownerMetrics = &m.metrics
	worker.mu.Unlock()
	if worker.recoveryBudget == nil {
		worker.recoveryBudget = &restartBudget{}
	}
	m.backgroundWG.Go(func() {
		<-worker.done
		// The channel notification is only a fast path. Exit observation also
		// recovers startup/direct-task panics when that notification was dropped.
		if worker.didPanic() && m.workerPanic != nil {
			m.recoverWorker(worker.recoveryIdentity())
		}
	})

	m.pool = append(m.pool, worker)
	m.workerAdded = true
	m.poolErr = nil
	m.signalPoolChangedLocked()

	m.queueWorkerLocked(worker)
}

// AddWorkers creates number of workers with counts; HINT: the workers support recovery
func (m *Master) AddWorkers(counts int) error {
	if counts < 0 {
		return ErrMasterSetupWithInvalidWorkerCount
	}
	if uint(counts) > maxPoolSize {
		return ErrMasterSetupWithTooLargePoolSize
	}
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}

	m.mu.RLock()
	if m.closing || m.stopped {
		m.mu.RUnlock()
		return ErrMasterStopped
	}
	if counts == 0 {
		m.mu.RUnlock()
		return nil
	}
	if uint(len(m.pool)+counts) > maxPoolSize {
		m.mu.RUnlock()
		return ErrMasterWorkerPoolIsFull
	}
	m.mu.RUnlock()

	workers := make([]*Worker, 0, counts)
	for range counts {
		opts := []Option{withRecovery(m.workerPanic != nil)}
		if m.loggerConfigured {
			opts = append(opts, WithLogger(m.logger))
		}
		w, err := NewWorker(opts...)
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if uint(len(m.pool)+len(workers)) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	for _, w := range workers {
		m.addWorkerLocked(w)
	}

	return nil
}

func (m *Master) hasWorker(worker *Worker) bool {
	if worker == nil || !m.isInitialized() {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Contains(m.pool, worker)
}

func (m *Master) hasStartedWorker() bool {
	if !m.isInitialized() {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, worker := range m.pool {
		if worker.isStarted() {
			return true
		}
	}
	return false
}

func (m *Master) removeWorker(id string, expectRecovery bool) bool {
	if !m.isInitialized() {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i, worker := range m.pool {
		if worker.identity() == id {
			m.removeWorkerAtLocked(i, expectRecovery)
			return true
		}
	}

	return false
}

// removeWorkerAtLocked preserves the pending-recovery state while a replacement
// is being created. Callers must hold m's write lock.
func (m *Master) removeWorkerAtLocked(index int, expectRecovery bool) {
	m.pool = append(m.pool[:index], m.pool[index+1:]...)
	if !expectRecovery {
		m.markWorkerPoolEmptyLocked()
	}
	m.signalPoolChangedLocked()
}

func (m *Master) signalPoolChangedLocked() {
	close(m.poolChanged)
	m.poolChanged = make(chan struct{})
}

func (m *Master) markWorkerPoolEmptyLocked() {
	if len(m.pool) == 0 {
		m.workerAdded = false
	}
}

// Workers returns the number of workers in the pool.
func (m *Master) Workers() int {
	if !m.isInitialized() {
		return 0
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pool)
}

// WakeAllWorkersUp weaks all of workers in the pool up
func (m *Master) WakeAllWorkersUp() error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if len(m.pool) == 0 {
		return ErrMasterWorkerPoolIsEmpty
	}

	var wakeErr error
	for i := 0; i < len(m.pool); {
		w := m.pool[i]
		if err := w.Start(); err != nil && err != ErrWorkerAlreadyStarted {
			if err == ErrWorkerStopped {
				m.removeWorkerAtLocked(i, false)
				wakeErr = err
				continue
			}
			return err
		}
		i++
	}

	return wakeErr
}

// Remove by pointer so a stale lease cannot remove a replacement with the same
// registered name. Recovery identity handles the corresponding panic path.
func (m *Master) removeWorkerReference(target *Worker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, w := range m.pool {
		if w == target {
			m.removeWorkerAtLocked(i, false)
			return
		}
	}
}
