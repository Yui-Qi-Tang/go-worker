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

	m.Lock()
	defer m.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if !worker.isInitialized() {
		return ErrWorkerNotInitialized
	}

	if worker.isStopped() {
		return ErrWorkerStopped
	}

	if worker.Name == "" {
		return ErrWorkerInvalidName
	}

	if m.hasWorkerIdentityLocked(worker.identity()) {
		return ErrMasterDuplicateWorkerName
	}

	if uint(len(m.Pool)+1) > maxPoolSize {
		return ErrMasterWorkerPoolIsFull
	}

	m.addWorkerLocked(worker)

	return nil
}

func (m *Master) hasWorkerIdentityLocked(id string) bool {
	for _, worker := range m.Pool {
		if worker.identity() == id {
			return true
		}
	}
	return false
}

func (m *Master) addWorkerLocked(worker *Worker) {
	// Master owns recovery signaling after a worker enters the pool.
	worker.Lock()
	worker.Recovery = m.workerPanic
	worker.ownerMetrics = &m.metrics
	worker.Unlock()
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

	m.Pool = append(m.Pool, worker)
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

	m.RLock()
	if m.closing || m.stopped {
		m.RUnlock()
		return ErrMasterStopped
	}
	if counts == 0 {
		m.RUnlock()
		return nil
	}
	if uint(len(m.Pool)+counts) > maxPoolSize {
		m.RUnlock()
		return ErrMasterWorkerPoolIsFull
	}
	m.RUnlock()

	workers := make([]*Worker, 0, counts)
	for range counts {
		opts := []Option{WithRecovery(m.workerPanic != nil)}
		if m.loggerConfigured {
			opts = append(opts, WithLogger(m.logger))
		}
		w, err := NewWorker(opts...)
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}

	m.Lock()
	defer m.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if uint(len(m.Pool)+len(workers)) > maxPoolSize {
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

	m.RLock()
	defer m.RUnlock()
	return slices.Contains(m.Pool, worker)
}

func (m *Master) hasStartedWorker() bool {
	if !m.isInitialized() {
		return false
	}

	m.RLock()
	defer m.RUnlock()

	for _, worker := range m.Pool {
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

	m.Lock()
	defer m.Unlock()

	for i, worker := range m.Pool {
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
	m.Pool = append(m.Pool[:index], m.Pool[index+1:]...)
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
	if len(m.Pool) == 0 {
		m.workerAdded = false
	}
}

// GetWorkers returns number of workers
func (m *Master) GetWorkers() int {
	if !m.isInitialized() {
		return 0
	}

	m.RLock()
	defer m.RUnlock()
	return len(m.Pool)
}

// GetPoolSize returns number of workers in the pool.
func (m *Master) GetPoolSize() int {
	return m.GetWorkers()
}

// WakeAllWorkersUp weaks all of workers in the pool up
func (m *Master) WakeAllWorkersUp() error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}

	m.Lock()
	defer m.Unlock()

	if m.closing || m.stopped {
		return ErrMasterStopped
	}

	if len(m.Pool) == 0 {
		return ErrMasterWorkerPoolIsEmpty
	}

	var wakeErr error
	for i := 0; i < len(m.Pool); {
		w := m.Pool[i]
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
	m.Lock()
	defer m.Unlock()
	for i, w := range m.Pool {
		if w == target {
			m.removeWorkerAtLocked(i, false)
			return
		}
	}
}
