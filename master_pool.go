package worker

import "time"

// Workers returns the number of running workers.
func (m *Master) Workers() int {
	if !m.isInitialized() {
		return 0
	}
	return int(m.live.Load())
}

// spawnLocked creates and starts one pool worker. Callers hold m.mu.
func (m *Master) spawnLocked() error {
	w, err := m.newWorker(m)
	if err != nil {
		return err
	}
	w.ownerMetrics = &m.metrics
	w.started = true
	m.workers[w] = struct{}{}
	m.live.Store(int32(len(m.workers)))
	// The caller's worker, if any, is still counted, so running never reaches
	// zero while a replacement is added during shutdown.
	m.running.Add(1)
	go func() {
		defer m.running.Done()
		w.run()
	}()
	if m.poolDown.Load() != nil {
		m.poolDown.Store(nil)
		m.stopDrainerLocked()
	}
	return nil
}

func (m *Master) removeLocked(w *Worker) {
	delete(m.workers, w)
	m.live.Store(int32(len(m.workers)))
}

// grow adds a worker when more tasks wait than workers are idle and the pool
// is below its maximum. demand counts the waiting tasks, including one being
// admitted. It returns the pool's error if no worker can run.
func (m *Master) grow(demand int) error {
	if !m.scales() || int(m.idle.Load()) >= demand || int(m.live.Load()) >= m.maxWorkers {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || int(m.idle.Load()) >= demand || len(m.workers) >= m.maxWorkers {
		return nil
	}
	if m.lost > 0 && m.healTimer != nil {
		// The restart limit was reached; the heal timer adds the worker.
		return nil
	}
	m.startLocked("")
	if failure := m.poolDown.Load(); failure != nil {
		return failure.err
	}
	return nil
}

// retire removes an idle worker if more than the minimum are running.
func (m *Master) retire(w *Worker) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || len(m.workers) <= m.minWorkers {
		return false
	}
	m.removeLocked(w)
	return true
}

// workerExited runs on the exiting worker's goroutine. A panicked worker is
// replaced unless the pool is aborting.
func (m *Master) workerExited(w *Worker, panicked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeLocked(w)
	if panicked && !m.aborting {
		m.lost++
		m.restoreLocked(w.name)
	}
}

// restoreLocked starts workers until the pool is back at its minimum, or at
// one worker while the pool is down, stopping early if the restart budget or
// worker creation fails.
func (m *Master) restoreLocked(lostWorker string) {
	for {
		target := m.minWorkers
		if target == 0 && m.poolDown.Load() != nil {
			target = 1
		}
		if len(m.workers) >= target {
			return
		}
		if !m.startLocked(lostWorker) {
			return
		}
	}
}

// startLocked starts one worker. While panicked workers are unreplaced, the
// new worker is a replacement: it spends the restart budget, and a denial
// schedules another try. It reports whether a worker started.
func (m *Master) startLocked(lostWorker string) bool {
	now := time.Now()
	replacing := m.lost > 0
	if replacing {
		if !m.budget.allow(now, m.recoveryPolicy) {
			m.recoveryStats.Exhausted++
			m.recordFailureLocked(lostWorker, "limit", ErrRecoveryExhausted, now)
			m.scheduleHealLocked(m.budget.nextAllowed(m.recoveryPolicy).Sub(now))
			m.markDownLocked(ErrRecoveryExhausted)
			return false
		}
		m.recoveryStats.Attempts++
	}
	if err := m.spawnLocked(); err != nil {
		if replacing {
			m.recoveryStats.Failed++
			m.recordFailureLocked(lostWorker, "create", err, now)
			m.scheduleHealLocked(m.budget.nextAllowed(m.recoveryPolicy).Sub(now))
		}
		m.markDownLocked(ErrRecoveryFailed)
		return false
	}
	if replacing {
		m.lost--
		m.recoveryStats.Started++
	}
	return true
}

func (m *Master) recordFailureLocked(worker, stage string, err error, now time.Time) {
	m.recoveryStats.LastFailure = RecoveryFailure{WorkerID: worker, Stage: stage, Err: err, Time: now}
}

// scheduleHealLocked retries restoreLocked after d, unless shutdown began.
func (m *Master) scheduleHealLocked(d time.Duration) {
	if m.closing {
		return
	}
	if m.healTimer != nil {
		m.healTimer.Stop()
	}
	m.healTimer = time.AfterFunc(d, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.healTimer = nil
		if !m.closing {
			m.restoreLocked("")
		}
	})
}

// markDownLocked starts a drainer when no worker is left, so callers get err
// instead of waiting. A started worker clears it in spawnLocked.
func (m *Master) markDownLocked(err error) {
	if len(m.workers) > 0 {
		return
	}
	m.poolDown.Store(&poolFailure{err: err})
	if m.drainStop != nil {
		return
	}
	stop := make(chan struct{})
	m.drainStop = stop
	m.background.Add(1)
	go func() {
		defer m.background.Done()
		m.drain(stop, err)
	}()
}

func (m *Master) stopDrainerLocked() {
	if m.drainStop != nil {
		close(m.drainStop)
		m.drainStop = nil
	}
}

// drain answers every request with err while the pool has no worker.
func (m *Master) drain(stop <-chan struct{}, err error) {
	for {
		select {
		case <-stop:
			return
		case request := <-m.direct:
			request.reply(Result{Err: err})
		case request, ok := <-m.queue:
			if !ok {
				return
			}
			request.reply(Result{Err: err})
		}
	}
}
