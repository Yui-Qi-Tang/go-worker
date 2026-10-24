package worker

import "context"

// Stop rejects new work and returns without waiting. Queued tasks are
// answered with ErrMasterStopped instead of running, and the context of each
// running task is canceled; that task still finishes with its own result.
func (m *Master) Stop() {
	if !m.isInitialized() {
		return
	}
	m.beginShutdown(true)
}

// Shutdown rejects new work, lets workers finish every accepted task, and
// waits for the workers to exit. Synchronous calls still waiting for a worker
// are rejected. The context limits this wait; draining continues after it
// expires. Call Stop to abort queued work and cancel running tasks' contexts.
// Do not wait for shutdown from a task in this same pool.
func (m *Master) Shutdown(ctx context.Context) error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}
	m.beginShutdown(false)
	return m.Wait(ctx)
}

// Wait waits for shutdown to finish, without initiating it.
func (m *Master) Wait(ctx context.Context) error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}
	return waitDone(ctx, m.done)
}

// beginShutdown stops admission and tells every worker to drain, or to abort
// when abort is set. Workers started later begin in the matching state.
func (m *Master) beginShutdown(abort bool) {
	m.mu.Lock()
	first := !m.closing
	m.closing = true
	startAbort := abort && !m.aborting
	m.aborting = m.aborting || abort
	if first {
		close(m.acceptDone)
		m.ready.rejectSchedules(ErrMasterStopped)
		for w := range m.workers {
			w.requestDrain()
		}
	}
	if m.healTimer != nil {
		m.healTimer.Stop()
		m.healTimer = nil
	}
	if startAbort {
		for w := range m.workers {
			w.requestAbort()
			w.cancelTask()
		}
		// Answer queued tasks now rather than when a busy worker frees up.
		m.ready.stop(ErrMasterStopped)
	}
	m.mu.Unlock()

	if first {
		go m.finishShutdown()
	}
}

func (m *Master) finishShutdown() {
	// Producers check acceptDone under admit's read lock, so once the write
	// lock is held no task can be queued and the queue can be closed.
	m.admit.Lock()
	close(m.ready.closed)
	m.admit.Unlock()

	// Workers drain the closed queue before they exit.
	m.running.Wait()
	err := ErrMasterStopped
	if failure := m.poolDown.Load(); failure != nil {
		err = failure.err
	}
	// Only a pool without workers leaves tasks behind.
	m.ready.drain(err)
	close(m.done)
}
