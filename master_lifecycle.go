package worker

import "context"

// Stop rejects new work and signals workers to stop without waiting. Queued
// tasks receive ErrMasterStopped; tasks already running are allowed to finish.
func (m *Master) Stop() {
	if !m.isInitialized() {
		return
	}
	m.beginShutdown()
	m.stopNow()
}

// Shutdown rejects new work, drains accepted submissions, and waits for workers
// and internal goroutines to exit. Pending synchronous handoffs are rejected.
// The context limits this wait; draining continues after it expires. Call Stop
// to abort queued work. Do not wait for shutdown from a task in this same pool.
func (m *Master) Shutdown(ctx context.Context) error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}
	m.beginShutdown()
	return m.Wait(ctx)
}

// Wait waits for shutdown to finish, without initiating it.
func (m *Master) Wait(ctx context.Context) error {
	if !m.isInitialized() {
		return ErrMasterNotInitialized
	}
	return waitDone(ctx, m.done)
}

func (m *Master) beginShutdown() {
	m.Lock()
	defer m.Unlock()
	if m.closing {
		return
	}
	m.closing = true
	close(m.acceptDone)
	// All tasksWG.Add calls pass the admission check under the same lock.
	go func() {
		m.tasksWG.Wait()
		m.stopNow()
		// No backgroundWG.Add is allowed once stopNow marks m.stopped.
		m.backgroundWG.Wait()
		close(m.done)
	}()
}

func (m *Master) stopNow() {
	m.stopOnce.Do(func() {
		m.Lock()
		defer m.Unlock()
		m.stopped = true
		for _, w := range m.Pool {
			w.Stop()
		}
		m.stopWorkerRecovery()
		close(m.Quit)
	})
}
