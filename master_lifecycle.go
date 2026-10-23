package worker

import "context"

// Stop rejects new work and returns without waiting. Workers answer queued
// tasks with ErrMasterStopped instead of running them, and the context of each
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

// beginShutdown moves the pool to draining, or to aborting when abort is set.
// Workers observe the change through acceptCtx and abortCtx.
func (m *Master) beginShutdown(abort bool) {
	m.mu.Lock()
	first := !m.closing
	m.closing = true
	startAbort := abort && !m.aborting
	if abort {
		m.aborting = true
	}
	if m.healTimer != nil {
		m.healTimer.Stop()
		m.healTimer = nil
	}
	if startAbort && m.queue != nil && !m.queueDrained {
		// Answer queued tasks now rather than when a busy worker frees up.
		// The loop ends when finishShutdown closes the queue.
		m.background.Add(1)
		go func() {
			defer m.background.Done()
			for request := range m.queue {
				request.reply(Result{Err: ErrMasterStopped})
			}
		}()
	}
	m.mu.Unlock()

	m.acceptCancel()
	if abort {
		m.abortCancel()
	}
	if first {
		go m.finishShutdown()
	}
}

func (m *Master) finishShutdown() {
	// Producers check acceptCtx under admit's read lock, so once the write
	// lock is held no send can follow and the queue can be closed.
	m.admit.Lock()
	if m.queue != nil {
		close(m.queue)
	}
	m.admit.Unlock()

	// Workers drain the closed queue before they exit.
	m.running.Wait()
	err := ErrMasterStopped
	if failure := m.poolDown.Load(); failure != nil {
		err = failure.err
	}
	if m.queue != nil {
		// Only a pool without workers leaves tasks behind.
		for request := range m.queue {
			request.reply(Result{Err: err})
		}
	}

	m.mu.Lock()
	// No goroutine may join background after this point.
	m.queueDrained = true
	m.stopDrainerLocked()
	m.mu.Unlock()
	m.background.Wait()

	if m.ownsLogger {
		_ = m.logger.Sync()
	}
	m.abortCancel()
	close(m.done)
}
