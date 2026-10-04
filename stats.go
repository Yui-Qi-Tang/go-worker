package worker

import "sync"

// TaskStats counts executions, including direct Worker.Task submissions.
// Pre-execution rejections are excluded. Started always equals Succeeded +
// Failed + Panicked + InFlight within a snapshot.
type TaskStats struct {
	Started   uint64
	Succeeded uint64
	Failed    uint64
	Panicked  uint64
	InFlight  uint64
}

type taskMetrics struct {
	mu    sync.Mutex
	stats TaskStats
}

func (m *taskMetrics) begin() {
	m.mu.Lock()
	m.stats.Started++
	m.stats.InFlight++
	m.mu.Unlock()
}

func (m *taskMetrics) finish(result Result) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stats.InFlight--
	switch result.Err {
	case nil:
		m.stats.Succeeded++
	case ErrWorkerPanic:
		m.stats.Panicked++
	default:
		m.stats.Failed++
	}
}

func (m *taskMetrics) snapshot() TaskStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

// WorkerStats is a snapshot of one worker's lifecycle and executions.
// Running means Start was called and the worker has not been stopped.
// InFlight can remain nonzero after Stop, until the current task returns.
type WorkerStats struct {
	TaskStats
	Running bool
	Stopped bool
}

// Stats returns a snapshot without exposing mutable worker state.
func (w *Worker) Stats() WorkerStats {
	if !w.isInitialized() {
		return WorkerStats{}
	}
	w.Lock()
	defer w.Unlock()
	return WorkerStats{TaskStats: w.metrics.snapshot(), Running: w.started && !w.stopped, Stopped: w.stopped}
}

// MasterStats reports the pool and its executions. Execution counters survive
// worker replacement and include direct tasks started after registration.
// Queued includes the head being offered to a worker; execution counters and
// pool/queue fields are sampled separately, not as a global atomic snapshot.
type MasterStats struct {
	TaskStats
	Workers       int
	Queued        int
	QueueCapacity int
	Closing       bool
	Stopped       bool
}

// Stats returns pool state and cumulative execution counters.
func (m *Master) Stats() MasterStats {
	if !m.isInitialized() {
		return MasterStats{}
	}
	m.RLock()
	defer m.RUnlock()
	return MasterStats{
		TaskStats: m.metrics.snapshot(), Workers: len(m.Pool),
		Queued: len(m.pending), QueueCapacity: m.queueCapacity,
		Closing: m.closing, Stopped: m.stopped,
	}
}
