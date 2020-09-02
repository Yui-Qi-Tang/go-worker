// Package worker provides creating worker with consistency task interface for your job.
// And we also provide a master to manage the workers of this package and support worker failed recovery.
package worker

import (
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	guuid "github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type workerStatus int32

const (
	ready workerStatus = iota // for init phases
	running
	quit

	taskReceived

	taskDone

	taskInitErr
	taskRunErr
	taskDoneErr
	taskPanicErr
)

// load returns status
func (ws *workerStatus) load() workerStatus {
	return workerStatus(atomic.LoadInt32((*int32)(ws)))
}

// update status
func (ws *workerStatus) update(s workerStatus) {
	atomic.StoreInt32((*int32)(ws), (int32)(s))
}

func (ws workerStatus) String() string {
	return [...]string{
		"ready",
		"starting",
		"quit",

		"received-task",
		"done",

		"error-init",
		"error-run",
		"error-done",
		"panic-error",
	}[atomic.LoadInt32((*int32)(&ws))]
}

const (
	// normal events
	workerEventStart    string = "starting"
	workerEventDone     string = "done"
	workerEventReceived string = "received-task"
	workerEventQuit     string = "quit"
	// error
	workerErrInit string = "error-init"
	workerErrRun  string = "error-run"
	workerErrDone string = "error-done"
	// panic
	workerPanic string = "panic"
)

var (
	// ErrWorkerTaskInit is denoted the worker processes job but get error in Init phase
	ErrWorkerTaskInit error = errors.New("worker got error from executing job in Init phase")
	// ErrWorkerTaskRun is denoted the worker processes job but get error in Run phase
	ErrWorkerTaskRun error = errors.New("worker got error from executing job in Run phase")
	// ErrWorkerTaskDone is denoted the worker process job but get error in Done phase
	ErrWorkerTaskDone error = errors.New("worker got error from executing job in Done phase")
	// ErrWorkerPanic is denoted the worker got panic error from executing job or itself.
	ErrWorkerPanic error = errors.New("worker got panic")

	// ErrWorkerWithNoUpstream
	ErrWorkerWithNoUpstream = errors.New("this worker does not set upstream")
)

// Worker is the structure for worker
type Worker struct {
	sync.Mutex
	Task chan Task
	Name string

	status workerStatus

	logger *zap.Logger

	//Upstream chan message
	// Quit     chan interface{}

	callRecovery bool
}

// Option is a functional option for worker setup
type Option func(w *Worker)

// WithName is setup worker name
func WithName(name string) Option {
	return func(w *Worker) {
		w.Name = name
	}
}

func withRecovery() Option {
	return func(w *Worker) {
		w.callRecovery = true
	}
}

// NewWorker returns worker
func NewWorker(opts ...Option) (*Worker, error) {

	w := &Worker{
		//Quit:         make(chan interface{}),
		Task: make(chan Task),
		//Upstream:     make(chan message),
		callRecovery: false,
		status:       ready,
	}

	uuid := guuid.New()
	name := uuid.String()
	if len(name) == 0 {
		return nil, errors.New("new worker error: invalid uuid(len==0)")
	}

	w.Name = name // default name

	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create logger for worker")
	}

	w.logger = logger

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Start waits the work...
// HINT: it's goroutine!
func (w *Worker) Start() {
	w.status.update(running)
	go func() {
		defer func() {
			if err := recover(); err != nil {

				w.status.update(taskPanicErr)
				//w.sendUpstream(ErrWorkerPanic)

				w.logger.Error(workerPanic, zap.String("worker", w.Name), zap.Any("reason", err))
				w.logger.Sync()
				return
			}
		}()

		w.logger.Info(workerEventStart, zap.String("worker", w.Name))

		for {
			if w.status.load() == quit {
				w.logger.Info(workerEventQuit, zap.String("worker", w.Name))
				w.logger.Sync()
				return
			}
			select {
			case task := <-w.Task:
				w.status.update(taskReceived)

				w.logger.Info(
					workerEventReceived,
					zap.String("worker", w.Name),
					zap.String("task_name", task.ID()),
				)

				if err := task.Init(); err != nil {
					w.logger.Error(
						workerErrInit,
						zap.String("worker", w.Name),
						zap.String("task_name", task.ID()),
						zap.Any("reason", err),
					)
					w.status.update(taskInitErr)
					//w.sendUpstream(ErrWorkerTaskInit)
					break
				}

				if err := task.Run(); err != nil {
					w.logger.Error(
						workerErrRun,
						zap.String("worker", w.Name),
						zap.String("task_name", task.ID()),
						zap.Any("reason", err),
					)
					w.status.update(taskRunErr)
					//w.sendUpstream(ErrWorkerTaskRun)
					break
				}

				if err := task.Done(); err != nil {
					w.logger.Error(
						workerErrDone,
						zap.String("worker", w.Name),
						zap.String("task_name", task.ID()),
						zap.Any("reason", err),
					)
					w.status.update(taskDoneErr)
					//w.sendUpstream(ErrWorkerTaskDone)
					break
				}

				w.logger.Info(
					workerEventDone,
					zap.String("worker", w.Name),
					zap.String("task_id", task.ID()),
				)

				w.status.update(taskDone)
				//w.sendUpstream(nil)
			}
		}

	}()
}

// Stop terminates worker
func (w *Worker) Stop() {
	w.status.update(quit)
	// if w.Upstream != nil {
	// 	close(w.Upstream)
	// }
	// close(w.Quit)
}

// Do processes task
func (w *Worker) Do(task Task) {
	go func() {
		w.Task <- task
	}()
}

// func (w *Worker) sendUpstream(err error) {
// 	if w.Upstream != nil {
// 		w.Upstream <- message{workerName: w.Name, err: err}
// 	}
// }
