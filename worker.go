// Package worker provides creating worker with consistency task interface for your job.
// And we also provide a master to manage the workers of this package and support worker failed recovery.
package worker

import (
	"sync"

	"github.com/pkg/errors"

	guuid "github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

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
)

// Worker is the structure for worker
type Worker struct {
	sync.Mutex
	Task chan Task
	Name string

	logger *zap.Logger

	// Recovery TODO: use a special type for this channel, it's between master and worker
	Recovery chan string
	Quit     chan interface{}

	status chan string
}

// Option is a functional option for worker setup
type Option func(w *Worker)

// WithName is setup worker name
func WithName(name string) Option {
	return func(w *Worker) {
		w.Name = name
	}
}

// WithRecovery sets recovery chan for upstream
// if no upstream exists, don't create worker with this option
func WithRecovery(ok bool) Option {
	return func(w *Worker) {
		if ok {
			w.Recovery = make(chan string)
		}
	}
}

// NewWorker returns worker
func NewWorker(opts ...Option) (*Worker, error) {

	w := &Worker{
		Quit:   make(chan interface{}),
		Task:   make(chan Task),
		status: make(chan string),
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
	go func() {
		defer func() {
			if err := recover(); err != nil {
				w.status <- workerPanic

				if w.Recovery != nil {
					w.Recovery <- w.Name
				}

				w.logger.Error(workerPanic, zap.String("worker", w.Name), zap.Any("reason", err))
				w.logger.Sync()
				return
			}
		}()

		w.logger.Info(workerEventStart, zap.String("worker", w.Name))

		for {
			select {
			case <-w.Quit:
				w.logger.Info(workerEventQuit, zap.String("worker", w.Name))
				w.logger.Sync()
				return
			case task := <-w.Task:
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
					w.status <- workerErrInit
					break
				}

				if err := task.Run(); err != nil {
					w.logger.Error(
						workerErrRun,
						zap.String("worker", w.Name),
						zap.String("task_name", task.ID()),
						zap.Any("reason", err),
					)
					w.status <- workerErrRun
					break
				}

				if err := task.Done(); err != nil {
					w.logger.Error(
						workerErrDone,
						zap.String("worker", w.Name),
						zap.String("task_name", task.ID()),
						zap.Any("reason", err),
					)
					w.status <- workerErrDone
					break
				}

				w.logger.Info(
					workerEventDone,
					zap.String("worker", w.Name),
					zap.String("task_id", task.ID()),
				)
				w.status <- workerEventDone
			}
		}

	}()
}

// Stop terminates worker
func (w *Worker) Stop() {
	close(w.Quit)
}

// waitStatus returns status of worker
func (w *Worker) waitStatus() string {
	s := <-w.status
	return s
}

// Do processes task; error if panic
func (w *Worker) Do(task Task) error {
	go func() {
		w.Task <- task
	}()

	if w.waitStatus() == workerPanic {
		return ErrWorkerPanic
	}
	return nil
}
