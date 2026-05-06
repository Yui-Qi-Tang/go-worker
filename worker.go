// Package worker provides creating worker with consistency task interface for your job.
// And we also provide a master to manage the workers of this package and support worker failed recovery.
package worker

import (
	"reflect"
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
	// ErrWorkerNotStarted is denoted the worker has not started accepting tasks.
	ErrWorkerNotStarted error = errors.New("worker is not started")
	// ErrWorkerAlreadyStarted is denoted the worker has already started accepting tasks.
	ErrWorkerAlreadyStarted error = errors.New("worker is already started")
	// ErrWorkerStopped is denoted the worker has already stopped accepting tasks.
	ErrWorkerStopped error = errors.New("worker is stopped")
	// ErrWorkerNilTask denotes the worker was asked to process a nil task.
	ErrWorkerNilTask error = errors.New("worker task is nil")
	// ErrWorkerInvalidName denotes a worker was configured with an empty name.
	ErrWorkerInvalidName error = errors.New("worker name can not be empty")
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

	started  bool
	stopped  bool
	stopOnce sync.Once
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

	if w.Name == "" {
		return nil, ErrWorkerInvalidName
	}

	return w, nil
}

// Start waits the work...
// HINT: it's goroutine!
func (w *Worker) Start() error {
	return w.start()
}

func (w *Worker) start() error {
	w.Lock()
	if w.stopped {
		w.Unlock()
		return ErrWorkerStopped
	}
	if w.started {
		w.Unlock()
		return ErrWorkerAlreadyStarted
	}
	w.started = true
	w.Unlock()

	go func() {
		defer func() {
			if err := recover(); err != nil {
				w.stop()
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
				w.markStopped()
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

	return nil
}

// Stop terminates worker. It is safe to call more than once.
func (w *Worker) Stop() {
	w.stop()
}

func (w *Worker) stop() {
	w.stopOnce.Do(func() {
		w.markStopped()
		close(w.Quit)
	})
}

// waitStatus returns status of worker
func (w *Worker) waitStatus() string {
	s := <-w.status
	return s
}

func workerStatusError(status string) error {
	switch status {
	case workerPanic:
		return ErrWorkerPanic
	case workerErrInit:
		return ErrWorkerTaskInit
	case workerErrRun:
		return ErrWorkerTaskRun
	case workerErrDone:
		return ErrWorkerTaskDone
	default:
		return nil
	}
}

func (w *Worker) readyError() error {
	w.Lock()
	defer w.Unlock()

	if w.stopped {
		return ErrWorkerStopped
	}
	if !w.started {
		return ErrWorkerNotStarted
	}
	return nil
}

func isNilTask(task Task) bool {
	if task == nil {
		return true
	}

	value := reflect.ValueOf(task)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (w *Worker) markStopped() {
	w.Lock()
	w.stopped = true
	w.Unlock()
}

// Do processes task and returns an error when the task fails or the worker panics.
func (w *Worker) Do(task Task) error {
	if err := w.readyError(); err != nil {
		return err
	}
	if isNilTask(task) {
		return ErrWorkerNilTask
	}

	select {
	case <-w.Quit:
		return ErrWorkerStopped
	case w.Task <- task:
	}

	return workerStatusError(w.waitStatus())
}
