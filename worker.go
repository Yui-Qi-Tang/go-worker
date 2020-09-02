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

// LogLevel level of logger
type LogLevel int32

const (
	// None no logging
	None LogLevel = iota
	// Info diplays logging  message with level 'info'
	Info
	// Debug diplays logging message with level 'Debug' and contains the log under this level
	Debug
	// Fatal diplays logging message with level 'Fatal' and contains the log under this level
	Fatal
)

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

	LogLevel LogLevel

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

// WithLogger setup logger with level
func WithLogger(lv LogLevel) Option {
	return func(w *Worker) {
		config := zap.NewProductionConfig()
		config.Encoding = "console"
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		logger, _ := config.Build()
		w.logger = logger

		w.LogLevel = lv // set logging level
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
		LogLevel:     None,
	}

	uuid := guuid.New()
	name := uuid.String()
	if len(name) == 0 {
		return nil, errors.New("new worker error: invalid uuid(len==0)")
	}

	w.Name = name // default name

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Start waits the work...
// HINT: it's goroutine!
func (w *Worker) Start() {
	w.status.update(running)
	w.logInfo("", running.String())
	go func() {
		defer func() {
			if err := recover(); err != nil {
				w.status.update(taskPanicErr)
				w.logFatal(taskPanicErr.String(), err.(error))
				return
			}
		}()

		for {
			if w.status.load() == quit {
				w.logInfo("", quit.String())
				w.logger.Sync()
				return
			}
			select {
			case task := <-w.Task:
				w.status.update(taskReceived)
				w.logInfo(task.ID(), w.status.load().String())

				if err := task.Init(); err != nil {
					w.status.update(taskInitErr)
					w.logDebug(task.ID(), taskInitErr.String(), err)
					break
				}

				if err := task.Run(); err != nil {
					w.status.update(taskRunErr)
					w.logDebug(task.ID(), taskRunErr.String(), err)
					break
				}

				if err := task.Done(); err != nil {
					w.status.update(taskDoneErr)
					w.logDebug(task.ID(), taskDoneErr.String(), err)
					break
				}

				w.status.update(taskDone)
				w.logInfo("", taskDone.String())
			}
		}

	}()
}

// Stop terminates worker
func (w *Worker) Stop() {
	w.status.update(quit)
}

// Do processes task
func (w *Worker) Do(task Task) {
	go func() {
		w.Task <- task
	}()
}

func (w *Worker) logDebug(taskID, msg string, err error) {
	if w.logger != nil && w.LogLevel >= Debug {
		w.logger.Error(
			err.Error(),
			zap.String("worker", w.Name),
			zap.String("task_name", taskID),
			zap.Any("reason", err),
		)
	}
}

func (w *Worker) logInfo(taskID string, info string) {
	if w.logger != nil && w.LogLevel >= Info {
		w.logger.Info(
			info,
			zap.String("worker", w.Name),
			zap.String("task_name", taskID),
		)
	}
}

func (w *Worker) logFatal(msg string, err error) {
	if w.logger != nil && w.LogLevel >= Fatal {
		w.logger.Fatal(
			err.Error(),
			zap.String("worker", w.Name),
			zap.Any("reason", err),
		)
	}
}
