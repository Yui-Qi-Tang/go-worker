package worker

import (
	"sync"

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

// NewWorker returns worker
func NewWorker() (*Worker, error) {

	id := guuid.New()

	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, newErr := config.Build()
	if newErr != nil {
		return nil, newErr
	}

	w := &Worker{
		Name:     id.String(),
		Quit:     make(chan interface{}),
		Task:     make(chan Task),
		Recovery: make(chan string),
		logger:   logger,
		status:   make(chan string),
	}

	return w, nil
}

// Worker is the structure for worker
type Worker struct {
	sync.Mutex
	Task chan Task
	Name string

	logger *zap.Logger

	Recovery chan string
	Quit     chan interface{}

	status chan string
}

// Start waits the work...
// HINT: it's goroutine!
func (w *Worker) Start() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				w.status <- workerPanic
				w.Recovery <- w.Name
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
					zap.String("task_name", task.ID()),
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

// Status returns status of worker
func (w *Worker) Status() string {
	s := <-w.status
	return s
}
