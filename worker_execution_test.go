package worker

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
)

type executionTraceTask struct {
	calls       []string
	failAt      string
	panicAt     string
	panicIDCall int
	idCalls     int
}

func (t *executionTraceTask) ID() string {
	t.calls = append(t.calls, "ID")
	t.idCalls++
	if t.idCalls == t.panicIDCall {
		panic("ID panic")
	}
	return "execution-trace"
}

func (t *executionTraceTask) phase(name string) error {
	t.calls = append(t.calls, name)
	if t.panicAt == name {
		panic(name)
	}
	if t.failAt == name {
		return errors.New(name + " failed")
	}
	return nil
}

func (t *executionTraceTask) Init() error { return t.phase("Init") }
func (t *executionTraceTask) Run() error  { return t.phase("Run") }
func (t *executionTraceTask) Done() error { return t.phase("Done") }

func TestWorkerExecutionOrderAndFailureBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		task  executionTraceTask
		want  error
		calls []string
	}{
		{"success", executionTraceTask{}, nil, []string{"ID", "Init", "Run", "Done", "ID"}},
		{"init error", executionTraceTask{failAt: "Init"}, ErrWorkerTaskInit, []string{"ID", "Init", "ID"}},
		{"run error", executionTraceTask{failAt: "Run"}, ErrWorkerTaskRun, []string{"ID", "Init", "Run", "ID"}},
		{"done error", executionTraceTask{failAt: "Done"}, ErrWorkerTaskDone, []string{"ID", "Init", "Run", "Done", "ID"}},
		{"init panic", executionTraceTask{panicAt: "Init"}, ErrWorkerPanic, []string{"ID", "Init"}},
		{"run panic", executionTraceTask{panicAt: "Run"}, ErrWorkerPanic, []string{"ID", "Init", "Run"}},
		{"done panic", executionTraceTask{panicAt: "Done"}, ErrWorkerPanic, []string{"ID", "Init", "Run", "Done"}},
		{"received ID panic", executionTraceTask{panicIDCall: 1}, ErrWorkerPanic, []string{"ID"}},
		{"error ID panic", executionTraceTask{failAt: "Run", panicIDCall: 2}, ErrWorkerPanic, []string{"ID", "Init", "Run", "ID"}},
		{"completed ID panic", executionTraceTask{panicIDCall: 2}, ErrWorkerPanic, []string{"ID", "Init", "Run", "Done", "ID"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := NewWorker()
			if err != nil {
				t.Fatal(err)
			}
			w.logger = zap.NewNop()
			t.Cleanup(w.Stop)
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}

			if got := doWithTimeout(t, w, &tt.task); got != tt.want {
				t.Fatalf("Do() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.task.calls, tt.calls) {
				t.Fatalf("task calls = %v, want %v", tt.task.calls, tt.calls)
			}

			wantNext := error(nil)
			if tt.want == ErrWorkerPanic {
				wantNext = ErrWorkerStopped
			}
			if got := doWithTimeout(t, w, phaseErrorTask{id: "next"}); got != wantNext {
				t.Fatalf("next Do() = %v, want %v", got, wantNext)
			}
		})
	}
}

func TestConcurrentDoKeepsTaskResultsSeparate(t *testing.T) {
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	w.logger = zap.NewNop()
	t.Cleanup(w.Stop)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		task phaseErrorTask
		want error
	}{
		{phaseErrorTask{id: "success"}, nil},
		{phaseErrorTask{id: "init", initErr: errors.New("init")}, ErrWorkerTaskInit},
		{phaseErrorTask{id: "run", runErr: errors.New("run")}, ErrWorkerTaskRun},
		{phaseErrorTask{id: "done", doneErr: errors.New("done")}, ErrWorkerTaskDone},
	}
	type result struct{ got, want error }
	const rounds = 8
	results := make(chan result, rounds*len(tests))
	start := make(chan struct{})
	for range rounds {
		for _, tt := range tests {
			go func(task phaseErrorTask, want error) {
				<-start
				results <- result{w.Do(task), want}
			}(tt.task, tt.want)
		}
	}
	close(start)

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for i := 0; i < rounds*len(tests); i++ {
		select {
		case result := <-results:
			if result.got != result.want {
				t.Errorf("Do() = %v, want %v", result.got, result.want)
			}
		case <-timer.C:
			t.Fatal("concurrent Do calls did not finish")
		}
	}
}

func doWithTimeout(t *testing.T, w *Worker, task Task) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- w.Do(task) }()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("Do did not return a task result")
		return nil
	}
}

type nilPanicTask struct{ phaseErrorTask }

func (nilPanicTask) Run() error { panic(nil) }

func TestWorkerReportsNilPanicWithModernGoSemantics(t *testing.T) {
	// Go 1.21+ defaults to a non-nil recovery value for panic(nil).
	// Make this runtime contract explicit even if the test caller uses GODEBUG.
	t.Setenv("GODEBUG", "panicnil=0")
	w, err := NewWorker()
	if err != nil {
		t.Fatal(err)
	}
	w.logger = zap.NewNop()
	t.Cleanup(w.Stop)
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	if got := doWithTimeout(t, w, nilPanicTask{}); got != ErrWorkerPanic {
		t.Fatalf("Do() = %v, want %v", got, ErrWorkerPanic)
	}
	if got := doWithTimeout(t, w, phaseErrorTask{}); got != ErrWorkerStopped {
		t.Fatalf("Do() after panic(nil) = %v, want %v", got, ErrWorkerStopped)
	}
}
