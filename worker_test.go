package worker

import (
	"errors"
	"log"
	"runtime"
	"testing"
)

type testTask int

const (
	normal testTask = iota
	normalErr
	panicerr
)

func (tt testTask) String() string {
	return [...]string{
		"normal task",
		"normal task with error",
		"normal task with panic",
	}[tt]
}

func (tt testTask) ID() string {
	return tt.String()
}

func (tt testTask) Init() error {
	log.Println(tt, "init...")

	return [...]error{
		nil,
		errors.New("normal task with error"),
		// errors.New("normal task with panic"), // panic, because out of range
	}[tt]
}

func (tt testTask) Run() error {
	log.Println(tt, "run...")

	return [...]error{
		nil,
		errors.New("normal task with error"),
	}[tt]
}

func (tt testTask) Done() error {
	log.Println(tt, "done...")

	return [...]error{
		nil,
		errors.New("normal task with error"),
	}[tt]
}

func TestLowLevelWorker(t *testing.T) {

	testcases := []struct { // task case form
		task   testTask
		answer string
	}{
		{
			task:   normal,
			answer: workerEventDone,
		},
		{
			task:   normalErr,
			answer: workerErrInit,
		},
		{
			task:   panicerr,
			answer: workerPanic,
		},
	}

	worker, err := NewWorker(WithName("my-worker-internal"))
	if err != nil {
		t.Fatal(err)
	}

	worker.Start()

	for _, testcase := range testcases {
		go func(task testTask) {
			worker.Task <- task
		}(testcase.task)
	}

	worker.Stop()

}

func TestWorkerForClient(t *testing.T) {

	testcases := []struct { // task case form
		task   testTask
		answer error
	}{
		{
			task:   normal,
			answer: nil,
		},
		{
			task:   normalErr,
			answer: ErrWorkerTaskInit,
		},
		{
			task:   panicerr,
			answer: ErrWorkerPanic,
		},
	}

	worker, err := NewWorker(WithName("my-worker-for-client"))
	if err != nil {
		t.Fatal(err)
	}

	worker.Start()

	for _, testcase := range testcases {
		worker.Do(testcase.task)
	}

	worker.Stop()

}

func TestWorkerStatus(t *testing.T) {

	worker, err := NewWorker()

	if err != nil {
		t.Fatal(err)
	}

	if worker.status.load() != ready {
		t.Fatal("worker does not state in 'ready'")
	}

	worker.Start()
	if worker.status.load() != running {
		t.Fatal("worker does not state in 'running'")
	}

	worker.Do(normal)

	runtime.Gosched()

	worker.Stop()

	if worker.status.load() != quit {
		t.Fatal("worker does not state in 'quit'", worker.status.load())
	}

}
