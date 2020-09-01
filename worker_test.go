package worker

import (
	"errors"
	"log"
	"testing"
)

type testTask int

const (
	normal testTask = iota
	normalErr
	panicErr
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
			task:   panicErr,
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

		// workerStatus := worker.waitStatus() // use low-level function
		// if workerStatus != testcase.answer {
		// 	t.Fatalf("wrong result :%s, expected: %s", workerStatus, testcase.answer)
		// }
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
			answer: nil,
		},
		{
			task:   panicErr,
			answer: ErrWorkerPanic,
		},
	}

	worker, err := NewWorker(WithName("my-worker-for-client"))
	if err != nil {
		t.Fatal(err)
	}

	worker.Start()

	for _, testcase := range testcases {

		err := worker.Do(testcase.task)

		if err != testcase.answer {
			t.Fatalf("wrong result: %v, expected: %v", err, testcase.answer)
		}

	}

	worker.Stop()

}
