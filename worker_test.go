package worker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// correct job
type correctJob string

func (correct correctJob) ID() string {
	return string(correct)
}

func (correctJob) Init() error {
	return nil
}

func (correct correctJob) Run() error {
	for i := 0; i < 3; i++ {
		fmt.Println("say my name:", correct)
	}
	return nil
}

func (correctJob) Done() error {
	return nil
}

// initErrJob job
type initErrJob string

func (i initErrJob) ID() string {
	return string(i)
}

func (initErrJob) Init() error {
	return errors.New("oops! init error")
}

func (initErrJob) Run() error {
	return nil
}

func (initErrJob) Done() error {
	return nil
}

// runErrTask job
type runErrTask string

func (i runErrTask) ID() string {
	return string(i)
}

func (runErrTask) Init() error {
	return nil
}

func (runErrTask) Run() error {
	panic("panic error")
}

func (runErrTask) Done() error {
	return nil
}

func TestWorkedCreated(t *testing.T) {
	t.Log("Start creating worker")

	if worker, err := NewWorker(); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("display worker: %+v", worker)
	}

	t.Log("...Passed")
}

func TestWorkerDoesCorrectJob(t *testing.T) {
	t.Log("Start testing worker does its job correctly")

	var fj correctJob = "correct-job"

	worker, err := NewWorker()

	if err != nil {
		t.Fatal(err)
	}

	worker.Start()
	worker.Task <- fj // send job
	worker.Stop()

	t.Log("...Passed")

}

func TestSchedule(t *testing.T) {
	master, err := NewMaster()
	if err != nil {
		t.Log(err)
	}

	workerNum := 2
	taskNum := 1000

	tasks := make([]Task, taskNum)

	for i := 0; i < workerNum; i++ {
		worker, err := NewWorker()
		if err != nil {
			t.Log(err)
		}
		if err := master.AddWorker(worker); err != nil {
			t.Fatal(err)
		}
	}

	if master.GetWorkers() != workerNum {
		t.Log("expected:", workerNum, "but got", master.GetWorkers())
	}

	for i := 0; i < taskNum; i++ {

		if i%5 < 3 {
			tasks[i] = runErrTask("incorrect_job" + strconv.Itoa(i))
		} else {
			tasks[i] = correctJob("correct_job" + strconv.Itoa(i))
		}

	}

	for _, task := range tasks {
		master.Schedule(task)
	}

	master.Stop()

	t.Log("passed")

}

// Painc in job does not pass by 'data race detector', but it's ok without 'data race detector'
// before imporve it, just comment out
func TestMasterWithSimpleTask(t *testing.T) {
	master, err := NewMaster()

	if err != nil {
		t.Fatal(err)
	}

	workerCounts := 2
	correctTaskCounts := 10
	incorrectTaskCounts := 10

	for i := 0; i < workerCounts; i++ {
		w, err := NewWorker()
		if err != nil {
			t.Fatal(err)
		}
		if err = master.AddWorker(w); err != nil {
			panic(err)
		}
	}

	tasks := func(corrects, incorrect int) []Task {

		tasks := make([]Task, 0)

		for i := 0; i < corrects+incorrect; i++ {

			if i%5 < 2 {
				tasks = append(tasks, runErrTask("incorrect_job"+strconv.Itoa(i)))
			} else {
				tasks = append(tasks, correctJob("correct_job"+strconv.Itoa(i)))
			}
		}
		return tasks

	}(correctTaskCounts, incorrectTaskCounts)

	for _, task := range tasks {
		master.Dispatch(task)
	}

	master.Stop()

	defer func() {
		dumpMemInfo(t)
	}()
	t.Log("...passed")
}

func TestRecovery(t *testing.T) {

	master, err := NewMaster()

	if err != nil {
		t.Fatal(err)
	}

	workerCounts := 2
	incorrectTaskCounts := 10
	tasks := make([]runErrTask, 10)

	for i := 0; i < workerCounts; i++ {
		w, err := NewWorker()
		if err != nil {
			t.Fatal(err)
		}
		if err = master.AddWorker(w); err != nil {
			panic(err)
		}
	}

	for i := 0; i < incorrectTaskCounts; i++ {
		tasks[i] = runErrTask("task-" + strconv.Itoa(i))
	}

	for _, task := range tasks {
		master.Schedule(task)
	}

	time.Sleep(1 * time.Second) // give some for worker recovery

	if master.GetWorkers() != workerCounts {
		t.Fatal("expected", workerCounts, "but got", master.GetWorkers())
	}

	t.Log("...Passed")

}

func dumpWorker(workers []*Worker) {
	fmt.Printf("====> workers: %+v", workers)
}

func dumpMemInfo(t *testing.T) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("Alloc: %d MB, TotalAlloc: %d MB, Sys: %d MB\n",
		ms.Alloc/1024/1024, ms.TotalAlloc/1024/1024, ms.Sys/1024/1024)
	t.Logf("Mallocs: %d, Frees: %d\n",
		ms.Mallocs, ms.Frees)
	t.Logf("HeapAlloc: %d MB, HeapSys: %d MB, HeapIdle: %d MB\n",
		ms.HeapAlloc/1024/1024, ms.HeapSys/1024/1024, ms.HeapIdle/1024/1024)
	t.Logf("HeapObjects: %d\n", ms.HeapObjects)
}

func cleanFile(file string) {
	files, _ := filepath.Glob(file) // log file in current folfer
	for _, fileName := range files {
		os.Remove(fileName)
	}
}
