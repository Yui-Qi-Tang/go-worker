# go-worker

    A simple worker manager with consistency task interface and fail recovery.

# Development

    go version: 1.14+

# Example

Single worker

```go
    // create worker
    worker, err := worker.NewWorker(WithName("my-worker"))
	if err != nil {
		// check error here
    }
    
    worker.Start() // start worker and wait task

    err := worker.Do(task) // task need to implement worker.Task interface

    if err != nil {
        // check error here...
    }

    // ...

    worker.Stop() // stop if task ok
    
```

Multiple workers are managed by Master

```go

    // ms, err := NewMaster(WithWorkerRecovery(true)) // master support worker recovery
    ms, err := NewMaster() // default master
	if err != nil {
		panic(err) 
	}

	if err := ms.AddWorkers(workers); err != nil {
        panic(err) 
    }

	if err := ms.WakeAllWorkersUp(); err != nil {
		if err == ErrMasterWorkerPoolIsEmpty {
			// handle the specify error
		}
		panic(err) // unexcepted error, so panic
	}


	for i := 0; i < 1000; i++ {
		ms.Schedule(task[i]) // where for all task in []task that implements task interface
    }
    
    // ...

	ms.Stop() // call stop if all task ok

```

