# go-worker

    A simple worker manager with consistency task interface and fail recovery.

# Development

    go version: 1.14+

# Example

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
    
```





# TODO

    1. document(README.md):
        - package usage

    2. setup master with functional option

    3. decuple addworker and run (will affect old test case)

