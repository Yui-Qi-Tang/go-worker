package worker

type message struct {
	err        error
	workerName string
	taskid     string
}

// Message is the result for worker
type Message struct {
	message

	// additional
}

// Err returns error
func (m Message) Err() error {
	return m.err
}

// WorkerName returns worker name
func (m Message) WorkerName() string {
	return m.workerName
}

// TaskID returns task id
func (m Message) TaskID() string {
	return m.taskid
}

/*
func (m message) String() string {
	return ""
}*/
