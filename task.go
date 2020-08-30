package worker

// Task is a general interface for worker
type Task interface {
	// Init is init phase if there are no steps for initial, just return nil
	Init() error
	// Run is your work behavior
	Run() error
	// Done is denoted complete status of work, if there are no steps for this phase, just return nil
	Done() error
	// ID returns the identity of your job
	ID() string
}
