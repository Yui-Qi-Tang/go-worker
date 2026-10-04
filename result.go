package worker

// Phase identifies the task method that failed. An empty phase means that no
// task method failed (success or rejection before execution).
type Phase string

const (
	PhaseID   Phase = "id"
	PhaseInit Phase = "init"
	PhaseRun  Phase = "run"
	PhaseDone Phase = "done"
)

// Result describes a task outcome. Err preserves the package's sentinel errors;
// Cause retains the original phase error, if any. TaskID is the last ID read
// successfully. PanicValue and PanicStack describe an abnormal worker exit.
// A returned Result is a value snapshot; callers own any mutable values stored
// in their error or panic payloads.
type Result struct {
	TaskID     string
	Phase      Phase
	Err        error
	Cause      error
	PanicValue any
	PanicStack string
}
