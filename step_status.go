package saga

// StepStatus is the status of a single step within one Saga execution.
//
// Like Status, values are stable strings for clean persistence and
// logging. Valid transitions are enforced by the state machine, not by
// this type.
type StepStatus string

const (
	// StepStatusPending means the step's forward action has not been
	// attempted yet.
	StepStatusPending StepStatus = "PENDING"
	// StepStatusRunning means the step's forward action is currently
	// executing (an attempt is in flight).
	StepStatusRunning StepStatus = "RUNNING"
	// StepStatusSucceeded means the step's forward action completed
	// successfully. A step in this status is eligible for compensation if
	// a later step fails.
	StepStatusSucceeded StepStatus = "SUCCEEDED"
	// StepStatusFailed means the step's forward action did not complete
	// successfully (attempts, if retried, were exhausted). A step in this
	// status must never be compensated: its forward action never
	// succeeded, so there is nothing to undo.
	StepStatusFailed StepStatus = "FAILED"
	// StepStatusCompensating means the step's compensating action is
	// currently executing.
	StepStatusCompensating StepStatus = "COMPENSATING"
	// StepStatusCompensated means the step's compensating action completed
	// successfully.
	StepStatusCompensated StepStatus = "COMPENSATED"
	// StepStatusCompensationFailed means the step's compensating action
	// itself failed.
	StepStatusCompensationFailed StepStatus = "COMPENSATION_FAILED"
)

// terminalStepStatuses are the StepStatus values from which a step can
// never transition further within a given execution.
var terminalStepStatuses = map[StepStatus]bool{
	StepStatusFailed:             true,
	StepStatusCompensated:        true,
	StepStatusCompensationFailed: true,
}

// validStepStatuses are all recognized StepStatus values.
var validStepStatuses = map[StepStatus]bool{
	StepStatusPending:            true,
	StepStatusRunning:            true,
	StepStatusSucceeded:          true,
	StepStatusFailed:             true,
	StepStatusCompensating:       true,
	StepStatusCompensated:        true,
	StepStatusCompensationFailed: true,
}

// Valid reports whether s is one of the recognized StepStatus values.
func (s StepStatus) Valid() bool {
	return validStepStatuses[s]
}

// IsTerminal reports whether s is a terminal status, meaning a step in
// this status will not transition to any other status within the same
// execution.
func (s StepStatus) IsTerminal() bool {
	return terminalStepStatuses[s]
}
