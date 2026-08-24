package saga

// Status is the status of a Saga execution as a whole.
//
// Status values are stable strings so they serialize cleanly through a
// Store and through logs without custom marshaling. The valid transitions
// between these values are defined and enforced by the state machine
// (introduced in a later phase), not by this type.
type Status string

const (
	// StatusPending means the execution has been created but forward
	// execution has not started yet.
	StatusPending Status = "PENDING"
	// StatusRunning means forward execution of steps is in progress.
	StatusRunning Status = "RUNNING"
	// StatusCompleted means every step executed successfully. This is a
	// terminal, successful outcome.
	StatusCompleted Status = "COMPLETED"
	// StatusCompensating means a step failed and previously successful
	// steps are being compensated in reverse order.
	StatusCompensating Status = "COMPENSATING"
	// StatusCompensated means a step failed and compensation of all
	// previously successful steps completed successfully. This is a
	// terminal outcome: the saga did not achieve its goal, but the system
	// was left in a consistent state.
	StatusCompensated Status = "COMPENSATED"
	// StatusFailed means the execution ended unsuccessfully without ever
	// entering compensation (for example, the first step failed and there
	// was nothing to compensate). This is a terminal outcome.
	StatusFailed Status = "FAILED"
	// StatusCompensationFailed means a step failed and, while compensating
	// previously successful steps, at least one compensating action also
	// failed. This is a terminal outcome that may leave the system in an
	// inconsistent state requiring manual intervention.
	StatusCompensationFailed Status = "COMPENSATION_FAILED"
)

// terminalStatuses are the Status values from which an execution can never
// transition further.
var terminalStatuses = map[Status]bool{
	StatusCompleted:          true,
	StatusCompensated:        true,
	StatusFailed:             true,
	StatusCompensationFailed: true,
}

// validStatuses are all recognized Status values.
var validStatuses = map[Status]bool{
	StatusPending:            true,
	StatusRunning:            true,
	StatusCompleted:          true,
	StatusCompensating:       true,
	StatusCompensated:        true,
	StatusFailed:             true,
	StatusCompensationFailed: true,
}

// Valid reports whether s is one of the recognized Status values.
func (s Status) Valid() bool {
	return validStatuses[s]
}

// IsTerminal reports whether s is a terminal status, meaning an execution
// in this status will not transition to any other status.
func (s Status) IsTerminal() bool {
	return terminalStatuses[s]
}
