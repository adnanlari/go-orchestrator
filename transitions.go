package saga

// statusTransitions enumerates the valid Status transitions for a Saga
// execution. A "from -> to" pair not present here is invalid. This is
// the single source of truth for how an execution's Status may change;
// nothing else in the package mutates Status without consulting it (see
// Execution.transition).
var statusTransitions = map[Status][]Status{
	StatusPending:      {StatusRunning},
	StatusRunning:      {StatusCompleted, StatusCompensating, StatusFailed},
	StatusCompensating: {StatusCompensated, StatusCompensationFailed},
}

// canTransitionStatus reports whether a Saga execution may move directly
// from status "from" to status "to".
func canTransitionStatus(from, to Status) bool {
	for _, allowed := range statusTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// stepStatusTransitions enumerates the valid StepStatus transitions for a
// single step within an execution. A "from -> to" pair not present here
// is invalid. This is the single source of truth for how a step's
// StepStatus may change; nothing else in the package mutates StepStatus
// without consulting it (see StepExecution.transition).
var stepStatusTransitions = map[StepStatus][]StepStatus{
	StepStatusPending:      {StepStatusRunning},
	StepStatusRunning:      {StepStatusSucceeded, StepStatusFailed},
	StepStatusSucceeded:    {StepStatusCompensating},
	StepStatusCompensating: {StepStatusCompensated, StepStatusCompensationFailed},
}

// canTransitionStepStatus reports whether a step within an execution may
// move directly from status "from" to status "to".
func canTransitionStepStatus(from, to StepStatus) bool {
	for _, allowed := range stepStatusTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}
