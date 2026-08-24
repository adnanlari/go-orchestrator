package saga

import "time"

// StepExecution is the durable record of one step's progress within a
// single Saga execution.
//
// It is pure data: constructing, reading, or mutating a StepExecution has
// no side effects and does not invoke any step's forward or compensating
// action. The engine (introduced in a later phase) is responsible for
// creating and updating these records as execution progresses.
type StepExecution struct {
	// Name identifies the step within its saga definition.
	Name string
	// Status is the step's current status.
	Status StepStatus
	// Attempts is the number of times the forward action has been
	// invoked for this step in this execution.
	Attempts int
	// Error is the message from the most recent failure of this step's
	// forward or compensating action, if any. Empty when there has been
	// no failure.
	Error string

	// StartedAt is when the forward action was first invoked. Nil if the
	// step has not started.
	StartedAt *time.Time
	// CompletedAt is when the forward action reached a terminal outcome
	// (StepStatusSucceeded or StepStatusFailed). Nil until then.
	CompletedAt *time.Time
	// CompensationStartedAt is when the compensating action was first
	// invoked. Nil if compensation has not started.
	CompensationStartedAt *time.Time
	// CompensatedAt is when the compensating action reached a terminal
	// outcome (StepStatusCompensated or StepStatusCompensationFailed).
	// Nil until then.
	CompensatedAt *time.Time
}
