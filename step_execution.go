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
	// Output is the value returned by this step's Action once it
	// succeeds. It is what Compensate is called with if this step later
	// needs to be undone. Nil until the step succeeds.
	Output any
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

// transition validates that moving from s's current Status to "to" is
// allowed by the state machine, and if so applies it: Status and the
// appropriate timestamp field are updated together so they can never
// disagree with Status. now is injected rather than read from time.Now
// so callers can produce deterministic timestamps in tests.
//
// On an invalid transition, s is left unchanged and an
// *InvalidStepTransitionError is returned.
func (s *StepExecution) transition(to StepStatus, now time.Time) error {
	if !canTransitionStepStatus(s.Status, to) {
		return &InvalidStepTransitionError{Step: s.Name, From: s.Status, To: to}
	}
	s.Status = to
	switch to {
	case StepStatusRunning:
		started := now
		s.StartedAt = &started
	case StepStatusSucceeded, StepStatusFailed:
		completed := now
		s.CompletedAt = &completed
	case StepStatusCompensating:
		compStarted := now
		s.CompensationStartedAt = &compStarted
	case StepStatusCompensated, StepStatusCompensationFailed:
		compDone := now
		s.CompensatedAt = &compDone
	}
	return nil
}
