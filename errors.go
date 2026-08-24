package saga

import (
	"errors"
	"fmt"
)

// ErrCompensationFailed is a sentinel error indicating that a step's
// compensating action itself failed. Errors returned by the engine during
// compensation wrap this sentinel so callers can detect the case with
// errors.Is, distinguishing it from an ordinary forward-step failure.
var ErrCompensationFailed = errors.New("saga: compensation failed")

// StepError associates an error with the name of the step that produced
// it. The engine wraps step failures in a StepError so callers can
// identify which step failed without parsing error strings.
type StepError struct {
	// Step is the name of the step that produced Err.
	Step string
	// Err is the underlying error returned by the step's action.
	Err error
}

// Error implements the error interface.
func (e *StepError) Error() string {
	return fmt.Sprintf("saga: step %q: %v", e.Step, e.Err)
}

// Unwrap returns the underlying error, enabling errors.Is and errors.As
// to see through a StepError to its cause.
func (e *StepError) Unwrap() error {
	return e.Err
}

// InvalidTransitionError indicates an attempted Status transition that
// the state machine does not allow (for example, PENDING directly to
// COMPLETED). It signals a bug in the engine, not a user-facing runtime
// failure: valid transitions are fully enumerated and enforced
// internally, so this should never occur in normal operation.
type InvalidTransitionError struct {
	From Status
	To   Status
}

// Error implements the error interface.
func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("saga: invalid status transition %s -> %s", e.From, e.To)
}

// InvalidStepTransitionError indicates an attempted StepStatus transition
// that the state machine does not allow. Like InvalidTransitionError, it
// signals a bug in the engine rather than a user-facing runtime failure.
type InvalidStepTransitionError struct {
	Step string
	From StepStatus
	To   StepStatus
}

// Error implements the error interface.
func (e *InvalidStepTransitionError) Error() string {
	return fmt.Sprintf("saga: invalid step %q status transition %s -> %s", e.Step, e.From, e.To)
}
