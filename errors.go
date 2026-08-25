package saga

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrCompensationFailed is a sentinel error indicating that a step's
// compensating action itself failed. Errors returned by the engine during
// compensation wrap this sentinel so callers can detect the case with
// errors.Is, distinguishing it from an ordinary forward-step failure.
var ErrCompensationFailed = errors.New("saga: compensation failed")

// ErrExecutionNotFound is returned by Store.Get when no execution with
// the requested ID has been persisted.
var ErrExecutionNotFound = errors.New("saga: execution not found")

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

// nonRetryableError marks an error as one the engine must never retry,
// regardless of the configured RetryPolicy.
type nonRetryableError struct {
	err error
}

// Error implements the error interface.
func (e *nonRetryableError) Error() string {
	return e.err.Error()
}

// Unwrap returns the underlying error, enabling errors.Is and errors.As
// to see through a NonRetryable wrapper to its cause.
func (e *nonRetryableError) Unwrap() error {
	return e.err
}

// NonRetryable wraps err so the engine will not retry the step that
// returned it, even if attempts remain under the configured RetryPolicy.
// Use it for failures a retry could never fix — a validation error, or a
// permanent rejection from a downstream service — as opposed to a
// transient failure like a network timeout. Returns nil if err is nil.
//
// errors.Is and errors.As still see through to err, so wrapping does not
// hide the original cause from callers.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// isNonRetryable reports whether err, or something it wraps, was marked
// with NonRetryable.
func isNonRetryable(err error) bool {
	var nre *nonRetryableError
	return errors.As(err, &nre)
}

// SagaTimeoutError indicates a saga's overall execution did not reach a
// terminal status within its configured timeout (see WithTimeout). It
// wraps context.DeadlineExceeded, so errors.Is(err,
// context.DeadlineExceeded) still reports true for callers that only
// care about the general case.
type SagaTimeoutError struct {
	Saga    string
	Timeout time.Duration
}

// Error implements the error interface.
func (e *SagaTimeoutError) Error() string {
	return fmt.Sprintf("saga: %q exceeded its %s timeout", e.Saga, e.Timeout)
}

// Unwrap enables errors.Is(err, context.DeadlineExceeded) to see through
// a SagaTimeoutError.
func (e *SagaTimeoutError) Unwrap() error {
	return context.DeadlineExceeded
}

// StepTimeoutError indicates one attempt of a step's Action did not
// return within its configured timeout (see WithStepTimeout). It wraps
// context.DeadlineExceeded, so errors.Is(err, context.DeadlineExceeded)
// still reports true for callers that only care about the general case.
//
// A StepTimeoutError is retried like any other step failure, per the
// step's effective RetryPolicy, unless a saga-level timeout or explicit
// cancellation also ended ctx — that always takes precedence and stops
// retries immediately.
type StepTimeoutError struct {
	Step    string
	Timeout time.Duration
}

// Error implements the error interface.
func (e *StepTimeoutError) Error() string {
	return fmt.Sprintf("saga: step %q exceeded its %s timeout", e.Step, e.Timeout)
}

// Unwrap enables errors.Is(err, context.DeadlineExceeded) to see through
// a StepTimeoutError.
func (e *StepTimeoutError) Unwrap() error {
	return context.DeadlineExceeded
}
