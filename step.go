package saga

import "context"

// ActionFunc is a step's forward action. It receives the execution's
// context and the saga data as produced by the previous step (or the
// Execute input, for the first step), and returns the data to hand to
// the next step along with an error if the action failed.
//
// ActionFunc must respect ctx cancellation; the engine does not guarantee
// it will wait for an ActionFunc that ignores it.
type ActionFunc func(ctx context.Context, data any) (any, error)

// CompensateFunc is a step's compensating action. It receives the
// execution's context and the data that was current when this step's
// ActionFunc completed successfully, and returns an error if
// compensation failed.
//
// A step with a nil CompensateFunc is treated as having nothing to undo:
// compensation for that step is skipped.
type CompensateFunc func(ctx context.Context, data any) error

// StepDefinition is a named forward action paired with its compensating
// action. Construct one with Step and add it to a Definition with
// Definition.AddStep.
type StepDefinition struct {
	// Name identifies the step within its saga. Must be unique within a
	// single Definition.
	Name string
	// Action is the step's forward action. Must not be nil.
	Action ActionFunc
	// Compensate is the step's compensating action, invoked in reverse
	// order if a later step fails. May be nil if this step has nothing
	// to undo.
	Compensate CompensateFunc
	// retryPolicy overrides the saga-level RetryPolicy for this step
	// alone. Nil means "use the saga's policy" (see
	// Definition.retryPolicyFor). Set via WithStepRetryPolicy.
	retryPolicy RetryPolicy
}

// StepOption configures a StepDefinition at construction time. Pass
// options to Step.
type StepOption func(*StepDefinition)

// WithStepRetryPolicy overrides the saga-level RetryPolicy (see
// WithRetryPolicy) for this one step. Panics if policy is nil.
func WithStepRetryPolicy(policy RetryPolicy) StepOption {
	if policy == nil {
		panic("saga: step retry policy must not be nil")
	}
	return func(s *StepDefinition) { s.retryPolicy = policy }
}

// Step constructs a StepDefinition from a name, a forward action, and a
// compensating action, applying any opts (see WithStepRetryPolicy).
// compensate may be nil if the step has nothing to undo.
//
// Step performs no validation beyond applying opts; AddStep validates
// the step when it is added to a Definition.
func Step(name string, action ActionFunc, compensate CompensateFunc, opts ...StepOption) StepDefinition {
	s := StepDefinition{Name: name, Action: action, Compensate: compensate}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}
