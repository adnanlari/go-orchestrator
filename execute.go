package saga

import (
	"context"
	"fmt"
	"time"

	"github.com/adnanlari/go-orchestrator/internal/idgen"
)

// Execute runs the saga sequentially: each step's Action is invoked in
// definition order, receiving the data returned by the previous step (or
// input, for the first step). It returns the completed Execution record
// together with an error describing why the run did not reach
// StatusCompleted, if any.
//
// If a step's Action fails, it is retried per the step's effective
// RetryPolicy (its own, via WithStepRetryPolicy, or otherwise the saga's,
// via WithRetryPolicy; NoRetry if neither is configured) unless the
// error was wrapped with NonRetryable. Once retries are exhausted (or
// ctx is cancelled partway through, including during a retry wait),
// every already-succeeded step is compensated in reverse order before
// Execute returns (skipping any step with a nil Compensate, which has
// nothing to undo). The step that failed is never compensated: its
// Action never completed, so there is nothing to undo. If no step had
// succeeded yet, compensation is skipped entirely and the execution goes
// straight to StatusFailed.
//
// The returned error always preserves the original failure — via
// errors.Is/As — whether or not compensation was needed. If compensation
// itself also failed for one or more steps, the returned error
// additionally wraps ErrCompensationFailed; the execution's Steps carry
// the per-step detail of which compensations failed and why.
//
// If a Store is configured (WithStore), Execute persists the execution
// after every Status or StepStatus change. A Store failure is treated as
// fatal to the call: Execute returns immediately with that error, since
// silently continuing would contradict the durability a Store is meant
// to provide. Attempt counts within a single step's retry loop are only
// persisted once that step reaches a final outcome, not attempt by
// attempt.
//
// Execute freezes the Definition on first call (see Definition.Freeze),
// so it is safe to call Execute concurrently with other Execute calls,
// but not concurrently with AddStep.
func (d *Definition) Execute(ctx context.Context, input any) (*Execution, error) {
	d.Freeze()

	now := time.Now()
	exec := &Execution{
		ID:        idgen.New(),
		SagaName:  d.name,
		Status:    StatusPending,
		Input:     input,
		Steps:     make([]StepExecution, len(d.steps)),
		CreatedAt: now,
		UpdatedAt: now,
	}
	for i, step := range d.steps {
		exec.Steps[i] = StepExecution{Name: step.Name, Status: StepStatusPending}
	}
	if err := d.save(ctx, exec); err != nil {
		return exec, err
	}

	if err := d.transitionExec(ctx, exec, StatusRunning); err != nil {
		return exec, err
	}

	data := input
	for i, step := range d.steps {
		select {
		case <-ctx.Done():
			exec.CurrentStep = ""
			return exec, d.abort(ctx, exec, i, ctx.Err())
		default:
		}

		exec.CurrentStep = step.Name
		stepExec := &exec.Steps[i]
		if err := d.transitionStep(ctx, exec, stepExec, StepStatusRunning); err != nil {
			return exec, err
		}

		out, stepErr := d.runStep(ctx, stepExec, step, data)
		if stepErr != nil {
			stepExec.Error = stepErr.Error()
			if err := d.transitionStep(ctx, exec, stepExec, StepStatusFailed); err != nil {
				return exec, err
			}
			exec.CurrentStep = ""
			return exec, d.abort(ctx, exec, i, stepErr)
		}
		stepExec.Output = out
		if err := d.transitionStep(ctx, exec, stepExec, StepStatusSucceeded); err != nil {
			return exec, err
		}
		data = out
	}

	exec.CurrentStep = ""
	exec.Output = data
	if err := d.transitionExec(ctx, exec, StatusCompleted); err != nil {
		return exec, err
	}
	return exec, nil
}

// runStep invokes step's Action, retrying per its effective RetryPolicy
// (see retryPolicyFor) until it succeeds, a NonRetryable error comes
// back, attempts are exhausted, or ctx ends. It returns the step's
// output on success. On failure it returns either ctx.Err() (if ctx is
// what ended the loop) or a *StepError wrapping the action's own error —
// never a bare, unwrapped action error.
func (d *Definition) runStep(ctx context.Context, stepExec *StepExecution, step StepDefinition, data any) (any, error) {
	policy := d.retryPolicyFor(step)
	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		stepExec.Attempts++
		out, err := step.Action(ctx, data)
		if err == nil {
			return out, nil
		}
		if isNonRetryable(err) || attempt >= policy.MaxAttempts() {
			return nil, &StepError{Step: step.Name, Err: err}
		}
		if waitErr := sleepOrDone(ctx, policy.Delay(attempt+1)); waitErr != nil {
			return nil, waitErr
		}
	}
}

// retryPolicyFor returns step's own RetryPolicy if it has one (set via
// WithStepRetryPolicy), otherwise the saga-level policy from
// WithRetryPolicy, otherwise NoRetry.
func (d *Definition) retryPolicyFor(step StepDefinition) RetryPolicy {
	if step.retryPolicy != nil {
		return step.retryPolicy
	}
	return d.retryPolicy
}

// sleepOrDone waits for delay to elapse, returning nil, or returns
// ctx.Err() if ctx ends first. A non-positive delay still checks ctx
// once rather than sleeping.
func sleepOrDone(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// abort ends a run that did not complete successfully. originalErr is
// why: either the error runStep returned for the failed step, or
// ctx.Err() if ctx was cancelled between steps. If any step before
// failedAt succeeded, abort compensates those steps in reverse order;
// otherwise there is nothing to undo and the execution goes straight to
// StatusFailed.
//
// originalErr is always preserved as, or wrapped within, the returned
// error — including when compensation itself fails. If persisting a
// state change fails partway through, that Store error is returned
// instead (see Execute's doc comment on Store failures).
func (d *Definition) abort(ctx context.Context, exec *Execution, failedAt int, originalErr error) error {
	exec.Error = originalErr.Error()

	if !anySucceeded(exec.Steps[:failedAt]) {
		if err := d.transitionExec(ctx, exec, StatusFailed); err != nil {
			return err
		}
		return originalErr
	}

	if err := d.transitionExec(ctx, exec, StatusCompensating); err != nil {
		return err
	}

	// Compensation must still run to completion even when ctx is itself
	// the reason we're aborting (cancelled or timed out) — otherwise a
	// caller giving up on the request would also abandon cleanup,
	// leaving already-succeeded steps permanently un-compensated.
	// WithoutCancel keeps any request-scoped values while dropping the
	// cancellation signal.
	compensateCtx := context.WithoutCancel(ctx)
	ok, err := d.compensate(compensateCtx, exec, failedAt)
	exec.CurrentStep = ""
	if err != nil {
		return err
	}

	if !ok {
		if err := d.transitionExec(ctx, exec, StatusCompensationFailed); err != nil {
			return err
		}
		return fmt.Errorf("%w: %w", ErrCompensationFailed, originalErr)
	}
	if err := d.transitionExec(ctx, exec, StatusCompensated); err != nil {
		return err
	}
	return originalErr
}

// anySucceeded reports whether any step in steps reached
// StepStatusSucceeded.
func anySucceeded(steps []StepExecution) bool {
	for _, s := range steps {
		if s.Status == StepStatusSucceeded {
			return true
		}
	}
	return false
}

// compensate runs the compensating action, in reverse order, for every
// step before failedAt that succeeded and has a non-nil Compensate.
// Steps with a nil Compensate are skipped (nothing to undo) and are left
// at StepStatusSucceeded rather than marked compensated.
//
// It keeps compensating remaining steps even after one fails, so as much
// of the saga as possible gets rolled back rather than stopping at the
// first compensation error. It reports whether every attempted
// compensation succeeded; a non-nil error means persisting a state
// change failed and compensation was abandoned partway through.
func (d *Definition) compensate(ctx context.Context, exec *Execution, failedAt int) (bool, error) {
	ok := true
	for i := failedAt - 1; i >= 0; i-- {
		stepExec := &exec.Steps[i]
		if stepExec.Status != StepStatusSucceeded {
			continue
		}
		compensateFn := d.steps[i].Compensate
		if compensateFn == nil {
			continue
		}

		exec.CurrentStep = stepExec.Name
		if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensating); err != nil {
			return false, err
		}
		if cErr := compensateFn(ctx, stepExec.Output); cErr != nil {
			stepExec.Error = cErr.Error()
			if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensationFailed); err != nil {
				return false, err
			}
			ok = false
			continue
		}
		if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensated); err != nil {
			return false, err
		}
	}
	return ok, nil
}

// transitionExec applies a Status transition that the engine itself
// controls and knows to be legal, then persists exec. A rejected
// transition means the engine drove an execution's status incorrectly,
// a bug in this package rather than a condition callers can hit, so it
// panics rather than threading an error return through every call site.
// A Store failure, in contrast, is a real runtime condition and is
// returned normally.
func (d *Definition) transitionExec(ctx context.Context, exec *Execution, to Status) error {
	if err := exec.transition(to, time.Now()); err != nil {
		panic(err)
	}
	return d.save(ctx, exec)
}

// transitionStep is transitionExec's StepExecution counterpart.
func (d *Definition) transitionStep(ctx context.Context, exec *Execution, step *StepExecution, to StepStatus) error {
	if err := step.transition(to, time.Now()); err != nil {
		panic(err)
	}
	return d.save(ctx, exec)
}

// save persists exec via the Definition's configured Store, wrapping any
// error with enough context to identify which execution failed to save.
func (d *Definition) save(ctx context.Context, exec *Execution) error {
	if err := d.store.Save(ctx, exec); err != nil {
		return fmt.Errorf("saga: failed to persist execution %q: %w", exec.ID, err)
	}
	return nil
}
