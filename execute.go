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
// If a step fails, or ctx is cancelled partway through, every
// already-succeeded step is compensated in reverse order before Execute
// returns (skipping any step with a nil Compensate, which has nothing to
// undo). The step that failed is never compensated: its Action never
// completed, so there is nothing to undo. If no step had succeeded yet,
// compensation is skipped entirely and the execution goes straight to
// StatusFailed.
//
// The returned error always preserves the original failure — via
// errors.Is/As — whether or not compensation was needed. If compensation
// itself also failed for one or more steps, the returned error
// additionally wraps ErrCompensationFailed; the execution's Steps carry
// the per-step detail of which compensations failed and why.
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

	mustTransition(exec, StatusRunning)

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
		mustTransitionStep(stepExec, StepStatusRunning)
		stepExec.Attempts++

		out, err := step.Action(ctx, data)
		if err != nil {
			stepExec.Error = err.Error()
			mustTransitionStep(stepExec, StepStatusFailed)
			exec.CurrentStep = ""
			return exec, d.abort(ctx, exec, i, &StepError{Step: step.Name, Err: err})
		}
		stepExec.Output = out
		mustTransitionStep(stepExec, StepStatusSucceeded)
		data = out
	}

	exec.CurrentStep = ""
	exec.Output = data
	mustTransition(exec, StatusCompleted)
	return exec, nil
}

// abort ends a run that did not complete successfully. originalErr is
// why: either the error a step's Action returned, or ctx.Err() if ctx
// was cancelled between steps. If any step before failedAt succeeded,
// abort compensates those steps in reverse order; otherwise there is
// nothing to undo and the execution goes straight to StatusFailed.
//
// originalErr is always preserved as, or wrapped within, the returned
// error — including when compensation itself fails.
func (d *Definition) abort(ctx context.Context, exec *Execution, failedAt int, originalErr error) error {
	exec.Error = originalErr.Error()

	if !anySucceeded(exec.Steps[:failedAt]) {
		mustTransition(exec, StatusFailed)
		return originalErr
	}

	mustTransition(exec, StatusCompensating)
	// Compensation must still run to completion even when ctx is itself
	// the reason we're aborting (cancelled or timed out) — otherwise a
	// caller giving up on the request would also abandon cleanup,
	// leaving already-succeeded steps permanently un-compensated.
	// WithoutCancel keeps any request-scoped values while dropping the
	// cancellation signal.
	compensateCtx := context.WithoutCancel(ctx)
	ok := d.compensate(compensateCtx, exec, failedAt)
	exec.CurrentStep = ""

	if !ok {
		mustTransition(exec, StatusCompensationFailed)
		return fmt.Errorf("%w: %w", ErrCompensationFailed, originalErr)
	}
	mustTransition(exec, StatusCompensated)
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
// compensation succeeded.
func (d *Definition) compensate(ctx context.Context, exec *Execution, failedAt int) bool {
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
		mustTransitionStep(stepExec, StepStatusCompensating)
		if err := compensateFn(ctx, stepExec.Output); err != nil {
			stepExec.Error = err.Error()
			mustTransitionStep(stepExec, StepStatusCompensationFailed)
			ok = false
			continue
		}
		mustTransitionStep(stepExec, StepStatusCompensated)
	}
	return ok
}

// mustTransition applies a Status transition that the engine itself
// controls and knows to be legal. A failure here means the engine drove
// an execution's status incorrectly, which is a bug in this package, not
// a condition callers can hit — so it panics rather than threading an
// error return through every call site.
func mustTransition(exec *Execution, to Status) {
	if err := exec.transition(to, time.Now()); err != nil {
		panic(err)
	}
}

// mustTransitionStep is mustTransition's StepExecution counterpart.
func mustTransitionStep(step *StepExecution, to StepStatus) {
	if err := step.transition(to, time.Now()); err != nil {
		panic(err)
	}
}
