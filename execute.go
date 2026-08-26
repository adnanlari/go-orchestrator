package saga

import (
	"context"
	"errors"
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
// If a saga timeout is configured (WithTimeout), it bounds this entire
// call; exceeding it aborts with a *SagaTimeoutError, handled exactly
// like any other failure (compensation runs if anything had succeeded).
// If a step exceeds its own timeout (WithStepTimeout), that attempt
// fails with a *StepTimeoutError and is retried per the step's
// RetryPolicy, unless the saga-level timeout or an explicit external
// cancellation of ctx ended things first — those always take precedence.
// Every timeout and cancellation error can still be matched with
// errors.Is(err, context.DeadlineExceeded) or
// errors.Is(err, context.Canceled) respectively, in addition to the more
// specific errors.As checks.
//
// Execute freezes the Definition on first call (see Definition.Freeze),
// so it is safe to call Execute concurrently with other Execute calls,
// but not concurrently with AddStep.
//
// Execute is the entry point for a brand-new run. Crash recovery
// (RecoveryManager) drives an already-started Execution back to a
// terminal status the same way, via the same underlying engine — see
// resume.
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

	return d.resume(ctx, exec)
}

// resume drives exec forward to a terminal status starting from wherever
// its persisted Status and per-step Statuses say it currently is,
// rather than assuming a brand-new run. Execute calls it immediately
// after creating a fresh (all StepStatusPending) Execution; a
// RecoveryManager calls it with an Execution loaded from a Store after a
// process restart, possibly with steps already Succeeded, one step
// still Running or Compensating (interrupted mid-attempt when the prior
// process exited), or already in a state that only needs the saga-level
// Status to catch up (e.g. a step recorded Failed but the process died
// before the transition to Compensating was persisted).
//
// Resuming a step (or its compensation) found already Running (or
// Compensating) means invoking it again without knowing whether the
// interrupted attempt already took effect for real — this is what
// "at-least-once execution" means in practice, and is only safe if that
// step's Action (and Compensate) are idempotent.
func (d *Definition) resume(ctx context.Context, exec *Execution) (*Execution, error) {
	var sagaTimeoutErr *SagaTimeoutError
	if d.timeout > 0 {
		sagaTimeoutErr = &SagaTimeoutError{Saga: d.name, Timeout: d.timeout}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, d.timeout, sagaTimeoutErr)
		defer cancel()
	}

	switch exec.Status {
	case StatusCompleted, StatusFailed, StatusCompensated, StatusCompensationFailed:
		return exec, nil // already terminal; nothing to resume
	case StatusCompensating:
		return exec, d.resumeCompensating(ctx, exec)
	case StatusPending:
		if err := d.transitionExec(ctx, exec, StatusRunning); err != nil {
			return exec, err
		}
		return d.resumeRunning(ctx, exec, sagaTimeoutErr)
	default: // StatusRunning
		return d.resumeRunning(ctx, exec, sagaTimeoutErr)
	}
}

// resumeRunning drives exec's forward-execution loop starting from
// wherever its persisted Steps say it left off:
//   - a step already StepStatusSucceeded is skipped; its Output becomes
//     the next step's input, exactly as if execution had never stopped.
//   - a step found StepStatusFailed means the process exited after
//     recording that failure but before persisting the saga-level
//     transition that should have followed — resume re-enters abort for
//     it directly, rather than mistaking it for pending work.
//   - a step found StepStatusPending or StepStatusRunning is (re-)run
//     normally; see the package documentation on why re-running a step
//     that was mid-attempt when the process exited is only safe for
//     idempotent steps.
func (d *Definition) resumeRunning(ctx context.Context, exec *Execution, sagaTimeoutErr *SagaTimeoutError) (*Execution, error) {
	for i, s := range exec.Steps {
		if s.Status == StepStatusFailed {
			return exec, d.abort(ctx, exec, i, &StepError{Step: s.Name, Err: errors.New(s.Error)})
		}
	}

	data := exec.Input
	startAt := 0
	for i := range exec.Steps {
		if exec.Steps[i].Status != StepStatusSucceeded {
			break
		}
		startAt = i + 1
		data = exec.Steps[i].Output
	}

	for i := startAt; i < len(d.steps); i++ {
		step := d.steps[i]
		select {
		case <-ctx.Done():
			exec.CurrentStep = ""
			return exec, d.abort(ctx, exec, i, context.Cause(ctx))
		default:
		}

		exec.CurrentStep = step.Name
		stepExec := &exec.Steps[i]
		if stepExec.Status == StepStatusPending {
			if err := d.transitionStep(ctx, exec, stepExec, StepStatusRunning); err != nil {
				return exec, err
			}
		}
		// If stepExec.Status is already StepStatusRunning, a prior
		// process was already mid-attempt on it when it exited; there is
		// nothing to transition into (it's already there), so fall
		// straight through to (re-)running it.

		out, stepErr := d.runStep(ctx, sagaTimeoutErr, exec.ID, stepExec, step, data)
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
// back, attempts are exhausted, or ctx ends. sagaTimeoutErr is the cause
// Execute attached to ctx if a saga-level timeout was configured
// (WithTimeout), or nil if not.
//
// A successful return is honored even if ctx happened to end during or
// because of that same attempt — for example, an Action that both
// completes its work and triggers cancellation as a side effect — with
// one exception: if sagaTimeoutErr is specifically why ctx ended (our
// own configured deadline, not some unrelated cancellation), the result
// is discarded exactly as callAction already does for a step's own
// timeout, since a result arriving after our own deadline can't be
// trusted as timely either way. Only a failed attempt otherwise consults
// ctx, to decide whether the failure should stop retries immediately. On
// failure it returns either context.Cause(ctx) (if the outer ctx is what
// ended things — a saga timeout or explicit cancellation, which always
// takes precedence over retrying) or a *StepError wrapping the attempt's
// own error (which may itself be a *StepTimeoutError; see callAction) —
// never a bare, unwrapped action error.
//
// Every attempt — the first, and every retry — carries the same
// OperationID (derived from executionID and step.Name), since from a
// downstream idempotency perspective they are all the same logical
// operation being attempted again, not new ones.
func (d *Definition) runStep(ctx context.Context, sagaTimeoutErr *SagaTimeoutError, executionID string, stepExec *StepExecution, step StepDefinition, data any) (any, error) {
	policy := d.retryPolicyFor(step)
	timeout := step.timeout
	opID := operationID(executionID, step.Name)
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}

		stepExec.Attempts++
		out, err := d.callAction(ctx, opID, step, timeout, data)

		if sagaTimeoutErr != nil && errors.Is(context.Cause(ctx), sagaTimeoutErr) {
			return nil, sagaTimeoutErr
		}
		if err == nil {
			return out, nil
		}

		// The outer (saga-level or caller) context ending takes
		// precedence over the attempt's own error, and makes retrying
		// pointless.
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if isNonRetryable(err) || attempt >= policy.MaxAttempts() {
			return nil, &StepError{Step: step.Name, Err: err}
		}
		if waitErr := sleepOrDone(ctx, policy.Delay(attempt+1)); waitErr != nil {
			return nil, waitErr
		}
	}
}

// callAction invokes step.Action once, bounded by timeout if timeout >
// 0, with ctx carrying opID (retrievable inside Action via OperationID).
// If the step's own timeout is specifically what elapses — as opposed to
// the outer ctx ending for some unrelated reason — that result is
// discarded, even if Action ignores the timeout and eventually returns
// success anyway, since a result arriving after the deadline this step
// was configured with can't be trusted as timely. An outer ctx ending is
// left for the caller (runStep) to handle, since a step timeout and a
// saga-level/external cancellation warrant different responses (a step
// timeout retries per policy; the other never does).
func (d *Definition) callAction(ctx context.Context, opID string, step StepDefinition, timeout time.Duration, data any) (any, error) {
	ctx = withOperationID(ctx, opID)
	if timeout <= 0 {
		return step.Action(ctx, data)
	}

	stepTimeoutErr := &StepTimeoutError{Step: step.Name, Timeout: timeout}
	actionCtx, cancel := context.WithTimeoutCause(ctx, timeout, stepTimeoutErr)
	defer cancel()

	out, err := step.Action(actionCtx, data)
	if errors.Is(context.Cause(actionCtx), stepTimeoutErr) {
		return nil, stepTimeoutErr
	}
	return out, err
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
// context.Cause(ctx) if ctx ends first. A non-positive delay still
// checks ctx once rather than sleeping.
func sleepOrDone(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
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
		return context.Cause(ctx)
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
	return d.finishCompensating(ctx, exec, failedAt, originalErr)
}

// resumeCompensating drives exec's compensation loop starting from
// wherever its persisted Steps say it left off: a step already
// StepStatusCompensated is never compensated again — this, together with
// compensate's own per-step status check, is what guarantees recovery
// never duplicates a compensation — and a step found StepStatusCompensating
// (interrupted mid-compensate when the prior process exited) has its
// Compensate re-invoked, subject to the same idempotency caveat resume
// documents for re-running a step's Action.
//
// failedAt is reconstructed from persisted step statuses rather than
// stored directly: since forward execution is strictly sequential, every
// step that ever reached StepStatusSucceeded (or further, into
// compensation) is necessarily a prefix of the steps, so one past the
// highest such index is exactly the boundary the original abort call
// used.
func (d *Definition) resumeCompensating(ctx context.Context, exec *Execution) error {
	failedAt := 0
	for i, s := range exec.Steps {
		switch s.Status {
		case StepStatusSucceeded, StepStatusCompensating, StepStatusCompensated, StepStatusCompensationFailed:
			failedAt = i + 1
		}
	}
	// exec.Error holds the original forward failure's message from
	// before the process exited; only the message survives a crash; its
	// specific type (e.g. *StepError) does not.
	return d.finishCompensating(ctx, exec, failedAt, errors.New(exec.Error))
}

// finishCompensating runs compensation for the steps before failedAt
// (see compensate) and settles exec on StatusCompensated or
// StatusCompensationFailed, preserving originalErr in the returned error
// exactly as abort's doc comment describes. Shared by abort (a fresh
// failure) and resumeCompensating (continuing one interrupted by a
// process exit).
func (d *Definition) finishCompensating(ctx context.Context, exec *Execution, failedAt int, originalErr error) error {
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
// step before failedAt that has something left to compensate — either
// StepStatusSucceeded (not yet compensated at all) or
// StepStatusCompensating (a compensation interrupted by a process exit,
// resumed here) — and has a non-nil Compensate. Steps with a nil
// Compensate are skipped (nothing to undo) and are left at
// StepStatusSucceeded rather than marked compensated. A step already
// StepStatusCompensated or StepStatusCompensationFailed is left exactly
// as it is: this is what prevents recovery from ever compensating the
// same step twice.
//
// It keeps compensating remaining steps even after one fails, so as much
// of the saga as possible gets rolled back rather than stopping at the
// first compensation error. Its final "did everything succeed" answer is
// computed by re-reading every step's status after the loop, not by
// tracking success only across this one call — so a step left
// StepStatusCompensationFailed by an earlier, interrupted attempt still
// correctly counts against the overall result even though this call
// never re-attempts it. It reports a non-nil error only if persisting a
// state change failed and compensation was abandoned partway through.
func (d *Definition) compensate(ctx context.Context, exec *Execution, failedAt int) (bool, error) {
	for i := failedAt - 1; i >= 0; i-- {
		stepExec := &exec.Steps[i]
		if stepExec.Status != StepStatusSucceeded && stepExec.Status != StepStatusCompensating {
			continue
		}
		compensateFn := d.steps[i].Compensate
		if compensateFn == nil {
			continue
		}

		exec.CurrentStep = stepExec.Name
		if stepExec.Status == StepStatusSucceeded {
			if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensating); err != nil {
				return false, err
			}
		}
		// If stepExec.Status is already StepStatusCompensating, a prior
		// process was already mid-compensate on it when it exited;
		// there is nothing to transition into, so fall straight through
		// to (re-)invoking Compensate.

		compensateCtx := withOperationID(ctx, compensationOperationID(exec.ID, stepExec.Name))
		if cErr := compensateFn(compensateCtx, stepExec.Output); cErr != nil {
			stepExec.Error = cErr.Error()
			if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensationFailed); err != nil {
				return false, err
			}
			continue
		}
		if err := d.transitionStep(ctx, exec, stepExec, StepStatusCompensated); err != nil {
			return false, err
		}
	}

	ok := true
	for i := 0; i < failedAt; i++ {
		if exec.Steps[i].Status == StepStatusCompensationFailed {
			ok = false
			break
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
