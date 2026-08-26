package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
// If the configured Store also implements Locker, Execute acquires an
// exclusive lease on the execution before doing anything else and holds
// it (renewing automatically on every persisted state change) until it
// returns, releasing it either way. If the lease is already held by
// another worker, Execute returns an *ExecutionLockedError immediately
// without touching the execution at all. See Locker's doc comment for
// why this matters most for RecoveryManager, not a fresh Execute call.
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
// If configured (WithEventPublisher, WithLogger, WithMetrics,
// WithTracer), Execute publishes an Event, emits a structured log line,
// and records a metric at each lifecycle transition, and wraps the whole
// call (and each step attempt) in a trace span. None of these can affect
// the saga's outcome: a panicking EventPublisher is recovered and
// otherwise ignored.
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
	// This first save happens before any lock is acquired: exec.ID is
	// freshly generated and unique, so no other worker could possibly be
	// contending for it yet — resume, immediately below, is what
	// acquires the lock before making any further progress.
	if err := d.store.Save(ctx, exec); err != nil {
		return exec, fmt.Errorf("saga: failed to persist execution %q: %w", exec.ID, err)
	}

	return d.resume(ctx, exec)
}

// runState carries state scoped to a single resume call (a fresh
// Execute, or one RecoveryManager.resume of a previously-started
// execution): the Definition being driven, plus, when its Store
// implements Locker, the lease this call holds on the execution. It
// embeds *Definition so its methods (the bulk of the engine) can read
// steps/policies/etc. exactly as if they were still methods on
// *Definition, while also having access to the per-call lock state.
type runState struct {
	*Definition
	locker    Locker // nil if the Store doesn't implement Locker
	lockOwner string
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
//
// If the Store implements Locker, resume acquires an execution-level
// lease before doing anything else (see Execute's doc comment) and
// releases it before returning, by any path.
func (d *Definition) resume(ctx context.Context, exec *Execution) (*Execution, error) {
	rs := &runState{Definition: d}
	if locker, ok := d.store.(Locker); ok {
		rs.locker = locker
		rs.lockOwner = idgen.New()
		acquired, err := locker.Acquire(ctx, exec.ID, rs.lockOwner, d.lockTTL)
		if err != nil {
			return exec, fmt.Errorf("saga: failed to acquire execution lock for %q: %w", exec.ID, err)
		}
		if !acquired {
			return exec, &ExecutionLockedError{ExecutionID: exec.ID}
		}
		defer func() {
			_ = locker.Release(context.WithoutCancel(ctx), exec.ID, rs.lockOwner)
		}()
	}

	ctx, endSpan := rs.startSpan(ctx, "saga:"+d.name)
	defer endSpan()

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
		return exec, rs.resumeCompensating(ctx, exec)
	case StatusPending:
		if err := rs.transitionExec(ctx, exec, StatusRunning); err != nil {
			return exec, err
		}
		return rs.resumeRunning(ctx, exec, sagaTimeoutErr)
	default: // StatusRunning
		return rs.resumeRunning(ctx, exec, sagaTimeoutErr)
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
func (rs *runState) resumeRunning(ctx context.Context, exec *Execution, sagaTimeoutErr *SagaTimeoutError) (*Execution, error) {
	for i, s := range exec.Steps {
		if s.Status == StepStatusFailed {
			return exec, rs.abort(ctx, exec, i, &StepError{Step: s.Name, Err: errors.New(s.Error)})
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

	for i := startAt; i < len(rs.steps); i++ {
		step := rs.steps[i]
		select {
		case <-ctx.Done():
			exec.CurrentStep = ""
			return exec, rs.abort(ctx, exec, i, context.Cause(ctx))
		default:
		}

		exec.CurrentStep = step.Name
		stepExec := &exec.Steps[i]
		if stepExec.Status == StepStatusPending {
			if err := rs.transitionStep(ctx, exec, stepExec, StepStatusRunning); err != nil {
				return exec, err
			}
		}
		// If stepExec.Status is already StepStatusRunning, a prior
		// process was already mid-attempt on it when it exited; there is
		// nothing to transition into (it's already there), so fall
		// straight through to (re-)running it.

		out, stepErr := rs.runStep(ctx, sagaTimeoutErr, exec.ID, stepExec, step, data)
		if stepErr != nil {
			stepExec.Error = stepErr.Error()
			if err := rs.transitionStep(ctx, exec, stepExec, StepStatusFailed); err != nil {
				return exec, err
			}
			exec.CurrentStep = ""
			return exec, rs.abort(ctx, exec, i, stepErr)
		}
		stepExec.Output = out
		if err := rs.transitionStep(ctx, exec, stepExec, StepStatusSucceeded); err != nil {
			return exec, err
		}
		data = out
	}

	exec.CurrentStep = ""
	exec.Output = data
	if err := rs.transitionExec(ctx, exec, StatusCompleted); err != nil {
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
func (rs *runState) runStep(ctx context.Context, sagaTimeoutErr *SagaTimeoutError, executionID string, stepExec *StepExecution, step StepDefinition, data any) (any, error) {
	policy := rs.retryPolicyFor(step)
	timeout := step.timeout
	opID := operationID(executionID, step.Name)
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}

		stepExec.Attempts++
		out, err := rs.callAction(ctx, opID, step, timeout, data)

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
// 0, with ctx carrying opID (retrievable inside Action via OperationID)
// and wrapped in its own trace span. If the step's own timeout is
// specifically what elapses — as opposed to the outer ctx ending for
// some unrelated reason — that result is discarded, even if Action
// ignores the timeout and eventually returns success anyway, since a
// result arriving after the deadline this step was configured with
// can't be trusted as timely. An outer ctx ending is left for the caller
// (runStep) to handle, since a step timeout and a saga-level/external
// cancellation warrant different responses (a step timeout retries per
// policy; the other never does).
func (rs *runState) callAction(ctx context.Context, opID string, step StepDefinition, timeout time.Duration, data any) (any, error) {
	ctx = withOperationID(ctx, opID)
	ctx, endSpan := rs.startSpan(ctx, "step:"+step.Name)
	defer endSpan()

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
func (rs *runState) retryPolicyFor(step StepDefinition) RetryPolicy {
	if step.retryPolicy != nil {
		return step.retryPolicy
	}
	return rs.retryPolicy
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
func (rs *runState) abort(ctx context.Context, exec *Execution, failedAt int, originalErr error) error {
	exec.Error = originalErr.Error()

	if !anySucceeded(exec.Steps[:failedAt]) {
		if err := rs.transitionExec(ctx, exec, StatusFailed); err != nil {
			return err
		}
		return originalErr
	}

	if err := rs.transitionExec(ctx, exec, StatusCompensating); err != nil {
		return err
	}
	return rs.finishCompensating(ctx, exec, failedAt, originalErr)
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
func (rs *runState) resumeCompensating(ctx context.Context, exec *Execution) error {
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
	return rs.finishCompensating(ctx, exec, failedAt, errors.New(exec.Error))
}

// finishCompensating runs compensation for the steps before failedAt
// (see compensate) and settles exec on StatusCompensated or
// StatusCompensationFailed, preserving originalErr in the returned error
// exactly as abort's doc comment describes. Shared by abort (a fresh
// failure) and resumeCompensating (continuing one interrupted by a
// process exit).
func (rs *runState) finishCompensating(ctx context.Context, exec *Execution, failedAt int, originalErr error) error {
	// Compensation must still run to completion even when ctx is itself
	// the reason we're aborting (cancelled or timed out) — otherwise a
	// caller giving up on the request would also abandon cleanup,
	// leaving already-succeeded steps permanently un-compensated.
	// WithoutCancel keeps any request-scoped values while dropping the
	// cancellation signal.
	compensateCtx := context.WithoutCancel(ctx)
	ok, err := rs.compensate(compensateCtx, exec, failedAt)
	exec.CurrentStep = ""
	if err != nil {
		return err
	}

	if !ok {
		if err := rs.transitionExec(ctx, exec, StatusCompensationFailed); err != nil {
			return err
		}
		return fmt.Errorf("%w: %w", ErrCompensationFailed, originalErr)
	}
	if err := rs.transitionExec(ctx, exec, StatusCompensated); err != nil {
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
func (rs *runState) compensate(ctx context.Context, exec *Execution, failedAt int) (bool, error) {
	for i := failedAt - 1; i >= 0; i-- {
		stepExec := &exec.Steps[i]
		if stepExec.Status != StepStatusSucceeded && stepExec.Status != StepStatusCompensating {
			continue
		}
		compensateFn := rs.steps[i].Compensate
		if compensateFn == nil {
			continue
		}

		exec.CurrentStep = stepExec.Name
		if stepExec.Status == StepStatusSucceeded {
			if err := rs.transitionStep(ctx, exec, stepExec, StepStatusCompensating); err != nil {
				return false, err
			}
		}
		// If stepExec.Status is already StepStatusCompensating, a prior
		// process was already mid-compensate on it when it exited;
		// there is nothing to transition into, so fall straight through
		// to (re-)invoking Compensate.

		if cErr := rs.invokeCompensate(ctx, exec, stepExec, compensateFn); cErr != nil {
			stepExec.Error = cErr.Error()
			if err := rs.transitionStep(ctx, exec, stepExec, StepStatusCompensationFailed); err != nil {
				return false, err
			}
			continue
		}
		if err := rs.transitionStep(ctx, exec, stepExec, StepStatusCompensated); err != nil {
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

// invokeCompensate calls compensateFn with ctx carrying stepExec's
// compensation OperationID and wrapped in its own trace span.
func (rs *runState) invokeCompensate(ctx context.Context, exec *Execution, stepExec *StepExecution, compensateFn CompensateFunc) error {
	ctx = withOperationID(ctx, compensationOperationID(exec.ID, stepExec.Name))
	ctx, endSpan := rs.startSpan(ctx, "step:"+stepExec.Name+":compensate")
	defer endSpan()
	return compensateFn(ctx, stepExec.Output)
}

// transitionExec applies a Status transition that the engine itself
// controls and knows to be legal, persists exec, and (if configured)
// publishes the corresponding Event, logs it, and records it as a
// metric. A rejected transition means the engine drove an execution's
// status incorrectly, a bug in this package rather than a condition
// callers can hit, so it panics rather than threading an error return
// through every call site. A Store failure, in contrast, is a real
// runtime condition and is returned normally.
func (rs *runState) transitionExec(ctx context.Context, exec *Execution, to Status) error {
	if err := exec.transition(to, time.Now()); err != nil {
		panic(err)
	}
	if err := rs.save(ctx, exec); err != nil {
		return err
	}
	rs.notifyExec(ctx, exec, to)
	return nil
}

// transitionStep is transitionExec's StepExecution counterpart.
func (rs *runState) transitionStep(ctx context.Context, exec *Execution, step *StepExecution, to StepStatus) error {
	if err := step.transition(to, time.Now()); err != nil {
		panic(err)
	}
	if err := rs.save(ctx, exec); err != nil {
		return err
	}
	rs.notifyStep(ctx, exec, step, to)
	return nil
}

// save persists exec via the Definition's configured Store, wrapping any
// error with enough context to identify which execution failed to save.
// If this runState holds an execution lock (the Store implements
// Locker), save also renews it — this is what lets a lease with a
// bounded TTL keep being held by a still-genuinely-progressing
// execution without expiring out from under it, since save is called on
// every persisted state change.
func (rs *runState) save(ctx context.Context, exec *Execution) error {
	if rs.locker != nil {
		acquired, err := rs.locker.Acquire(ctx, exec.ID, rs.lockOwner, rs.lockTTL)
		if err != nil {
			return fmt.Errorf("saga: failed to renew execution lock for %q: %w", exec.ID, err)
		}
		if !acquired {
			return &ExecutionLockedError{ExecutionID: exec.ID}
		}
	}
	if err := rs.store.Save(ctx, exec); err != nil {
		return fmt.Errorf("saga: failed to persist execution %q: %w", exec.ID, err)
	}
	return nil
}

// startSpan starts a trace span via the configured Tracer, or returns
// ctx unchanged with a no-op end function if none is configured, so
// callers can unconditionally defer the result.
func (rs *runState) startSpan(ctx context.Context, name string) (context.Context, func()) {
	if rs.tracer == nil {
		return ctx, noopSpanEnd
	}
	return rs.tracer.StartSpan(ctx, name)
}

// notifyExec publishes, logs, and records a metric for the Event
// corresponding to exec transitioning to Status "to", and — once "to" is
// terminal — records the execution's total duration (measured from
// exec.StartedAt, so it includes any time spent stopped between a crash
// and recovery, not just this process's share of the work). Does
// nothing for a "to" with no corresponding Event (see sagaEventType) or
// if nothing is configured to receive it.
func (rs *runState) notifyExec(ctx context.Context, exec *Execution, to Status) {
	evtType, ok := sagaEventType(to)
	if !ok {
		return
	}
	ev := Event{Type: evtType, ExecutionID: exec.ID, SagaName: exec.SagaName, Error: exec.Error, At: time.Now()}
	rs.publish(ctx, ev)
	rs.logEvent(ctx, ev)
	rs.incEventCounter(ev)
	if to.IsTerminal() && exec.StartedAt != nil {
		rs.observeDuration("saga_duration_seconds", time.Since(*exec.StartedAt), "saga", exec.SagaName, "status", string(to))
	}
}

// notifyStep is notifyExec's StepExecution counterpart (see
// stepEventType for which StepStatus values have a corresponding Event).
func (rs *runState) notifyStep(ctx context.Context, exec *Execution, step *StepExecution, to StepStatus) {
	evtType, ok := stepEventType(to)
	if !ok {
		return
	}
	rs.publishAndLog(ctx, Event{Type: evtType, ExecutionID: exec.ID, SagaName: exec.SagaName, Step: step.Name, Error: step.Error, At: time.Now()})
}

// publishAndLog publishes ev, logs it, and records it as a metric — the
// three things notifyExec and notifyStep both always do together.
func (rs *runState) publishAndLog(ctx context.Context, ev Event) {
	rs.publish(ctx, ev)
	rs.logEvent(ctx, ev)
	rs.incEventCounter(ev)
}

// publish delivers ev to the configured EventPublisher, if any. A panic
// from Publish is recovered and discarded: a broken EventPublisher must
// never be able to affect the saga it's observing.
func (rs *runState) publish(ctx context.Context, ev Event) {
	if rs.publisher == nil {
		return
	}
	defer func() { _ = recover() }()
	rs.publisher.Publish(ctx, ev)
}

// logEvent emits a structured log line for ev via the configured
// *slog.Logger, if any. Saga/step failure events log at Error; every
// other event type logs at Info.
func (rs *runState) logEvent(ctx context.Context, ev Event) {
	if rs.logger == nil {
		return
	}
	level := slog.LevelInfo
	switch ev.Type {
	case EventSagaFailed, EventStepFailed, EventCompensationFailed:
		level = slog.LevelError
	}
	attrs := []any{"execution_id", ev.ExecutionID, "saga", ev.SagaName}
	if ev.Step != "" {
		attrs = append(attrs, "step", ev.Step)
	}
	if ev.Error != "" {
		attrs = append(attrs, "error", ev.Error)
	}
	rs.logger.Log(ctx, level, string(ev.Type), attrs...)
}

// incEventCounter increments the "saga_events_total" counter via the
// configured Metrics, if any, labeled by saga name, event type, and
// (when applicable) step name.
func (rs *runState) incEventCounter(ev Event) {
	if rs.metrics == nil {
		return
	}
	labels := []string{"saga", ev.SagaName, "event", string(ev.Type)}
	if ev.Step != "" {
		labels = append(labels, "step", ev.Step)
	}
	rs.metrics.IncCounter("saga_events_total", labels...)
}

// observeDuration records d (as seconds) against the named metric via
// the configured Metrics, if any.
func (rs *runState) observeDuration(name string, d time.Duration, labels ...string) {
	if rs.metrics == nil {
		return
	}
	rs.metrics.ObserveDuration(name, d.Seconds(), labels...)
}
