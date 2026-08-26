package saga

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// ErrRecoverySkipped is the error recorded in a RecoveryResult for an
// execution a RecoveryManager configured with RecoverSkipInFlight
// deliberately left untouched, because it has a step (or compensation)
// that was still in flight when the prior process exited.
var ErrRecoverySkipped = errors.New("saga: recovery skipped: execution has an in-flight step")

// RecoveryPolicy decides how a RecoveryManager treats an execution whose
// persisted state shows a step (or compensation) still in flight —
// StepStatusRunning or StepStatusCompensating — meaning it is unknown
// whether that step's Action (or Compensate) actually took effect before
// the prior process exited.
type RecoveryPolicy int

const (
	// RecoverAutomatically resumes every incomplete execution
	// immediately, re-invoking any step (or compensation) that was in
	// flight when the prior process exited. This is the default: safe
	// only if every step's Action and Compensate are idempotent.
	RecoverAutomatically RecoveryPolicy = iota
	// RecoverSkipInFlight resumes executions that were cleanly between
	// steps (nothing left StepStatusRunning or StepStatusCompensating)
	// but leaves any execution with an in-flight step untouched,
	// reporting it with ErrRecoverySkipped instead of guessing — for
	// callers who are not confident every step is idempotent and would
	// rather decide those cases manually.
	RecoverSkipInFlight
)

// RecoveryResult is the outcome of resuming (or deliberately skipping)
// one incomplete execution.
type RecoveryResult struct {
	// Execution is the execution's state after recovery acted on it (or
	// its unchanged persisted state, if it was skipped or its saga
	// wasn't registered).
	Execution *Execution
	// Err is the same error Execute would have returned had the process
	// not exited, ErrRecoverySkipped if RecoverSkipInFlight left it
	// untouched, or an error if no Definition was registered for this
	// execution's SagaName.
	Err error
}

// RecoveryManager finds executions left incomplete by a process that
// exited (including a crash) mid-run, and resumes each one from exactly
// where its persisted state says it left off, using the same engine
// Execute itself uses (see Definition.resume).
//
// A RecoveryManager is read-only after construction and safe for
// concurrent use, though Recover itself processes executions
// sequentially and deterministically (sorted by CreatedAt, then ID) —
// see its doc comment.
type RecoveryManager struct {
	lister Lister
	sagas  map[string]*Definition
	policy RecoveryPolicy
}

// RecoveryOption configures a RecoveryManager at construction time. Pass
// options to NewRecoveryManager.
type RecoveryOption func(*RecoveryManager)

// WithSaga registers d so a RecoveryManager can resume executions whose
// SagaName matches d.Name(). Panics if d is nil, or if a saga with the
// same name was already registered on the same RecoveryManager.
func WithSaga(d *Definition) RecoveryOption {
	if d == nil {
		panic("saga: WithSaga requires a non-nil Definition")
	}
	return func(r *RecoveryManager) {
		if _, dup := r.sagas[d.name]; dup {
			panic(fmt.Sprintf("saga: two definitions named %q registered with the same RecoveryManager", d.name))
		}
		r.sagas[d.name] = d
	}
}

// WithRecoveryPolicy configures how the RecoveryManager treats
// executions with an in-flight step. The default is RecoverAutomatically.
func WithRecoveryPolicy(policy RecoveryPolicy) RecoveryOption {
	return func(r *RecoveryManager) { r.policy = policy }
}

// NewRecoveryManager creates a RecoveryManager that scans lister for
// incomplete executions and resumes each against whichever registered
// saga (see WithSaga) matches its SagaName. Panics if lister is nil.
func NewRecoveryManager(lister Lister, opts ...RecoveryOption) *RecoveryManager {
	if lister == nil {
		panic("saga: recovery manager requires a non-nil Lister")
	}
	r := &RecoveryManager{
		lister: lister,
		sagas:  make(map[string]*Definition),
		policy: RecoverAutomatically,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Recover lists every incomplete execution from the configured Lister
// and resumes each one to a terminal status (or, under
// RecoverSkipInFlight, leaves it untouched — see RecoveryPolicy),
// processed one at a time in a deterministic order (sorted by
// CreatedAt, then ID, so repeated runs over the same persisted state
// process executions in the same order).
//
// It returns one RecoveryResult per incomplete execution found. A single
// execution failing to resume, or belonging to an unregistered saga,
// does not stop the rest from being processed — that failure is recorded
// in its own RecoveryResult.Err instead. Recover returns a non-nil error
// only if listing incomplete executions itself failed.
func (r *RecoveryManager) Recover(ctx context.Context) ([]RecoveryResult, error) {
	incomplete, err := r.lister.ListIncomplete(ctx)
	if err != nil {
		return nil, fmt.Errorf("saga: failed to list incomplete executions: %w", err)
	}

	sort.Slice(incomplete, func(i, j int) bool {
		if !incomplete[i].CreatedAt.Equal(incomplete[j].CreatedAt) {
			return incomplete[i].CreatedAt.Before(incomplete[j].CreatedAt)
		}
		return incomplete[i].ID < incomplete[j].ID
	})

	results := make([]RecoveryResult, 0, len(incomplete))
	for _, exec := range incomplete {
		d, ok := r.sagas[exec.SagaName]
		if !ok {
			results = append(results, RecoveryResult{
				Execution: exec,
				Err:       fmt.Errorf("saga: no definition registered for saga %q (execution %q)", exec.SagaName, exec.ID),
			})
			continue
		}
		if r.policy == RecoverSkipInFlight && hasInFlightStep(exec) {
			results = append(results, RecoveryResult{Execution: exec, Err: ErrRecoverySkipped})
			continue
		}
		resumed, resumeErr := d.resume(ctx, exec)
		results = append(results, RecoveryResult{Execution: resumed, Err: resumeErr})
	}
	return results, nil
}

// hasInFlightStep reports whether exec has a step recorded as
// StepStatusRunning or StepStatusCompensating — meaning it's unknown
// whether that step's Action or Compensate actually took effect before
// the process that was running it exited.
func hasInFlightStep(exec *Execution) bool {
	for _, s := range exec.Steps {
		if s.Status == StepStatusRunning || s.Status == StepStatusCompensating {
			return true
		}
	}
	return false
}
