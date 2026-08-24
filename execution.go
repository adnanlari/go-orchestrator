package saga

import "time"

// Execution is the durable record of one run of a Saga definition.
//
// It is pure data: constructing, reading, or mutating an Execution has no
// side effects and does not run any saga. The engine is responsible for
// creating and updating these records, and a Store is responsible for
// persisting them.
type Execution struct {
	// ID uniquely identifies this execution. Generation is the engine's
	// responsibility.
	ID string
	// SagaName is the name of the saga definition this execution was
	// created from.
	SagaName string
	// Status is the execution's current status.
	Status Status
	// Input is the value the execution was started with (the "input"
	// argument to Execute).
	Input any
	// Output is the value produced by the last step to run, once the
	// execution reaches StatusCompleted. Nil until then.
	Output any
	// Steps are this execution's step records, in the same order as the
	// saga definition's steps.
	Steps []StepExecution
	// CurrentStep is the name of the step currently being acted on
	// (forward or compensating), or empty if no step is in flight.
	CurrentStep string
	// Error is the message describing why the execution is not
	// proceeding toward StatusCompleted, if any. Empty while pending,
	// running, or completed successfully.
	Error string

	// CreatedAt is when the execution record was created.
	CreatedAt time.Time
	// UpdatedAt is when the execution record was last modified.
	UpdatedAt time.Time
	// StartedAt is when the execution left StatusPending. Nil if it has
	// not started.
	StartedAt *time.Time
	// CompletedAt is when the execution reached a terminal status. Nil
	// until then.
	CompletedAt *time.Time
}

// Clone returns a deep copy of e: its Steps slice and every *time.Time
// pointer are copied rather than shared. Input, Output, and each step's
// Output are copied by reference only (they are `any`, so a true deep
// copy isn't possible without reflection) — if step data is itself a
// mutable pointer type, avoid mutating it after it has been returned
// from a step, or a Store holding a Clone could observe the mutation.
//
// Store implementations should call Clone before persisting or
// returning an Execution, so that the engine mutating the live
// Execution it is working with afterward cannot alter what was already
// persisted, and vice versa.
func (e *Execution) Clone() *Execution {
	cp := *e
	cp.Steps = make([]StepExecution, len(e.Steps))
	copy(cp.Steps, e.Steps)
	for i := range cp.Steps {
		cp.Steps[i].StartedAt = clonedTime(e.Steps[i].StartedAt)
		cp.Steps[i].CompletedAt = clonedTime(e.Steps[i].CompletedAt)
		cp.Steps[i].CompensationStartedAt = clonedTime(e.Steps[i].CompensationStartedAt)
		cp.Steps[i].CompensatedAt = clonedTime(e.Steps[i].CompensatedAt)
	}
	cp.StartedAt = clonedTime(e.StartedAt)
	cp.CompletedAt = clonedTime(e.CompletedAt)
	return &cp
}

func clonedTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	cp := *t
	return &cp
}

// transition validates that moving from e's current Status to "to" is
// allowed by the state machine, and if so applies it: Status, UpdatedAt,
// and (when entering StatusRunning or a terminal status) StartedAt or
// CompletedAt are updated together so they can never disagree with
// Status. now is injected rather than read from time.Now so callers can
// produce deterministic timestamps in tests.
//
// On an invalid transition, e is left unchanged and an
// *InvalidTransitionError is returned.
func (e *Execution) transition(to Status, now time.Time) error {
	if !canTransitionStatus(e.Status, to) {
		return &InvalidTransitionError{From: e.Status, To: to}
	}
	e.Status = to
	e.UpdatedAt = now
	if to == StatusRunning {
		started := now
		e.StartedAt = &started
	}
	if to.IsTerminal() {
		completed := now
		e.CompletedAt = &completed
	}
	return nil
}
