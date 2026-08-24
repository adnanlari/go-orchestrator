package saga

import "time"

// Execution is the durable record of one run of a Saga definition.
//
// It is pure data: constructing, reading, or mutating an Execution has no
// side effects and does not run any saga. The engine (introduced in a
// later phase) is responsible for creating and updating these records,
// and a Store (introduced in a later phase) is responsible for persisting
// them.
type Execution struct {
	// ID uniquely identifies this execution. Generation is the engine's
	// responsibility.
	ID string
	// SagaName is the name of the saga definition this execution was
	// created from.
	SagaName string
	// Status is the execution's current status.
	Status Status
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
