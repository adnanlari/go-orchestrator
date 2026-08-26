package saga

import (
	"context"
	"time"
)

// EventType identifies which lifecycle point an Event represents.
type EventType string

const (
	EventSagaStarted           EventType = "SAGA_STARTED"
	EventSagaCompleted         EventType = "SAGA_COMPLETED"
	EventSagaFailed            EventType = "SAGA_FAILED"
	EventStepStarted           EventType = "STEP_STARTED"
	EventStepCompleted         EventType = "STEP_COMPLETED"
	EventStepFailed            EventType = "STEP_FAILED"
	EventCompensationStarted   EventType = "COMPENSATION_STARTED"
	EventCompensationCompleted EventType = "COMPENSATION_COMPLETED"
	EventCompensationFailed    EventType = "COMPENSATION_FAILED"
)

// Event describes one lifecycle occurrence during a saga execution.
// Step is empty for saga-level events (EventSagaStarted,
// EventSagaCompleted, EventSagaFailed). Error is populated only for the
// two failure event types (EventSagaFailed, which fires for
// StatusFailed, StatusCompensated, and StatusCompensationFailed alike —
// the same set of outcomes for which Execute returns a non-nil error —
// and EventStepFailed/EventCompensationFailed).
type Event struct {
	Type        EventType
	ExecutionID string
	SagaName    string
	Step        string
	Error       string
	At          time.Time
}

// EventPublisher receives Events as they occur during Execute or
// RecoveryManager.Recover. Publish is called synchronously, in the same
// goroutine that is driving the execution, immediately after the
// corresponding state change has already been durably persisted (if a
// Store is configured) — so a publisher can safely treat an Event as
// confirmation that its status change is final, not merely in-flight.
//
// A panic from Publish is recovered and otherwise ignored: a broken or
// misbehaving EventPublisher must never be able to alter what happens to
// the saga it's merely observing. For the same reason, Publish has no
// return value — there is no way for a publisher to fail the saga.
// Publish should not block for long; the engine does not run publishers
// concurrently with the execution or with each other.
type EventPublisher interface {
	Publish(ctx context.Context, event Event)
}

type multiPublisher []EventPublisher

// Publish implements EventPublisher by forwarding event to every
// publisher in turn.
func (m multiPublisher) Publish(ctx context.Context, event Event) {
	for _, p := range m {
		p.Publish(ctx, event)
	}
}

// MultiPublisher returns an EventPublisher that forwards every Event to
// each of publishers in turn, in order. Useful for composing more than
// one destination (for example, structured logging and a metrics
// counter) into the single EventPublisher WithEventPublisher accepts.
func MultiPublisher(publishers ...EventPublisher) EventPublisher {
	return multiPublisher(publishers)
}

// sagaEventType returns the Event.Type that corresponds to a Saga
// execution transitioning to Status "to", and whether "to" has a
// corresponding saga-level event at all (StatusPending and
// StatusCompensating do not: StatusPending is the execution's initial
// state, not a transition into it, and StatusCompensating has no
// dedicated saga-level event in this package's event vocabulary — see
// EventCompensationStarted for the per-step equivalent).
func sagaEventType(to Status) (EventType, bool) {
	switch to {
	case StatusRunning:
		return EventSagaStarted, true
	case StatusCompleted:
		return EventSagaCompleted, true
	case StatusFailed, StatusCompensated, StatusCompensationFailed:
		return EventSagaFailed, true
	default:
		return "", false
	}
}

// stepEventType returns the Event.Type that corresponds to a step
// transitioning to StepStatus "to", and whether "to" has a corresponding
// event at all (StepStatusPending does not, for the same reason
// StatusPending doesn't above).
func stepEventType(to StepStatus) (EventType, bool) {
	switch to {
	case StepStatusRunning:
		return EventStepStarted, true
	case StepStatusSucceeded:
		return EventStepCompleted, true
	case StepStatusFailed:
		return EventStepFailed, true
	case StepStatusCompensating:
		return EventCompensationStarted, true
	case StepStatusCompensated:
		return EventCompensationCompleted, true
	case StepStatusCompensationFailed:
		return EventCompensationFailed, true
	default:
		return "", false
	}
}
