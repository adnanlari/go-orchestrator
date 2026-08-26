package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recordingPublisher collects every Event it receives, safe for
// concurrent use even though the engine only ever calls it sequentially.
type recordingPublisher struct {
	mu     sync.Mutex
	events []Event
}

func (p *recordingPublisher) Publish(ctx context.Context, event Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
}

func (p *recordingPublisher) types() []EventType {
	p.mu.Lock()
	defer p.mu.Unlock()
	types := make([]EventType, len(p.events))
	for i, e := range p.events {
		types[i] = e.Type
	}
	return types
}

func assertEventTypes(t *testing.T, got []EventType, want []EventType) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("events[%d] = %q, want %q (full: %v)", i, got[i], w, got)
		}
	}
}

func TestEvents_SuccessfulRunFiresExpectedSequence(t *testing.T) {
	pub := &recordingPublisher{}
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	assertEventTypes(t, pub.types(), []EventType{
		EventSagaStarted,
		EventStepStarted, EventStepCompleted,
		EventStepStarted, EventStepCompleted,
		EventSagaCompleted,
	})
}

func TestEvents_FailureAndCompensationFiresExpectedSequence(t *testing.T) {
	pub := &recordingPublisher{}
	failingErr := errors.New("insufficient stock")
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	assertEventTypes(t, pub.types(), []EventType{
		EventSagaStarted,
		EventStepStarted, EventStepCompleted, // A succeeds
		EventStepStarted, EventStepFailed, // B fails
		EventCompensationStarted, EventCompensationCompleted, // A compensated
		EventSagaFailed,
	})
}

func TestEvents_CompensationFailureFiresCompensationFailed(t *testing.T) {
	pub := &recordingPublisher{}
	failingErr := errors.New("boom")
	compErr := errors.New("refund gateway down")
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error { return compErr })).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, ErrCompensationFailed) {
		t.Fatalf("errors.Is(err, ErrCompensationFailed) = false, err = %v", err)
	}

	assertEventTypes(t, pub.types(), []EventType{
		EventSagaStarted,
		EventStepStarted, EventStepCompleted,
		EventStepStarted, EventStepFailed,
		EventCompensationStarted, EventCompensationFailed,
		EventSagaFailed,
	})
}

func TestEvents_FirstStepFailsNoCompensationEvents(t *testing.T) {
	pub := &recordingPublisher{}
	failingErr := errors.New("boom")
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	assertEventTypes(t, pub.types(), []EventType{
		EventSagaStarted,
		EventStepStarted, EventStepFailed,
		EventSagaFailed,
	})
}

func TestEvents_ContainCorrectMetadata(t *testing.T) {
	pub := &recordingPublisher{}
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("reserve_inventory", noopAction, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	for _, ev := range pub.events {
		if ev.ExecutionID != exec.ID {
			t.Errorf("event %q ExecutionID = %q, want %q", ev.Type, ev.ExecutionID, exec.ID)
		}
		if ev.SagaName != "order_creation" {
			t.Errorf("event %q SagaName = %q, want %q", ev.Type, ev.SagaName, "order_creation")
		}
		if ev.At.IsZero() {
			t.Errorf("event %q At is zero", ev.Type)
		}
	}
	// Step-level events must carry the step name; saga-level ones must not.
	var sawStepStarted bool
	for _, ev := range pub.events {
		switch ev.Type {
		case EventStepStarted, EventStepCompleted:
			sawStepStarted = true
			if ev.Step != "reserve_inventory" {
				t.Errorf("event %q Step = %q, want %q", ev.Type, ev.Step, "reserve_inventory")
			}
		case EventSagaStarted, EventSagaCompleted:
			if ev.Step != "" {
				t.Errorf("event %q Step = %q, want empty", ev.Type, ev.Step)
			}
		}
	}
	if !sawStepStarted {
		t.Fatal("expected at least one step event")
	}
}

func TestEvents_FailureEventsCarryErrorMessage(t *testing.T) {
	pub := &recordingPublisher{}
	failingErr := errors.New("insufficient stock")
	d := New("order_creation", WithEventPublisher(pub)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err == nil {
		t.Fatal("expected an error")
	}

	var sawStepFailed, sawSagaFailed bool
	for _, ev := range pub.events {
		switch ev.Type {
		case EventStepFailed:
			sawStepFailed = true
			if ev.Error == "" {
				t.Error("EventStepFailed.Error should not be empty")
			}
		case EventSagaFailed:
			sawSagaFailed = true
			if ev.Error == "" {
				t.Error("EventSagaFailed.Error should not be empty")
			}
		}
	}
	if !sawStepFailed || !sawSagaFailed {
		t.Fatalf("expected both StepFailed and SagaFailed events, got %v", pub.types())
	}
}

func TestEvents_NoPublisherConfigured_NoOp(t *testing.T) {
	// No WithEventPublisher: Execute must behave exactly as before Event
	// existed (this is really just confirming no panic/behavior change).
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

type panickingPublisher struct{}

func (panickingPublisher) Publish(ctx context.Context, event Event) {
	panic("boom: this publisher is broken")
}

func TestEvents_PanickingPublisherDoesNotAffectExecution(t *testing.T) {
	d := New("order_creation", WithEventPublisher(panickingPublisher{})).
		AddStep(Step("A", noopAction, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v (a panicking publisher must not affect the saga)", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

func TestMultiPublisher_ForwardsToAll(t *testing.T) {
	pub1 := &recordingPublisher{}
	pub2 := &recordingPublisher{}
	d := New("order_creation", WithEventPublisher(MultiPublisher(pub1, pub2))).
		AddStep(Step("A", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if len(pub1.events) == 0 || len(pub2.events) == 0 {
		t.Fatalf("both publishers should have received events: pub1=%d pub2=%d", len(pub1.events), len(pub2.events))
	}
	if len(pub1.events) != len(pub2.events) {
		t.Errorf("pub1 got %d events, pub2 got %d, want equal", len(pub1.events), len(pub2.events))
	}
}

func TestEvents_FiredDuringRecovery(t *testing.T) {
	store := newRecoveryStore()
	pub := &recordingPublisher{}
	d := New("order_creation", WithStore(store), WithEventPublisher(pub)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "input"},
			{Name: "B", Status: StepStatusPending},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	if _, err := rm.Recover(context.Background()); err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}

	// A already succeeded before the "crash": no StepStarted/Completed
	// for A should fire again, only B's, then SagaCompleted. No
	// SagaStarted either, since the saga was already Running.
	assertEventTypes(t, pub.types(), []EventType{
		EventStepStarted, EventStepCompleted, // B
		EventSagaCompleted,
	})
}
