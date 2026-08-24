package saga

import (
	"errors"
	"testing"
	"time"
)

func TestExecution_ZeroValue(t *testing.T) {
	var e Execution

	if e.Status != Status("") {
		t.Errorf("zero-value Execution.Status = %q, want empty", e.Status)
	}
	if e.Status.Valid() {
		t.Error("zero-value Execution.Status should not be Valid")
	}
	if e.Steps != nil {
		t.Errorf("zero-value Execution.Steps = %v, want nil", e.Steps)
	}
	if e.StartedAt != nil || e.CompletedAt != nil {
		t.Error("zero-value Execution should have nil StartedAt/CompletedAt")
	}
}

func TestExecution_FieldsRoundTrip(t *testing.T) {
	now := time.Now()
	e := Execution{
		ID:          "exec-1",
		SagaName:    "order_creation",
		Status:      StatusRunning,
		CurrentStep: "reserve_inventory",
		Steps: []StepExecution{
			{Name: "reserve_inventory", Status: StepStatusSucceeded},
			{Name: "charge_payment", Status: StepStatusRunning},
		},
		CreatedAt: now,
		UpdatedAt: now,
		StartedAt: &now,
	}

	if e.ID != "exec-1" {
		t.Errorf("ID = %q, want %q", e.ID, "exec-1")
	}
	if e.SagaName != "order_creation" {
		t.Errorf("SagaName = %q, want %q", e.SagaName, "order_creation")
	}
	if !e.Status.Valid() || e.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", e.Status, StatusRunning)
	}
	if len(e.Steps) != 2 {
		t.Fatalf("len(Steps) = %d, want 2", len(e.Steps))
	}
	// Step order must be preserved exactly as constructed — the engine
	// relies on Steps reflecting saga definition order.
	if e.Steps[0].Name != "reserve_inventory" || e.Steps[1].Name != "charge_payment" {
		t.Errorf("Steps order not preserved: %+v", e.Steps)
	}
	if e.CompletedAt != nil {
		t.Error("CompletedAt should still be nil for a running execution")
	}
}

func TestExecution_transition_PendingToRunning(t *testing.T) {
	e := &Execution{Status: StatusPending}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := e.transition(StatusRunning, now); err != nil {
		t.Fatalf("transition returned error: %v", err)
	}
	if e.Status != StatusRunning {
		t.Errorf("Status = %q, want %q", e.Status, StatusRunning)
	}
	if e.UpdatedAt != now {
		t.Errorf("UpdatedAt = %v, want %v", e.UpdatedAt, now)
	}
	if e.StartedAt == nil || !e.StartedAt.Equal(now) {
		t.Errorf("StartedAt = %v, want %v", e.StartedAt, now)
	}
	if e.CompletedAt != nil {
		t.Error("CompletedAt should still be nil after entering Running")
	}
}

func TestExecution_transition_TerminalSetsCompletedAt(t *testing.T) {
	tests := []struct {
		name string
		to   Status
	}{
		{"completed", StatusCompleted},
		{"failed", StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Execution{Status: StatusRunning}
			now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

			if err := e.transition(tt.to, now); err != nil {
				t.Fatalf("transition returned error: %v", err)
			}
			if e.CompletedAt == nil || !e.CompletedAt.Equal(now) {
				t.Errorf("CompletedAt = %v, want %v", e.CompletedAt, now)
			}
		})
	}
}

func TestExecution_transition_Invalid(t *testing.T) {
	e := &Execution{Status: StatusPending}
	now := time.Now()

	err := e.transition(StatusCompleted, now)
	if err == nil {
		t.Fatal("expected an error transitioning Pending directly to Completed")
	}

	var target *InvalidTransitionError
	if !errors.As(err, &target) {
		t.Fatalf("expected *InvalidTransitionError, got %T: %v", err, err)
	}
	if target.From != StatusPending || target.To != StatusCompleted {
		t.Errorf("InvalidTransitionError = %+v, want From=%s To=%s", target, StatusPending, StatusCompleted)
	}
	// A rejected transition must leave the execution unchanged.
	if e.Status != StatusPending {
		t.Errorf("Status changed to %q after a rejected transition", e.Status)
	}
	if e.UpdatedAt != (time.Time{}) {
		t.Error("UpdatedAt changed after a rejected transition")
	}
}

func TestExecution_transition_FullForwardSuccessPath(t *testing.T) {
	e := &Execution{Status: StatusPending}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	if err := e.transition(StatusRunning, t0); err != nil {
		t.Fatalf("Pending -> Running: %v", err)
	}
	if err := e.transition(StatusCompleted, t1); err != nil {
		t.Fatalf("Running -> Completed: %v", err)
	}
	if e.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", e.Status, StatusCompleted)
	}
	if !e.StartedAt.Equal(t0) {
		t.Errorf("StartedAt = %v, want %v", e.StartedAt, t0)
	}
	if !e.CompletedAt.Equal(t1) {
		t.Errorf("CompletedAt = %v, want %v", e.CompletedAt, t1)
	}
}

func TestExecution_transition_FullCompensationPath(t *testing.T) {
	e := &Execution{Status: StatusPending}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(e.transition(StatusRunning, t0))
	must(e.transition(StatusCompensating, t0))
	must(e.transition(StatusCompensated, t0))

	if e.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", e.Status, StatusCompensated)
	}
	if e.CompletedAt == nil {
		t.Error("CompletedAt should be set once Compensated is reached")
	}
}

func TestExecution_Clone_IndependentOfOriginal(t *testing.T) {
	started := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	original := &Execution{
		ID:     "exec-1",
		Status: StatusRunning,
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, StartedAt: &started},
		},
		StartedAt: &started,
	}

	clone := original.Clone()

	// Mutating the clone must not affect the original.
	clone.Status = StatusCompleted
	clone.Steps[0].Status = StepStatusCompensated
	*clone.StartedAt = started.Add(time.Hour)
	*clone.Steps[0].StartedAt = started.Add(time.Hour)

	if original.Status != StatusRunning {
		t.Errorf("original.Status = %q, want unchanged %q", original.Status, StatusRunning)
	}
	if original.Steps[0].Status != StepStatusSucceeded {
		t.Errorf("original.Steps[0].Status = %q, want unchanged %q", original.Steps[0].Status, StepStatusSucceeded)
	}
	if !original.StartedAt.Equal(started) {
		t.Errorf("original.StartedAt = %v, want unchanged %v", original.StartedAt, started)
	}
	if !original.Steps[0].StartedAt.Equal(started) {
		t.Errorf("original.Steps[0].StartedAt = %v, want unchanged %v", original.Steps[0].StartedAt, started)
	}
}

func TestExecution_Clone_NilTimestampsStayNil(t *testing.T) {
	original := &Execution{ID: "exec-1", Status: StatusPending}
	clone := original.Clone()

	if clone.StartedAt != nil || clone.CompletedAt != nil {
		t.Error("Clone of an Execution with nil timestamps should also have nil timestamps")
	}
}
