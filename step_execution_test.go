package saga

import (
	"errors"
	"testing"
	"time"
)

func TestStepExecution_ZeroValue(t *testing.T) {
	var s StepExecution

	if s.Status != StepStatus("") {
		t.Errorf("zero-value StepExecution.Status = %q, want empty", s.Status)
	}
	if s.Status.Valid() {
		t.Error("zero-value StepExecution.Status should not be Valid")
	}
	if s.Attempts != 0 {
		t.Errorf("zero-value StepExecution.Attempts = %d, want 0", s.Attempts)
	}
	if s.StartedAt != nil || s.CompletedAt != nil || s.CompensationStartedAt != nil || s.CompensatedAt != nil {
		t.Error("zero-value StepExecution should have all timestamp pointers nil")
	}
}

func TestStepExecution_FieldsRoundTrip(t *testing.T) {
	start := time.Now()
	done := start.Add(time.Second)

	s := StepExecution{
		Name:        "reserve_inventory",
		Status:      StepStatusSucceeded,
		Attempts:    2,
		StartedAt:   &start,
		CompletedAt: &done,
	}

	if s.Name != "reserve_inventory" {
		t.Errorf("Name = %q, want %q", s.Name, "reserve_inventory")
	}
	if s.Status != StepStatusSucceeded {
		t.Errorf("Status = %q, want %q", s.Status, StepStatusSucceeded)
	}
	if s.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", s.Attempts)
	}
	if s.CompensationStartedAt != nil || s.CompensatedAt != nil {
		t.Error("a step that only succeeded forward should have nil compensation timestamps")
	}
	if s.StartedAt == nil || !s.StartedAt.Equal(start) {
		t.Errorf("StartedAt = %v, want %v", s.StartedAt, start)
	}
	if s.CompletedAt == nil || !s.CompletedAt.Equal(done) {
		t.Errorf("CompletedAt = %v, want %v", s.CompletedAt, done)
	}
}

func TestStepExecution_transition_PendingToRunning(t *testing.T) {
	s := &StepExecution{Name: "reserve_inventory", Status: StepStatusPending}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := s.transition(StepStatusRunning, now); err != nil {
		t.Fatalf("transition returned error: %v", err)
	}
	if s.Status != StepStatusRunning {
		t.Errorf("Status = %q, want %q", s.Status, StepStatusRunning)
	}
	if s.StartedAt == nil || !s.StartedAt.Equal(now) {
		t.Errorf("StartedAt = %v, want %v", s.StartedAt, now)
	}
}

func TestStepExecution_transition_ForwardSuccessPath(t *testing.T) {
	s := &StepExecution{Name: "reserve_inventory", Status: StepStatusPending}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(s.transition(StepStatusRunning, t0))
	must(s.transition(StepStatusSucceeded, t1))

	if s.Status != StepStatusSucceeded {
		t.Errorf("Status = %q, want %q", s.Status, StepStatusSucceeded)
	}
	if !s.StartedAt.Equal(t0) {
		t.Errorf("StartedAt = %v, want %v", s.StartedAt, t0)
	}
	if !s.CompletedAt.Equal(t1) {
		t.Errorf("CompletedAt = %v, want %v", s.CompletedAt, t1)
	}
	if s.CompensationStartedAt != nil || s.CompensatedAt != nil {
		t.Error("compensation timestamps should remain nil on the pure forward-success path")
	}
}

func TestStepExecution_transition_CompensationPath(t *testing.T) {
	s := &StepExecution{Name: "reserve_inventory", Status: StepStatusPending}
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(s.transition(StepStatusRunning, t0))
	must(s.transition(StepStatusSucceeded, t1))
	must(s.transition(StepStatusCompensating, t2))
	must(s.transition(StepStatusCompensated, t3))

	if s.Status != StepStatusCompensated {
		t.Errorf("Status = %q, want %q", s.Status, StepStatusCompensated)
	}
	if !s.CompensationStartedAt.Equal(t2) {
		t.Errorf("CompensationStartedAt = %v, want %v", s.CompensationStartedAt, t2)
	}
	if !s.CompensatedAt.Equal(t3) {
		t.Errorf("CompensatedAt = %v, want %v", s.CompensatedAt, t3)
	}
	// The forward-run timestamps must be untouched by compensation.
	if !s.StartedAt.Equal(t0) || !s.CompletedAt.Equal(t1) {
		t.Error("compensation must not overwrite the forward-run timestamps")
	}
}

func TestStepExecution_transition_Invalid(t *testing.T) {
	s := &StepExecution{Name: "charge_payment", Status: StepStatusFailed}
	now := time.Now()

	err := s.transition(StepStatusCompensating, now)
	if err == nil {
		t.Fatal("expected an error compensating a step that failed forward")
	}

	var target *InvalidStepTransitionError
	if !errors.As(err, &target) {
		t.Fatalf("expected *InvalidStepTransitionError, got %T: %v", err, err)
	}
	if target.Step != "charge_payment" || target.From != StepStatusFailed || target.To != StepStatusCompensating {
		t.Errorf("InvalidStepTransitionError = %+v, unexpected fields", target)
	}
	if s.Status != StepStatusFailed {
		t.Errorf("Status changed to %q after a rejected transition", s.Status)
	}
}
