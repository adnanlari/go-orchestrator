package saga

import (
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
