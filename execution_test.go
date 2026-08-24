package saga

import (
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
