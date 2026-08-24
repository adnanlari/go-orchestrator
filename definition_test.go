package saga

import (
	"context"
	"strings"
	"testing"
)

func noopAction(ctx context.Context, data any) (any, error) { return data, nil }
func noopCompensate(ctx context.Context, data any) error    { return nil }

// assertPanics runs fn and fails the test unless it panics with a message
// containing want.
func assertPanics(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got no panic", want)
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("expected panic value to be a string, got %T: %v", r, r)
		}
		if !strings.Contains(msg, want) {
			t.Fatalf("panic message %q does not contain %q", msg, want)
		}
	}()
	fn()
}

func TestNew(t *testing.T) {
	d := New("order_creation")
	if d.Name() != "order_creation" {
		t.Errorf("Name() = %q, want %q", d.Name(), "order_creation")
	}
	if len(d.Steps()) != 0 {
		t.Errorf("new Definition should have no steps, got %d", len(d.Steps()))
	}
	if d.Frozen() {
		t.Error("new Definition should not be frozen")
	}
}

func TestNew_EmptyNamePanics(t *testing.T) {
	assertPanics(t, "saga name must not be empty", func() {
		New("")
	})
}

func TestNew_WhitespaceNamePanics(t *testing.T) {
	assertPanics(t, "saga name must not be empty", func() {
		New("   ")
	})
}

func TestDefinition_AddStep_Valid(t *testing.T) {
	d := New("order_creation")
	got := d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))

	if got != d {
		t.Error("AddStep should return the same Definition for chaining")
	}
	steps := d.Steps()
	if len(steps) != 1 {
		t.Fatalf("len(Steps()) = %d, want 1", len(steps))
	}
	if steps[0].Name != "reserve_inventory" {
		t.Errorf("Steps()[0].Name = %q, want %q", steps[0].Name, "reserve_inventory")
	}
}

func TestDefinition_AddStep_PreservesOrder(t *testing.T) {
	d := New("order_creation")
	d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))
	d.AddStep(Step("charge_payment", noopAction, noopCompensate))
	d.AddStep(Step("ship_order", noopAction, noopCompensate))

	steps := d.Steps()
	wantNames := []string{"reserve_inventory", "charge_payment", "ship_order"}
	if len(steps) != len(wantNames) {
		t.Fatalf("len(Steps()) = %d, want %d", len(steps), len(wantNames))
	}
	for i, want := range wantNames {
		if steps[i].Name != want {
			t.Errorf("Steps()[%d].Name = %q, want %q", i, steps[i].Name, want)
		}
	}
}

func TestDefinition_AddStep_NilCompensateAllowed(t *testing.T) {
	d := New("order_creation")
	d.AddStep(Step("send_notification", noopAction, nil))

	steps := d.Steps()
	if steps[0].Compensate != nil {
		t.Error("Compensate should remain nil")
	}
}

func TestDefinition_AddStep_EmptyNamePanics(t *testing.T) {
	d := New("order_creation")
	assertPanics(t, "step name must not be empty", func() {
		d.AddStep(Step("", noopAction, noopCompensate))
	})
}

func TestDefinition_AddStep_WhitespaceNamePanics(t *testing.T) {
	d := New("order_creation")
	assertPanics(t, "step name must not be empty", func() {
		d.AddStep(Step("   ", noopAction, noopCompensate))
	})
}

func TestDefinition_AddStep_DuplicateNamePanics(t *testing.T) {
	d := New("order_creation")
	d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))

	assertPanics(t, `duplicate step name "reserve_inventory"`, func() {
		d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))
	})
}

func TestDefinition_AddStep_NilActionPanics(t *testing.T) {
	d := New("order_creation")
	assertPanics(t, `step "reserve_inventory" must have a non-nil action`, func() {
		d.AddStep(Step("reserve_inventory", nil, noopCompensate))
	})
}

func TestDefinition_AddStep_AfterFreezePanics(t *testing.T) {
	d := New("order_creation")
	d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))
	d.Freeze()

	assertPanics(t, "cannot add step", func() {
		d.AddStep(Step("charge_payment", noopAction, noopCompensate))
	})
}

func TestDefinition_Freeze_Idempotent(t *testing.T) {
	d := New("order_creation")
	d.Freeze()
	d.Freeze() // must not panic
	if !d.Frozen() {
		t.Error("Frozen() should be true after Freeze()")
	}
}

func TestDefinition_Steps_ReturnsDefensiveCopy(t *testing.T) {
	d := New("order_creation")
	d.AddStep(Step("reserve_inventory", noopAction, noopCompensate))

	steps := d.Steps()
	steps[0].Name = "mutated"

	original := d.Steps()
	if original[0].Name != "reserve_inventory" {
		t.Errorf("mutating the slice returned by Steps() affected the Definition: got %q", original[0].Name)
	}
}
