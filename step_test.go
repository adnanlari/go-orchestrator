package saga

import (
	"context"
	"testing"
)

func TestStep_ConstructsStepDefinition(t *testing.T) {
	action := func(ctx context.Context, data any) (any, error) { return data, nil }
	compensate := func(ctx context.Context, data any) error { return nil }

	s := Step("reserve_inventory", action, compensate)

	if s.Name != "reserve_inventory" {
		t.Errorf("Name = %q, want %q", s.Name, "reserve_inventory")
	}
	if s.Action == nil {
		t.Error("Action should not be nil")
	}
	if s.Compensate == nil {
		t.Error("Compensate should not be nil")
	}
}

func TestStep_NilCompensateAllowed(t *testing.T) {
	action := func(ctx context.Context, data any) (any, error) { return data, nil }

	s := Step("send_notification", action, nil)

	if s.Compensate != nil {
		t.Error("Compensate should be nil when passed nil")
	}
}
