package saga

import (
	"errors"
	"fmt"
	"testing"
)

func TestStepError_Error(t *testing.T) {
	cause := errors.New("insufficient stock")
	err := &StepError{Step: "reserve_inventory", Err: cause}

	got := err.Error()
	want := `saga: step "reserve_inventory": insufficient stock`
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestStepError_Unwrap(t *testing.T) {
	cause := errors.New("insufficient stock")
	err := &StepError{Step: "reserve_inventory", Err: cause}

	if !errors.Is(err, cause) {
		t.Error("errors.Is(err, cause) = false, want true")
	}

	var target *StepError
	if !errors.As(err, &target) {
		t.Fatal("errors.As(err, &target) = false, want true")
	}
	if target.Step != "reserve_inventory" {
		t.Errorf("target.Step = %q, want %q", target.Step, "reserve_inventory")
	}
}

func TestStepError_UnwrapNil(t *testing.T) {
	err := &StepError{Step: "reserve_inventory", Err: nil}
	if err.Unwrap() != nil {
		t.Error("Unwrap() of a StepError with a nil Err should return nil")
	}
}

func TestErrCompensationFailed_Is(t *testing.T) {
	// The engine will, in a later phase, wrap ErrCompensationFailed
	// together with the underlying compensation error. Verify that
	// pattern is detectable via errors.Is once wrapped.
	underlying := errors.New("payment gateway unreachable")
	wrapped := fmt.Errorf("%w: %v", ErrCompensationFailed, underlying)

	if !errors.Is(wrapped, ErrCompensationFailed) {
		t.Error("errors.Is(wrapped, ErrCompensationFailed) = false, want true")
	}
}

func TestInvalidTransitionError_Error(t *testing.T) {
	err := &InvalidTransitionError{From: StatusPending, To: StatusCompleted}
	got := err.Error()
	want := "saga: invalid status transition PENDING -> COMPLETED"
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestInvalidStepTransitionError_Error(t *testing.T) {
	err := &InvalidStepTransitionError{Step: "charge_payment", From: StepStatusFailed, To: StepStatusCompensating}
	got := err.Error()
	want := `saga: invalid step "charge_payment" status transition FAILED -> COMPENSATING`
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestStepError_WrapsCompensationFailure(t *testing.T) {
	// A StepError can itself wrap ErrCompensationFailed, so callers can
	// detect both which step failed to compensate and that it was a
	// compensation (not forward) failure, in a single errors.Is/As chain.
	cause := fmt.Errorf("%w: refund declined", ErrCompensationFailed)
	err := &StepError{Step: "charge_payment", Err: cause}

	if !errors.Is(err, ErrCompensationFailed) {
		t.Error("errors.Is(err, ErrCompensationFailed) = false, want true")
	}
}
