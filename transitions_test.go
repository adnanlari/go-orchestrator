package saga

import "testing"

// TestCanTransitionStatus exhaustively checks every (from, to) pair over
// all known Status values, so both "every valid transition is tested"
// and "invalid transitions are tested" are satisfied by construction:
// anything not explicitly listed as valid is asserted invalid.
func TestCanTransitionStatus(t *testing.T) {
	valid := map[[2]Status]bool{
		{StatusPending, StatusRunning}:                 true,
		{StatusRunning, StatusCompleted}:               true,
		{StatusRunning, StatusCompensating}:            true,
		{StatusRunning, StatusFailed}:                  true,
		{StatusCompensating, StatusCompensated}:        true,
		{StatusCompensating, StatusCompensationFailed}: true,
	}

	all := []Status{
		StatusPending, StatusRunning, StatusCompleted, StatusCompensating,
		StatusCompensated, StatusFailed, StatusCompensationFailed,
	}

	for _, from := range all {
		for _, to := range all {
			want := valid[[2]Status{from, to}]
			if got := canTransitionStatus(from, to); got != want {
				t.Errorf("canTransitionStatus(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestCanTransitionStatus_UnknownStatus(t *testing.T) {
	if canTransitionStatus(Status(""), StatusRunning) {
		t.Error("empty status should not transition to Running")
	}
	if canTransitionStatus(StatusPending, Status("BOGUS")) {
		t.Error("should not be able to transition to an unknown status")
	}
}

// TestCanTransitionStepStatus exhaustively checks every (from, to) pair
// over all known StepStatus values, for the same reason as
// TestCanTransitionStatus above.
func TestCanTransitionStepStatus(t *testing.T) {
	valid := map[[2]StepStatus]bool{
		{StepStatusPending, StepStatusRunning}:                 true,
		{StepStatusRunning, StepStatusSucceeded}:               true,
		{StepStatusRunning, StepStatusFailed}:                  true,
		{StepStatusSucceeded, StepStatusCompensating}:          true,
		{StepStatusCompensating, StepStatusCompensated}:        true,
		{StepStatusCompensating, StepStatusCompensationFailed}: true,
	}

	all := []StepStatus{
		StepStatusPending, StepStatusRunning, StepStatusSucceeded, StepStatusFailed,
		StepStatusCompensating, StepStatusCompensated, StepStatusCompensationFailed,
	}

	for _, from := range all {
		for _, to := range all {
			want := valid[[2]StepStatus{from, to}]
			if got := canTransitionStepStatus(from, to); got != want {
				t.Errorf("canTransitionStepStatus(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestCanTransitionStepStatus_UnknownStatus(t *testing.T) {
	if canTransitionStepStatus(StepStatus(""), StepStatusRunning) {
		t.Error("empty step status should not transition to Running")
	}
	if canTransitionStepStatus(StepStatusPending, StepStatus("BOGUS")) {
		t.Error("should not be able to transition to an unknown step status")
	}
}

// TestStepStatusSucceeded_OnlyLeadsToCompensating guards the saga
// semantics that a step which failed forward (StepStatusFailed) must
// never be compensated — only a step that reached StepStatusSucceeded
// can transition into StepStatusCompensating.
func TestStepStatusFailed_HasNoOutgoingTransitions(t *testing.T) {
	for _, to := range []StepStatus{
		StepStatusPending, StepStatusRunning, StepStatusSucceeded,
		StepStatusCompensating, StepStatusCompensated, StepStatusCompensationFailed,
	} {
		if canTransitionStepStatus(StepStatusFailed, to) {
			t.Errorf("a step that failed forward must never transition to %s (must not be compensated)", to)
		}
	}
}
