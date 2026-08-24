package saga

import "testing"

func TestStepStatus_Valid(t *testing.T) {
	tests := []struct {
		name   string
		status StepStatus
		want   bool
	}{
		{"pending", StepStatusPending, true},
		{"running", StepStatusRunning, true},
		{"succeeded", StepStatusSucceeded, true},
		{"failed", StepStatusFailed, true},
		{"compensating", StepStatusCompensating, true},
		{"compensated", StepStatusCompensated, true},
		{"compensation failed", StepStatusCompensationFailed, true},
		{"empty", StepStatus(""), false},
		{"unknown", StepStatus("BOGUS"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.Valid(); got != tt.want {
				t.Errorf("StepStatus(%q).Valid() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

func TestStepStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		status StepStatus
		want   bool
	}{
		{"pending is not terminal", StepStatusPending, false},
		{"running is not terminal", StepStatusRunning, false},
		{"succeeded is not terminal (may still be compensated)", StepStatusSucceeded, false},
		{"compensating is not terminal", StepStatusCompensating, false},
		{"failed is terminal", StepStatusFailed, true},
		{"compensated is terminal", StepStatusCompensated, true},
		{"compensation failed is terminal", StepStatusCompensationFailed, true},
		{"unknown is not terminal", StepStatus("BOGUS"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsTerminal(); got != tt.want {
				t.Errorf("StepStatus(%q).IsTerminal() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

// TestStepStatus_SucceededIsNotTerminal guards the deliberate design
// choice that a succeeded step remains eligible for compensation, so it
// must not be treated as terminal.
func TestStepStatus_SucceededIsNotTerminal(t *testing.T) {
	if StepStatusSucceeded.IsTerminal() {
		t.Fatal("StepStatusSucceeded must not be terminal: a succeeded step can still be compensated")
	}
}
