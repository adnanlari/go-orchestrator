package saga

import "testing"

func TestStatus_Valid(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		want   bool
	}{
		{"pending", StatusPending, true},
		{"running", StatusRunning, true},
		{"completed", StatusCompleted, true},
		{"compensating", StatusCompensating, true},
		{"compensated", StatusCompensated, true},
		{"failed", StatusFailed, true},
		{"compensation failed", StatusCompensationFailed, true},
		{"empty", Status(""), false},
		{"unknown", Status("BOGUS"), false},
		{"lowercase of a real value is not valid", Status("pending"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.Valid(); got != tt.want {
				t.Errorf("Status(%q).Valid() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		status Status
		want   bool
	}{
		{"pending is not terminal", StatusPending, false},
		{"running is not terminal", StatusRunning, false},
		{"compensating is not terminal", StatusCompensating, false},
		{"completed is terminal", StatusCompleted, true},
		{"compensated is terminal", StatusCompensated, true},
		{"failed is terminal", StatusFailed, true},
		{"compensation failed is terminal", StatusCompensationFailed, true},
		{"unknown is not terminal", Status("BOGUS"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.status.IsTerminal(); got != tt.want {
				t.Errorf("Status(%q).IsTerminal() = %v, want %v", tt.status, got, tt.want)
			}
		})
	}
}

// TestStatus_CompletedVsCompensatedAreDistinct guards the deliberate design
// choice that a saga which succeeded outright and a saga which failed but
// rolled back successfully are different, distinguishable terminal states.
func TestStatus_CompletedVsCompensatedAreDistinct(t *testing.T) {
	if StatusCompleted == StatusCompensated {
		t.Fatal("StatusCompleted and StatusCompensated must be distinct values")
	}
}
