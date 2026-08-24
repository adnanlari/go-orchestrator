package idgen

import "testing"

func TestNew_Length(t *testing.T) {
	id := New()
	if len(id) != 32 {
		t.Errorf("len(New()) = %d, want 32", len(id))
	}
}

func TestNew_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := New()
		if seen[id] {
			t.Fatalf("New() produced a duplicate id: %s", id)
		}
		seen[id] = true
	}
}
