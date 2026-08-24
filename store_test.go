package saga

import (
	"context"
	"errors"
	"testing"
)

func TestNoopStore_SaveAlwaysSucceeds(t *testing.T) {
	var s noopStore
	if err := s.Save(context.Background(), &Execution{ID: "exec-1"}); err != nil {
		t.Errorf("Save returned error: %v", err)
	}
}

func TestNoopStore_GetAlwaysNotFound(t *testing.T) {
	var s noopStore
	_, err := s.Get(context.Background(), "exec-1")
	if !errors.Is(err, ErrExecutionNotFound) {
		t.Errorf("errors.Is(err, ErrExecutionNotFound) = false, err = %v", err)
	}
}
