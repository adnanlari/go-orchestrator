package memory

import (
	"context"
	"errors"
	"sync"
	"testing"

	saga "github.com/adnanlari/go-orchestrator"
)

func TestStore_SaveAndGet(t *testing.T) {
	s := New()
	exec := &saga.Execution{ID: "exec-1", SagaName: "order_creation", Status: saga.StatusRunning}

	if err := s.Save(context.Background(), exec); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	got, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.ID != exec.ID || got.SagaName != exec.SagaName || got.Status != exec.Status {
		t.Errorf("Get returned %+v, want fields matching %+v", got, exec)
	}
}

func TestStore_GetMissing(t *testing.T) {
	s := New()
	_, err := s.Get(context.Background(), "does-not-exist")
	if !errors.Is(err, saga.ErrExecutionNotFound) {
		t.Errorf("errors.Is(err, ErrExecutionNotFound) = false, err = %v", err)
	}
}

func TestStore_SaveOverwrites(t *testing.T) {
	s := New()
	exec := &saga.Execution{ID: "exec-1", Status: saga.StatusRunning}
	if err := s.Save(context.Background(), exec); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	exec.Status = saga.StatusCompleted
	if err := s.Save(context.Background(), exec); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	got, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != saga.StatusCompleted {
		t.Errorf("Status = %q, want %q", got.Status, saga.StatusCompleted)
	}
}

func TestStore_SaveRejectsNilOrEmptyID(t *testing.T) {
	s := New()
	if err := s.Save(context.Background(), nil); err == nil {
		t.Error("Save(nil) should return an error")
	}
	if err := s.Save(context.Background(), &saga.Execution{}); err == nil {
		t.Error("Save with an empty ID should return an error")
	}
}

func TestStore_SaveIsIndependentOfCaller(t *testing.T) {
	s := New()
	exec := &saga.Execution{ID: "exec-1", Status: saga.StatusRunning}
	if err := s.Save(context.Background(), exec); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	// Mutating the caller's copy after Save must not affect what was stored.
	exec.Status = saga.StatusCompleted

	got, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got.Status != saga.StatusRunning {
		t.Errorf("stored Status = %q, want unchanged %q", got.Status, saga.StatusRunning)
	}
}

func TestStore_GetIsIndependentOfInternalStorage(t *testing.T) {
	s := New()
	exec := &saga.Execution{ID: "exec-1", Status: saga.StatusRunning}
	if err := s.Save(context.Background(), exec); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	got, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	got.Status = saga.StatusCompleted // mutate the caller's copy

	got2, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got2.Status != saga.StatusRunning {
		t.Errorf("stored Status = %q, want unchanged %q", got2.Status, saga.StatusRunning)
	}
}

func TestStore_ConcurrentUse(t *testing.T) {
	s := New()
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := string(rune('a' + i%26))
			_ = s.Save(context.Background(), &saga.Execution{ID: id, Status: saga.StatusRunning})
			_, _ = s.Get(context.Background(), id)
		}(i)
	}
	wg.Wait()
}
