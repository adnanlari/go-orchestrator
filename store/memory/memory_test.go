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

func TestStore_ListIncomplete_OnlyNonTerminal(t *testing.T) {
	s := New()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("Save returned error: %v", err)
		}
	}
	must(s.Save(ctx, &saga.Execution{ID: "running", Status: saga.StatusRunning}))
	must(s.Save(ctx, &saga.Execution{ID: "compensating", Status: saga.StatusCompensating}))
	must(s.Save(ctx, &saga.Execution{ID: "completed", Status: saga.StatusCompleted}))
	must(s.Save(ctx, &saga.Execution{ID: "failed", Status: saga.StatusFailed}))

	got, err := s.ListIncomplete(ctx)
	if err != nil {
		t.Fatalf("ListIncomplete returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	ids := map[string]bool{}
	for _, exec := range got {
		ids[exec.ID] = true
	}
	if !ids["running"] || !ids["compensating"] {
		t.Errorf("got ids = %v, want running and compensating", ids)
	}
}

func TestStore_ListIncomplete_Empty(t *testing.T) {
	s := New()
	got, err := s.ListIncomplete(context.Background())
	if err != nil {
		t.Fatalf("ListIncomplete returned error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}

func TestStore_ListIncomplete_IsIndependentOfInternalStorage(t *testing.T) {
	s := New()
	if err := s.Save(context.Background(), &saga.Execution{ID: "exec-1", Status: saga.StatusRunning}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	got, err := s.ListIncomplete(context.Background())
	if err != nil {
		t.Fatalf("ListIncomplete returned error: %v", err)
	}
	got[0].Status = saga.StatusCompleted // mutate the caller's copy

	got2, err := s.Get(context.Background(), "exec-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got2.Status != saga.StatusRunning {
		t.Errorf("stored Status = %q, want unchanged %q", got2.Status, saga.StatusRunning)
	}
}
