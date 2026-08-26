package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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

func TestStore_Acquire_BlocksDifferentOwner(t *testing.T) {
	s := New()
	ctx := context.Background()
	ok, err := s.Acquire(ctx, "exec-1", "owner-A", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first Acquire: ok=%v err=%v", ok, err)
	}
	ok, err = s.Acquire(ctx, "exec-1", "owner-B", time.Minute)
	if err != nil || ok {
		t.Fatalf("second Acquire by different owner: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestStore_Acquire_SameOwnerRenews(t *testing.T) {
	s := New()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ok, err := s.Acquire(ctx, "exec-1", "owner-A", time.Minute)
		if err != nil || !ok {
			t.Fatalf("Acquire #%d: ok=%v err=%v", i, ok, err)
		}
	}
}

func TestStore_Release_AllowsReacquisition(t *testing.T) {
	s := New()
	ctx := context.Background()
	if ok, err := s.Acquire(ctx, "exec-1", "owner-A", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if err := s.Release(ctx, "exec-1", "owner-A"); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if ok, err := s.Acquire(ctx, "exec-1", "owner-B", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire after release: ok=%v err=%v, want ok=true", ok, err)
	}
}

func TestStore_Release_WrongOwnerIsNoop(t *testing.T) {
	s := New()
	ctx := context.Background()
	if ok, err := s.Acquire(ctx, "exec-1", "owner-A", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if err := s.Release(ctx, "exec-1", "owner-B"); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if ok, err := s.Acquire(ctx, "exec-1", "owner-C", time.Minute); err != nil || ok {
		t.Fatalf("Acquire by owner-C: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestStore_Acquire_ExpiredLeaseCanBeReclaimed(t *testing.T) {
	s := New()
	ctx := context.Background()
	if ok, err := s.Acquire(ctx, "exec-1", "owner-A", time.Millisecond); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	time.Sleep(20 * time.Millisecond)
	if ok, err := s.Acquire(ctx, "exec-1", "owner-B", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire after expiry: ok=%v err=%v, want ok=true", ok, err)
	}
}

func TestStore_Lock_ConcurrentUse(t *testing.T) {
	s := New()
	const n = 50
	var wg sync.WaitGroup
	var successCount int32
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := s.Acquire(context.Background(), "exec-1", "owner", time.Minute)
			if err != nil {
				t.Errorf("Acquire returned error: %v", err)
				return
			}
			if ok {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	// Same owner: every concurrent Acquire should succeed (renewal).
	if int(successCount) != n {
		t.Errorf("successCount = %d, want %d (same owner should always succeed)", successCount, n)
	}
}
