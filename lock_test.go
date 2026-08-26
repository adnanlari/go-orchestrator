package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// lockingStore adds an in-memory Locker implementation on top of
// mapStore, kept separate from mapStore itself so most existing tests
// (using plain mapStore) continue to exercise the "Store doesn't
// implement Locker" no-op path, while locking-specific tests opt in here.
type lockingStore struct {
	*mapStore
	mu     sync.Mutex
	leases map[string]lockingLease
}

type lockingLease struct {
	owner   string
	expires time.Time
}

func newLockingStore() *lockingStore {
	return &lockingStore{mapStore: newMapStore(), leases: make(map[string]lockingLease)}
}

func (s *lockingStore) Acquire(ctx context.Context, id string, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if l, ok := s.leases[id]; ok && l.owner != owner && now.Before(l.expires) {
		return false, nil
	}
	s.leases[id] = lockingLease{owner: owner, expires: now.Add(ttl)}
	return true, nil
}

func (s *lockingStore) Release(ctx context.Context, id string, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.leases[id]; ok && l.owner == owner {
		delete(s.leases, id)
	}
	return nil
}

func (s *lockingStore) ListIncomplete(ctx context.Context) ([]*Execution, error) {
	s.mapStore.mu.Lock()
	defer s.mapStore.mu.Unlock()
	var result []*Execution
	for _, exec := range s.mapStore.data {
		if !exec.Status.IsTerminal() {
			result = append(result, exec.Clone())
		}
	}
	return result, nil
}

func TestLockingStore_AcquireBlocksDifferentOwner(t *testing.T) {
	s := newLockingStore()
	ok, err := s.Acquire(context.Background(), "exec-1", "owner-A", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first Acquire: ok=%v err=%v", ok, err)
	}
	ok, err = s.Acquire(context.Background(), "exec-1", "owner-B", time.Minute)
	if err != nil || ok {
		t.Fatalf("second Acquire by different owner: ok=%v err=%v, want ok=false", ok, err)
	}
}

func TestLockingStore_SameOwnerRenews(t *testing.T) {
	s := newLockingStore()
	for i := 0; i < 3; i++ {
		ok, err := s.Acquire(context.Background(), "exec-1", "owner-A", time.Minute)
		if err != nil || !ok {
			t.Fatalf("Acquire #%d: ok=%v err=%v", i, ok, err)
		}
	}
}

func TestLockingStore_ReleaseAllowsReacquisition(t *testing.T) {
	s := newLockingStore()
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-A", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if err := s.Release(context.Background(), "exec-1", "owner-A"); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-B", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire after release: ok=%v err=%v, want ok=true", ok, err)
	}
}

func TestLockingStore_ReleaseByWrongOwnerIsNoop(t *testing.T) {
	s := newLockingStore()
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-A", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if err := s.Release(context.Background(), "exec-1", "owner-B"); err != nil {
		t.Fatalf("Release returned error: %v", err)
	}
	// owner-A's lease must still stand.
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-C", time.Minute); err != nil || ok {
		t.Fatalf("Acquire by owner-C: ok=%v err=%v, want ok=false (owner-A's lease should be untouched)", ok, err)
	}
}

func TestLockingStore_ExpiredLeaseCanBeReclaimed(t *testing.T) {
	s := newLockingStore()
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-A", time.Millisecond); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	time.Sleep(20 * time.Millisecond)
	if ok, err := s.Acquire(context.Background(), "exec-1", "owner-B", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire after expiry: ok=%v err=%v, want ok=true", ok, err)
	}
}

func TestExecute_ReleasesLockAfterSuccess(t *testing.T) {
	store := newLockingStore()
	d := New("order_creation", WithStore(store)).AddStep(Step("A", noopAction, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// The lock must be released: a different owner can acquire it now.
	ok, acqErr := store.Acquire(context.Background(), exec.ID, "someone-else", time.Minute)
	if acqErr != nil || !ok {
		t.Fatalf("Acquire after Execute completed: ok=%v err=%v, want ok=true", ok, acqErr)
	}
}

func TestExecute_ReleasesLockAfterFailure(t *testing.T) {
	store := newLockingStore()
	failingErr := errors.New("boom")
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	ok, acqErr := store.Acquire(context.Background(), exec.ID, "someone-else", time.Minute)
	if acqErr != nil || !ok {
		t.Fatalf("Acquire after Execute failed: ok=%v err=%v, want ok=true", ok, acqErr)
	}
}

func TestExecute_LeaseIsRenewedAcrossMultipleSteps(t *testing.T) {
	// A short TTL that would expire between steps if it were never
	// renewed; since save() renews on every persisted transition, the
	// execution must still complete successfully.
	store := newLockingStore()
	d := New("order_creation", WithStore(store), WithLockTTL(5*time.Millisecond)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			time.Sleep(10 * time.Millisecond)
			return data, nil
		}, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			time.Sleep(10 * time.Millisecond)
			return data, nil
		}, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

func TestRecover_LockedExecutionIsSkipped(t *testing.T) {
	store := newLockingStore()
	var called bool
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			called = true
			return data, nil
		}, noopCompensate))

	seed2(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusPending, Input: "input",
		Steps:     []StepExecution{{Name: "A", Status: StepStatusPending}},
		CreatedAt: time.Now(),
	})

	// Simulate another worker already resuming this execution.
	if ok, err := store.Acquire(context.Background(), "exec-1", "other-worker", time.Minute); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	var target *ExecutionLockedError
	if !errors.As(results[0].Err, &target) {
		t.Fatalf("expected *ExecutionLockedError, got %T: %v", results[0].Err, results[0].Err)
	}
	if called {
		t.Error("Action must not run: the execution is locked by another worker")
	}
}

func TestRecover_ReleasesLockAfterRecovery(t *testing.T) {
	store := newLockingStore()
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate))

	seed2(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusPending, Input: "input",
		Steps:     []StepExecution{{Name: "A", Status: StepStatusPending}},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	if _, err := rm.Recover(context.Background()); err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}

	ok, err := store.Acquire(context.Background(), "exec-1", "someone-else", time.Minute)
	if err != nil || !ok {
		t.Fatalf("Acquire after Recover completed: ok=%v err=%v, want ok=true", ok, err)
	}
}

// seed2 mirrors seed (recovery_test.go) but for the *lockingStore fake
// used in this file.
func seed2(t *testing.T, store *lockingStore, exec *Execution) {
	t.Helper()
	if err := store.Save(context.Background(), exec); err != nil {
		t.Fatalf("seed: Save returned error: %v", err)
	}
}
