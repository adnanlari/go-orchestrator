package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// mapStore is a minimal, in-package Store used to test that Execute
// wires into the Store interface correctly. store/memory (which cannot
// be imported here without an import cycle, since it itself imports this
// package) is tested independently in its own package.
type mapStore struct {
	mu   sync.Mutex
	data map[string]*Execution
}

func newMapStore() *mapStore { return &mapStore{data: make(map[string]*Execution)} }

func (s *mapStore) Save(ctx context.Context, exec *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[exec.ID] = exec.Clone()
	return nil
}

func (s *mapStore) Get(ctx context.Context, id string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec, ok := s.data[id]
	if !ok {
		return nil, ErrExecutionNotFound
	}
	return exec.Clone(), nil
}

// --- Store wiring ---

func TestExecute_PersistsSuccessfulRun(t *testing.T) {
	store := newMapStore()
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	got, err := store.Get(context.Background(), exec.ID)
	if err != nil {
		t.Fatalf("store.Get returned error: %v", err)
	}
	if got.Status != StatusCompleted {
		t.Errorf("persisted Status = %q, want %q", got.Status, StatusCompleted)
	}
	if len(got.Steps) != 2 || got.Steps[0].Status != StepStatusSucceeded || got.Steps[1].Status != StepStatusSucceeded {
		t.Errorf("persisted Steps = %+v, want both Succeeded", got.Steps)
	}
}

func TestExecute_PersistsCompensatedRun(t *testing.T) {
	store := newMapStore()
	failingErr := errors.New("insufficient stock")
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	got, getErr := store.Get(context.Background(), exec.ID)
	if getErr != nil {
		t.Fatalf("store.Get returned error: %v", getErr)
	}
	if got.Status != StatusCompensated {
		t.Errorf("persisted Status = %q, want %q", got.Status, StatusCompensated)
	}
	if got.Steps[0].Status != StepStatusCompensated {
		t.Errorf("persisted step A Status = %q, want %q", got.Steps[0].Status, StepStatusCompensated)
	}
}

func TestExecute_WithoutStore_BehavesAsBeforeStoreExisted(t *testing.T) {
	// No WithStore: the default noopStore must not change any observable
	// behavior versus Phases 4/5.
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

// failingStore fails every Save after allowedSaves successful ones, to
// exercise the "Store failure is fatal to Execute" path deterministically.
type failingStore struct {
	mu        sync.Mutex
	allowed   int
	saveCount int
	saveErr   error
	lastSaved *Execution
}

func (s *failingStore) Save(ctx context.Context, exec *Execution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCount++
	if s.saveCount > s.allowed {
		return s.saveErr
	}
	s.lastSaved = exec.Clone()
	return nil
}

func (s *failingStore) Get(ctx context.Context, id string) (*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSaved == nil || s.lastSaved.ID != id {
		return nil, ErrExecutionNotFound
	}
	return s.lastSaved.Clone(), nil
}

func TestExecute_StoreFailureIsFatal(t *testing.T) {
	storeErr := errors.New("connection refused")
	store := &failingStore{allowed: 0, saveErr: storeErr} // fail even the very first Save

	var actionRan bool
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			actionRan = true
			return data, nil
		}, noopCompensate))

	_, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, storeErr) {
		t.Fatalf("errors.Is(err, storeErr) = false, err = %v", err)
	}
	if actionRan {
		t.Error("step Action should never run if the very first Save (creating the execution) fails")
	}
}

func TestExecute_StoreFailureMidRunStopsExecution(t *testing.T) {
	storeErr := errors.New("connection refused")
	// Saves, in order: (1) initial Pending record, (2) Pending->Running,
	// (3) A Pending->Running, (4) A Running->Succeeded. Allowing exactly
	// those 4 means the 5th save — B's Pending->Running — is the one that
	// fails, so A's Action must have run but B's must not have.
	store := &failingStore{allowed: 4, saveErr: storeErr}

	var order []string
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", recordingAction("A", &order), noopCompensate)).
		AddStep(Step("B", recordingAction("B", &order), noopCompensate))

	_, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, storeErr) {
		t.Fatalf("errors.Is(err, storeErr) = false, err = %v", err)
	}
	if len(order) != 1 || order[0] != "A" {
		t.Errorf("order = %v, want [A] (B must not run after a Store failure)", order)
	}
}

// --- Retry wiring ---

func TestExecute_RetriesOnFailureThenSucceeds(t *testing.T) {
	var attempts int
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("transient error")
		}
		return "ok", nil
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 5))).
		AddStep(Step("A", action, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if exec.Steps[0].Attempts != 3 {
		t.Errorf("exec.Steps[0].Attempts = %d, want 3", exec.Steps[0].Attempts)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

func TestExecute_RetriesExhausted(t *testing.T) {
	var attempts int
	permanentErr := errors.New("still down")
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		return nil, permanentErr
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 3))).
		AddStep(Step("A", action, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, permanentErr) {
		t.Fatalf("errors.Is(err, permanentErr) = false, err = %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (MaxAttempts)", attempts)
	}
	if exec.Steps[0].Attempts != 3 {
		t.Errorf("exec.Steps[0].Attempts = %d, want 3", exec.Steps[0].Attempts)
	}
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusFailed)
	}
}

func TestExecute_NonRetryableErrorSkipsRetries(t *testing.T) {
	var attempts int
	permanentErr := errors.New("invalid SKU")
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		return nil, NonRetryable(permanentErr)
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 5))).
		AddStep(Step("A", action, noopCompensate))

	_, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, permanentErr) {
		t.Fatalf("errors.Is(err, permanentErr) = false, err = %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (NonRetryable must skip retries entirely)", attempts)
	}
}

func TestExecute_StepRetryPolicyOverridesSagaPolicy(t *testing.T) {
	var attemptsA, attemptsB int
	failThenSucceed := func(counter *int) ActionFunc {
		return func(ctx context.Context, data any) (any, error) {
			*counter++
			if *counter < 2 {
				return nil, errors.New("transient")
			}
			return data, nil
		}
	}

	// Saga default is NoRetry; step A overrides it via WithStepRetryPolicy,
	// step B uses the saga default as-is.
	stepA := Step("A", failThenSucceed(&attemptsA), noopCompensate,
		WithStepRetryPolicy(FixedDelay(time.Millisecond, 3)))
	stepB := Step("B", failThenSucceed(&attemptsB), noopCompensate)
	d := New("order_creation", WithRetryPolicy(NoRetry())).AddStep(stepA).AddStep(stepB)

	_, err := d.Execute(context.Background(), "input")
	if err == nil {
		t.Fatal("expected an error: step B has no retries and fails on its first attempt")
	}
	if attemptsA != 2 {
		t.Errorf("attemptsA = %d, want 2 (step-level retry policy should have retried once)", attemptsA)
	}
	if attemptsB != 1 {
		t.Errorf("attemptsB = %d, want 1 (saga-level NoRetry should apply)", attemptsB)
	}
}

func TestExecute_RetryRespectsContextCancellation(t *testing.T) {
	var attempts int
	ctx, cancel := context.WithCancel(context.Background())
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		if attempts == 1 {
			cancel() // cancel during the retry backoff wait after attempt 1
		}
		return nil, errors.New("transient")
	}
	// A long delay: if cancellation weren't respected, this test would hang.
	d := New("order_creation", WithRetryPolicy(FixedDelay(10*time.Second, 5))).
		AddStep(Step("A", action, noopCompensate))

	done := make(chan struct{})
	var err error
	go func() {
		_, err = d.Execute(ctx, "input")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return promptly after ctx was cancelled during a retry wait")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false, err = %v", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (must not retry after cancellation)", attempts)
	}
}
