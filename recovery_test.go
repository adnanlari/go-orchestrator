package saga

import (
	"context"
	"errors"
	"testing"
	"time"
)

// recoveryStore extends mapStore (defined in persistence_retry_test.go)
// with ListIncomplete, so it satisfies both Store and Lister.
type recoveryStore struct {
	*mapStore
}

func newRecoveryStore() *recoveryStore {
	return &recoveryStore{mapStore: newMapStore()}
}

func (s *recoveryStore) ListIncomplete(ctx context.Context) ([]*Execution, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*Execution
	for _, exec := range s.data {
		if !exec.Status.IsTerminal() {
			result = append(result, exec.Clone())
		}
	}
	return result, nil
}

// seed directly persists exec into the store, simulating what a process
// would have written before crashing mid-run — Execute is never called.
func seed(t *testing.T, store *recoveryStore, exec *Execution) {
	t.Helper()
	if err := store.Save(context.Background(), exec); err != nil {
		t.Fatalf("seed: Save returned error: %v", err)
	}
}

func TestListIncomplete_OnlyReturnsNonTerminal(t *testing.T) {
	store := newRecoveryStore()
	seed(t, store, &Execution{ID: "incomplete-1", Status: StatusRunning, CreatedAt: time.Now()})
	seed(t, store, &Execution{ID: "incomplete-2", Status: StatusCompensating, CreatedAt: time.Now()})
	seed(t, store, &Execution{ID: "done-1", Status: StatusCompleted, CreatedAt: time.Now()})
	seed(t, store, &Execution{ID: "done-2", Status: StatusFailed, CreatedAt: time.Now()})

	got, err := store.ListIncomplete(context.Background())
	if err != nil {
		t.Fatalf("ListIncomplete returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}
	ids := map[string]bool{got[0].ID: true, got[1].ID: true}
	if !ids["incomplete-1"] || !ids["incomplete-2"] {
		t.Errorf("got = %v, want incomplete-1 and incomplete-2", ids)
	}
}

func TestNewRecoveryManager_NilListerPanics(t *testing.T) {
	assertPanics(t, "requires a non-nil Lister", func() {
		NewRecoveryManager(nil)
	})
}

func TestWithSaga_NilDefinitionPanics(t *testing.T) {
	assertPanics(t, "requires a non-nil Definition", func() {
		WithSaga(nil)
	})
}

func TestWithSaga_DuplicateNamePanics(t *testing.T) {
	store := newRecoveryStore()
	d1 := New("order_creation")
	d2 := New("order_creation")
	assertPanics(t, `two definitions named "order_creation"`, func() {
		NewRecoveryManager(store, WithSaga(d1), WithSaga(d2))
	})
}

func TestRecover_NoIncompleteExecutions(t *testing.T) {
	store := newRecoveryStore()
	rm := NewRecoveryManager(store)

	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("len(results) = %d, want 0", len(results))
	}
}

func TestRecover_ListErrorPropagates(t *testing.T) {
	listErr := errors.New("store unavailable")
	rm := NewRecoveryManager(failingLister{err: listErr})

	_, err := rm.Recover(context.Background())
	if !errors.Is(err, listErr) {
		t.Errorf("errors.Is(err, listErr) = false, err = %v", err)
	}
}

type failingLister struct{ err error }

func (f failingLister) ListIncomplete(ctx context.Context) ([]*Execution, error) {
	return nil, f.err
}

func TestRecover_UnregisteredSagaReportsErrorButContinues(t *testing.T) {
	store := newRecoveryStore()
	seed(t, store, &Execution{
		ID: "exec-unknown", SagaName: "unknown_saga", Status: StatusPending,
		Steps:     []StepExecution{{Name: "A", Status: StepStatusPending}},
		CreatedAt: time.Now(),
	})

	var called bool
	d := New("order_creation").AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
		called = true
		return data, nil
	}, noopCompensate))
	seed(t, store, &Execution{
		ID: "exec-known", SagaName: "order_creation", Status: StatusPending,
		Steps:     []StepExecution{{Name: "A", Status: StepStatusPending}},
		CreatedAt: time.Now().Add(time.Second),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("len(results) = %d, want 2", len(results))
	}

	var unknownResult, knownResult *RecoveryResult
	for i := range results {
		switch results[i].Execution.ID {
		case "exec-unknown":
			unknownResult = &results[i]
		case "exec-known":
			knownResult = &results[i]
		}
	}
	if unknownResult == nil || unknownResult.Err == nil {
		t.Fatal("expected an error result for the unregistered saga")
	}
	if knownResult == nil || knownResult.Err != nil {
		t.Fatalf("expected the registered saga's execution to recover cleanly, got %+v", knownResult)
	}
	if !called {
		t.Error("the known saga's step should still have run despite the other execution's unregistered saga")
	}
}

func TestRecover_DeterministicOrder(t *testing.T) {
	store := newRecoveryStore()
	base := time.Now()
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))

	// Seed out of order; CreatedAt determines the expected result order.
	seed(t, store, &Execution{ID: "c", SagaName: "order_creation", Status: StatusPending,
		Steps: []StepExecution{{Name: "A", Status: StepStatusPending}}, CreatedAt: base.Add(2 * time.Second)})
	seed(t, store, &Execution{ID: "a", SagaName: "order_creation", Status: StatusPending,
		Steps: []StepExecution{{Name: "A", Status: StepStatusPending}}, CreatedAt: base})
	seed(t, store, &Execution{ID: "b", SagaName: "order_creation", Status: StatusPending,
		Steps: []StepExecution{{Name: "A", Status: StepStatusPending}}, CreatedAt: base.Add(time.Second)})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("len(results) = %d, want 3", len(results))
	}
	want := []string{"a", "b", "c"}
	for i, id := range want {
		if results[i].Execution.ID != id {
			t.Errorf("results[%d].Execution.ID = %q, want %q", i, results[i].Execution.ID, id)
		}
	}
}

func TestRecover_PendingExecution_CompletesNormally(t *testing.T) {
	store := newRecoveryStore()
	var order []string
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", recordingAction("A", &order), noopCompensate)).
		AddStep(Step("B", recordingAction("B", &order), noopCompensate))

	// Simulates a crash between saving the initial Pending record and
	// ever transitioning to Running.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusPending, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusPending},
			{Name: "B", Status: StepStatusPending},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("unexpected results: %+v", results)
	}
	if results[0].Execution.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompleted)
	}
	if len(order) != 2 || order[0] != "A" || order[1] != "B" {
		t.Errorf("order = %v, want [A B]", order)
	}
}

func TestRecover_RunningExecution_SkipsAlreadySucceededSteps(t *testing.T) {
	store := newRecoveryStore()
	var calledA, calledB bool
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			calledA = true
			return "should-not-run", nil
		}, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			calledB = true
			if data != "A-output" {
				t.Errorf("step B received data = %v, want %q (A's persisted Output)", data, "A-output")
			}
			return "B-output", nil
		}, noopCompensate))

	// Simulates a crash after A succeeded and was persisted, but before B
	// started.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "A-output"},
			{Name: "B", Status: StepStatusPending},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("unexpected results: %+v", results)
	}
	if calledA {
		t.Error("step A's Action must not run again: it already succeeded before the crash")
	}
	if !calledB {
		t.Error("step B's Action should have run")
	}
	if results[0].Execution.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompleted)
	}
}

func TestRecover_RunningExecution_ReRunsInFlightStep(t *testing.T) {
	store := newRecoveryStore()
	var attempts int
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			attempts++
			return "B-output", nil
		}, noopCompensate))

	// Simulates a crash while B's Action was in flight: its own outcome
	// (success or failure) was never known/persisted.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "A-output"},
			{Name: "B", Status: StepStatusRunning, Attempts: 1},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("unexpected results: %+v", results)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (B's Action should be re-invoked exactly once)", attempts)
	}
	if results[0].Execution.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompleted)
	}
}

func TestRecover_RunningExecutionWithPersistedFailure_TriggersCompensation(t *testing.T) {
	store := newRecoveryStore()
	var compensateOrder []string
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, recordingCompensate("A", &compensateOrder))).
		AddStep(Step("B", noopAction, recordingCompensate("B", &compensateOrder)))

	// Simulates a crash after B's Action failed and that was persisted,
	// but before the saga-level transition to StatusCompensating was.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Error: "boom",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "A-output"},
			{Name: "B", Status: StepStatusFailed, Error: "boom"},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if results[0].Err == nil {
		t.Fatal("expected a non-nil error (the original failure), got nil")
	}
	if results[0].Execution.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompensated)
	}
	// B never succeeded, so it must never be compensated; only A should.
	if len(compensateOrder) != 1 || compensateOrder[0] != "A" {
		t.Errorf("compensateOrder = %v, want [A]", compensateOrder)
	}
}

func TestRecover_NoDuplicateCompensation(t *testing.T) {
	store := newRecoveryStore()
	var compensateCalls int
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error {
			compensateCalls++
			return nil
		})).
		AddStep(Step("B", noopAction, noopCompensate))

	// Simulates a crash after A was already fully compensated and that
	// was persisted, but before the saga reached a terminal status.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusCompensating, Input: "input",
		Error: "boom",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusCompensated, Output: "A-output"},
			{Name: "B", Status: StepStatusFailed, Error: "boom"},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if compensateCalls != 0 {
		t.Errorf("compensateCalls = %d, want 0: A was already compensated before the crash and must not run again", compensateCalls)
	}
	if results[0].Execution.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompensated)
	}
}

func TestRecover_ResumesInFlightCompensation(t *testing.T) {
	store := newRecoveryStore()
	var compensateCalls int
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error {
			compensateCalls++
			return nil
		})).
		AddStep(Step("B", noopAction, noopCompensate))

	// Simulates a crash while A's Compensate was in flight.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusCompensating, Input: "input",
		Error: "boom",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusCompensating, Output: "A-output"},
			{Name: "B", Status: StepStatusFailed, Error: "boom"},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if compensateCalls != 1 {
		t.Errorf("compensateCalls = %d, want 1 (A's Compensate should be re-invoked exactly once)", compensateCalls)
	}
	if results[0].Execution.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompensated)
	}
}

func TestRecover_CompensationFailurePersistedAcrossCrash(t *testing.T) {
	store := newRecoveryStore()
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error {
			return errors.New("refund gateway down") // still failing on resume
		})).
		AddStep(Step("B", noopAction, noopCompensate))

	// C already permanently failed to compensate before the crash.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusCompensating, Input: "input",
		Error: "boom",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusCompensationFailed, Output: "A-output", Error: "refund gateway down"},
			{Name: "B", Status: StepStatusFailed, Error: "boom"},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if !errors.Is(results[0].Err, ErrCompensationFailed) {
		t.Errorf("errors.Is(err, ErrCompensationFailed) = false, err = %v", results[0].Err)
	}
	if results[0].Execution.Status != StatusCompensationFailed {
		t.Errorf("Status = %q, want %q (a step already CompensationFailed before the crash must still count)", results[0].Execution.Status, StatusCompensationFailed)
	}
}

func TestRecover_SkipInFlightPolicy(t *testing.T) {
	store := newRecoveryStore()
	var called bool
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			called = true
			return data, nil
		}, noopCompensate))

	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusRunning},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d), WithRecoveryPolicy(RecoverSkipInFlight))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("len(results) = %d, want 1", len(results))
	}
	if !errors.Is(results[0].Err, ErrRecoverySkipped) {
		t.Errorf("errors.Is(err, ErrRecoverySkipped) = false, err = %v", results[0].Err)
	}
	if called {
		t.Error("Action must not run when RecoverSkipInFlight leaves an in-flight execution untouched")
	}

	// Persisted state must be genuinely untouched.
	persisted, getErr := store.Get(context.Background(), "exec-1")
	if getErr != nil {
		t.Fatalf("Get returned error: %v", getErr)
	}
	if persisted.Status != StatusRunning || persisted.Steps[0].Status != StepStatusRunning {
		t.Errorf("persisted state changed: Status=%q Steps[0].Status=%q", persisted.Status, persisted.Steps[0].Status)
	}
}

func TestRecover_SkipInFlightPolicy_StillResumesCleanExecutions(t *testing.T) {
	store := newRecoveryStore()
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	// Clean boundary: A succeeded, B never started — nothing in flight.
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "A-output"},
			{Name: "B", Status: StepStatusPending},
		},
		CreatedAt: time.Now(),
	})

	rm := NewRecoveryManager(store, WithSaga(d), WithRecoveryPolicy(RecoverSkipInFlight))
	results, err := rm.Recover(context.Background())
	if err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if results[0].Err != nil {
		t.Fatalf("expected a clean recovery, got error: %v", results[0].Err)
	}
	if results[0].Execution.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", results[0].Execution.Status, StatusCompleted)
	}
}
