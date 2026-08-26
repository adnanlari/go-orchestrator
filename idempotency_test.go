package saga

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOperationID_EmptyOutsideStepInvocation(t *testing.T) {
	if got := OperationID(context.Background()); got != "" {
		t.Errorf("OperationID(context.Background()) = %q, want empty", got)
	}
}

func TestOperationID_Format(t *testing.T) {
	var got string
	d := New("order_creation").AddStep(Step("reserve_inventory", func(ctx context.Context, data any) (any, error) {
		got = OperationID(ctx)
		return data, nil
	}, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	want := exec.ID + "/reserve_inventory"
	if got != want {
		t.Errorf("OperationID = %q, want %q", got, want)
	}
}

func TestOperationID_StableAcrossRetries(t *testing.T) {
	var seen []string
	action := func(ctx context.Context, data any) (any, error) {
		seen = append(seen, OperationID(ctx))
		if len(seen) < 3 {
			return nil, errors.New("transient")
		}
		return data, nil
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 5))).
		AddStep(Step("A", action, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("len(seen) = %d, want 3", len(seen))
	}
	for i, id := range seen {
		if id == "" {
			t.Errorf("seen[%d] is empty", i)
		}
		if id != seen[0] {
			t.Errorf("seen[%d] = %q, want %q (same as attempt 1)", i, id, seen[0])
		}
	}
}

func TestOperationID_StableAcrossRecovery(t *testing.T) {
	store := newRecoveryStore()
	var seen []string
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			seen = append(seen, OperationID(ctx))
			return data, nil
		}, noopCompensate))

	// Simulate a crash while B's Action was in flight (its own outcome
	// was never known/persisted).
	seed(t, store, &Execution{
		ID: "exec-1", SagaName: "order_creation", Status: StatusRunning, Input: "input",
		Steps: []StepExecution{
			{Name: "A", Status: StepStatusSucceeded, Output: "A-output"},
			{Name: "B", Status: StepStatusRunning},
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
	if len(seen) != 1 {
		t.Fatalf("len(seen) = %d, want 1", len(seen))
	}
	want := "exec-1/B"
	if seen[0] != want {
		t.Errorf("OperationID on recovery = %q, want %q (same as it would have been pre-crash)", seen[0], want)
	}
}

func TestOperationID_DiffersBetweenSteps(t *testing.T) {
	var idA, idB string
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			idA = OperationID(ctx)
			return data, nil
		}, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			idB = OperationID(ctx)
			return data, nil
		}, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if idA == "" || idB == "" {
		t.Fatal("both operation IDs should be non-empty")
	}
	if idA == idB {
		t.Errorf("idA and idB should differ, both = %q", idA)
	}
}

func TestOperationID_DiffersBetweenExecutions(t *testing.T) {
	var ids []string
	d := New("order_creation").AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
		ids = append(ids, OperationID(ctx))
		return data, nil
	}, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("len(ids) = %d, want 2", len(ids))
	}
	if ids[0] == ids[1] {
		t.Errorf("two different executions got the same operation ID: %q", ids[0])
	}
}

func TestOperationID_DistinctForCompensate(t *testing.T) {
	var actionID, compensateID string
	failingErr := errors.New("boom")
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			actionID = OperationID(ctx)
			return data, nil
		}, func(ctx context.Context, data any) error {
			compensateID = OperationID(ctx)
			return nil
		})).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	if actionID == "" || compensateID == "" {
		t.Fatal("both IDs should be non-empty")
	}
	if actionID == compensateID {
		t.Errorf("Action and Compensate got the same operation ID: %q", actionID)
	}
}

func TestOperationID_CompensateStableAcrossRecovery(t *testing.T) {
	store := newRecoveryStore()
	var seen []string
	d := New("order_creation", WithStore(store)).
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error {
			seen = append(seen, OperationID(ctx))
			return nil
		})).
		AddStep(Step("B", noopAction, noopCompensate))

	// Simulate a crash while A's Compensate was in flight.
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
	if _, err := rm.Recover(context.Background()); err != nil {
		t.Fatalf("Recover returned error: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("len(seen) = %d, want 1", len(seen))
	}
	want := "exec-1/A/compensate"
	if seen[0] != want {
		t.Errorf("OperationID on recovered compensation = %q, want %q", seen[0], want)
	}
}
