package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func recordingAction(name string, order *[]string) ActionFunc {
	return func(ctx context.Context, data any) (any, error) {
		*order = append(*order, name)
		return data, nil
	}
}

func recordingCompensate(name string, order *[]string) CompensateFunc {
	return func(ctx context.Context, data any) error {
		*order = append(*order, name)
		return nil
	}
}

func TestExecute_RunsStepsInOrder(t *testing.T) {
	var order []string
	d := New("order_creation").
		AddStep(Step("A", recordingAction("A", &order), noopCompensate)).
		AddStep(Step("B", recordingAction("B", &order), noopCompensate)).
		AddStep(Step("C", recordingAction("C", &order), noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	want := []string{"A", "B", "C"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i, name := range want {
		if order[i] != name {
			t.Errorf("order[%d] = %q, want %q", i, order[i], name)
		}
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

func TestExecute_GeneratesExecutionID(t *testing.T) {
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))

	exec1, err := d.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	exec2, err := d.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if exec1.ID == "" {
		t.Error("exec1.ID should not be empty")
	}
	if exec2.ID == "" {
		t.Error("exec2.ID should not be empty")
	}
	if exec1.ID == exec2.ID {
		t.Errorf("two executions got the same ID: %q", exec1.ID)
	}
}

func TestExecute_SuccessfulRunBecomesCompleted(t *testing.T) {
	d := New("order_creation").
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
	if exec.Error != "" {
		t.Errorf("Error = %q, want empty", exec.Error)
	}
	if exec.CurrentStep != "" {
		t.Errorf("CurrentStep = %q, want empty once completed", exec.CurrentStep)
	}
	for _, s := range exec.Steps {
		if s.Status != StepStatusSucceeded {
			t.Errorf("step %q Status = %q, want %q", s.Name, s.Status, StepStatusSucceeded)
		}
		if s.Attempts != 1 {
			t.Errorf("step %q Attempts = %d, want 1", s.Name, s.Attempts)
		}
	}
}

func TestExecute_DataFlowsThroughSteps(t *testing.T) {
	appendStep := func(suffix string) ActionFunc {
		return func(ctx context.Context, data any) (any, error) {
			return data.(string) + suffix, nil
		}
	}
	d := New("order_creation").
		AddStep(Step("A", appendStep("-A"), noopCompensate)).
		AddStep(Step("B", appendStep("-B"), noopCompensate)).
		AddStep(Step("C", appendStep("-C"), noopCompensate))

	exec, err := d.Execute(context.Background(), "start")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	want := "start-A-B-C"
	if exec.Output != want {
		t.Errorf("Output = %v, want %v", exec.Output, want)
	}
	if exec.Input != "start" {
		t.Errorf("Input = %v, want %v", exec.Input, "start")
	}
}

func TestExecute_StepFailureStopsExecution(t *testing.T) {
	// A succeeded, so its Compensate now runs as part of aborting —
	// see TestExecute_CompensatesInReverseOrder for order/skip coverage.
	// This test just confirms C (after the failed step) never runs.
	var order []string
	failingErr := errors.New("insufficient stock")
	d := New("order_creation").
		AddStep(Step("A", recordingAction("A", &order), noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			order = append(order, "B")
			return nil, failingErr
		}, noopCompensate)).
		AddStep(Step("C", recordingAction("C", &order), noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, failingErr) {
		t.Errorf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	var stepErr *StepError
	if !errors.As(err, &stepErr) || stepErr.Step != "B" {
		t.Errorf("expected *StepError for step B, got %v", err)
	}

	if want := []string{"A", "B"}; len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Errorf("order = %v, want %v (C must not run)", order, want)
	}
	// A succeeded, so compensation runs and the execution ends rolled
	// back, not bare-failed.
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
	if exec.Steps[0].Status != StepStatusCompensated {
		t.Errorf("step A Status = %q, want %q", exec.Steps[0].Status, StepStatusCompensated)
	}
	if exec.Steps[1].Status != StepStatusFailed {
		t.Errorf("step B Status = %q, want %q", exec.Steps[1].Status, StepStatusFailed)
	}
	if exec.Steps[2].Status != StepStatusPending {
		t.Errorf("step C Status = %q, want %q (must never have started)", exec.Steps[2].Status, StepStatusPending)
	}
}

func TestExecute_CancelledBeforeStart(t *testing.T) {
	var order []string
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	d := New("order_creation").AddStep(Step("A", recordingAction("A", &order), noopCompensate))

	exec, err := d.Execute(ctx, "input")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false, err = %v", err)
	}
	if len(order) != 0 {
		t.Errorf("no step should have run, order = %v", order)
	}
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusFailed)
	}
}

func TestExecute_CancelledMidRun(t *testing.T) {
	var order []string
	ctx, cancel := context.WithCancel(context.Background())

	d := New("order_creation").
		AddStep(Step("A", recordingAction("A", &order), noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			order = append(order, "B")
			cancel() // cancel partway through, before step C would run
			return data, nil
		}, noopCompensate)).
		AddStep(Step("C", recordingAction("C", &order), noopCompensate))

	exec, err := d.Execute(ctx, "input")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false, err = %v", err)
	}
	if want := []string{"A", "B"}; len(order) != len(want) {
		t.Errorf("order = %v, want %v (C must not run)", order, want)
	}
	if exec.Steps[2].Status != StepStatusPending {
		t.Errorf("step C Status = %q, want %q", exec.Steps[2].Status, StepStatusPending)
	}
	// A and B both succeeded before cancellation was noticed, so
	// compensation still runs for both, even though ctx is cancelled.
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
	if exec.Steps[0].Status != StepStatusCompensated || exec.Steps[1].Status != StepStatusCompensated {
		t.Errorf("A/B should be Compensated: A=%q B=%q", exec.Steps[0].Status, exec.Steps[1].Status)
	}
}

func TestExecute_EmptyDefinitionCompletesImmediately(t *testing.T) {
	d := New("noop_saga")

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
	if exec.Output != "input" {
		t.Errorf("Output = %v, want unchanged input %v", exec.Output, "input")
	}
}

func TestExecute_FreezesDefinition(t *testing.T) {
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	if d.Frozen() {
		t.Fatal("Definition should not be frozen before Execute")
	}

	if _, err := d.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !d.Frozen() {
		t.Error("Execute should freeze the Definition")
	}
}

func TestExecute_CompensatesInReverseOrder(t *testing.T) {
	var actionOrder, compensateOrder []string
	failingErr := errors.New("insufficient stock")

	d := New("order_creation").
		AddStep(Step("A", recordingAction("A", &actionOrder), recordingCompensate("A", &compensateOrder))).
		AddStep(Step("B", recordingAction("B", &actionOrder), recordingCompensate("B", &compensateOrder))).
		AddStep(Step("C", func(ctx context.Context, data any) (any, error) {
			actionOrder = append(actionOrder, "C")
			return nil, failingErr
		}, recordingCompensate("C", &compensateOrder)))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	want := []string{"B", "A"} // reverse order; C must never appear
	if len(compensateOrder) != len(want) {
		t.Fatalf("compensateOrder = %v, want %v", compensateOrder, want)
	}
	for i, name := range want {
		if compensateOrder[i] != name {
			t.Errorf("compensateOrder[%d] = %q, want %q", i, compensateOrder[i], name)
		}
	}
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
	if exec.Steps[0].Status != StepStatusCompensated || exec.Steps[1].Status != StepStatusCompensated {
		t.Errorf("A/B should be Compensated: A=%q B=%q", exec.Steps[0].Status, exec.Steps[1].Status)
	}
	if exec.Steps[2].Status != StepStatusFailed {
		t.Errorf("C Status = %q, want %q", exec.Steps[2].Status, StepStatusFailed)
	}
}

func TestExecute_OriginalErrorPreservedAfterCompensation(t *testing.T) {
	failingErr := errors.New("insufficient stock")
	d := New("order_creation").
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	var stepErr *StepError
	if !errors.As(err, &stepErr) || stepErr.Step != "B" {
		t.Fatalf("expected *StepError for step B, got %v", err)
	}
	if exec.Error != stepErr.Error() {
		t.Errorf("exec.Error = %q, want %q", exec.Error, stepErr.Error())
	}
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
}

func TestExecute_CompensationFailure(t *testing.T) {
	failingErr := errors.New("insufficient stock")
	compErrB := errors.New("refund gateway down")

	d := New("order_creation").
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, func(ctx context.Context, data any) error { return compErrB })).
		AddStep(Step("C", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")

	if !errors.Is(err, ErrCompensationFailed) {
		t.Errorf("errors.Is(err, ErrCompensationFailed) = false, err = %v", err)
	}
	if !errors.Is(err, failingErr) {
		t.Errorf("original error not preserved: errors.Is(err, failingErr) = false, err = %v", err)
	}
	if exec.Status != StatusCompensationFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensationFailed)
	}
	if exec.Steps[1].Status != StepStatusCompensationFailed {
		t.Errorf("step B Status = %q, want %q", exec.Steps[1].Status, StepStatusCompensationFailed)
	}
	if exec.Steps[1].Error != compErrB.Error() {
		t.Errorf("step B Error = %q, want %q", exec.Steps[1].Error, compErrB.Error())
	}
	// A's compensation must still be attempted even though B's compensation failed.
	if exec.Steps[0].Status != StepStatusCompensated {
		t.Errorf("step A Status = %q, want %q (compensation must continue past a failure)", exec.Steps[0].Status, StepStatusCompensated)
	}
}

func TestExecute_MultipleCompensationFailures(t *testing.T) {
	failingErr := errors.New("boom")
	compErrA := errors.New("compensate A failed")
	compErrB := errors.New("compensate B failed")

	d := New("order_creation").
		AddStep(Step("A", noopAction, func(ctx context.Context, data any) error { return compErrA })).
		AddStep(Step("B", noopAction, func(ctx context.Context, data any) error { return compErrB })).
		AddStep(Step("C", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")

	if !errors.Is(err, ErrCompensationFailed) {
		t.Errorf("errors.Is(err, ErrCompensationFailed) = false, err = %v", err)
	}
	if exec.Status != StatusCompensationFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensationFailed)
	}
	if exec.Steps[0].Status != StepStatusCompensationFailed {
		t.Errorf("step A Status = %q, want %q", exec.Steps[0].Status, StepStatusCompensationFailed)
	}
	if exec.Steps[0].Error != compErrA.Error() {
		t.Errorf("step A Error = %q, want %q", exec.Steps[0].Error, compErrA.Error())
	}
	if exec.Steps[1].Status != StepStatusCompensationFailed {
		t.Errorf("step B Status = %q, want %q", exec.Steps[1].Status, StepStatusCompensationFailed)
	}
	if exec.Steps[1].Error != compErrB.Error() {
		t.Errorf("step B Error = %q, want %q", exec.Steps[1].Error, compErrB.Error())
	}
}

func TestExecute_NilCompensateSkipped(t *testing.T) {
	failingErr := errors.New("boom")
	d := New("order_creation").
		AddStep(Step("A", noopAction, nil)). // nothing to undo
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	// Nothing to undo for A: it stays Succeeded rather than being
	// marked Compensated, since no compensating action ever ran.
	if exec.Steps[0].Status != StepStatusSucceeded {
		t.Errorf("step A Status = %q, want %q", exec.Steps[0].Status, StepStatusSucceeded)
	}
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
}

func TestExecute_FirstStepFailsSkipsCompensation(t *testing.T) {
	var compensateOrder []string
	failingErr := errors.New("boom")
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, recordingCompensate("A", &compensateOrder)))

	exec, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	if len(compensateOrder) != 0 {
		t.Errorf("no compensation should run when no step succeeded, got %v", compensateOrder)
	}
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusFailed)
	}
}

func TestExecute_CompensateReceivesStepOutput(t *testing.T) {
	var gotData any
	failingErr := errors.New("boom")
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			return "A-output", nil
		}, func(ctx context.Context, data any) error {
			gotData = data
			return nil
		})).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	_, err := d.Execute(context.Background(), "input")
	if !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}
	if gotData != "A-output" {
		t.Errorf("compensate received %v, want %q", gotData, "A-output")
	}
}

// TestExecute_SafeForConcurrentUse runs many Executes of the same
// Definition in parallel. It exists to back up the doc comment's claim
// that Execute is safe to call concurrently with itself; run with
// -race to be meaningful.
func TestExecute_SafeForConcurrentUse(t *testing.T) {
	d := New("order_creation").
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	const n = 50
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			exec, err := d.Execute(context.Background(), i)
			if err != nil {
				t.Errorf("Execute returned error: %v", err)
				return
			}
			ids[i] = exec.ID
		}(i)
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for _, id := range ids {
		if id == "" {
			t.Fatal("an execution finished with an empty ID")
		}
		if seen[id] {
			t.Fatalf("two concurrent executions got the same ID: %q", id)
		}
		seen[id] = true
	}
}
