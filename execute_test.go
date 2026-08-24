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
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusFailed)
	}
	if exec.Steps[0].Status != StepStatusSucceeded {
		t.Errorf("step A Status = %q, want %q", exec.Steps[0].Status, StepStatusSucceeded)
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
