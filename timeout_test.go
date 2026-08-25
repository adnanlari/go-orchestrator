package saga

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// --- Saga-level timeout ---

func TestExecute_SagaTimeoutExceeded(t *testing.T) {
	// Action ignores ctx and blocks past the timeout on purpose, to prove
	// cooperative cancellation: Execute cannot return until Action does,
	// but once it does, the outcome must still reflect the timeout, not
	// whatever Action itself returns.
	block := make(chan struct{})
	d := New("order_creation", WithTimeout(20*time.Millisecond)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			<-block
			return data, nil
		}, noopCompensate))

	type result struct {
		exec *Execution
		err  error
	}
	done := make(chan result, 1)
	go func() {
		exec, err := d.Execute(context.Background(), "input")
		done <- result{exec, err}
	}()

	// Give the saga timeout time to fire internally before releasing the
	// blocked Action.
	time.Sleep(60 * time.Millisecond)
	close(block)

	var res result
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return after Action was unblocked")
	}

	var target *SagaTimeoutError
	if !errors.As(res.err, &target) {
		t.Fatalf("expected *SagaTimeoutError, got %T: %v", res.err, res.err)
	}
	if target.Saga != "order_creation" {
		t.Errorf("SagaTimeoutError.Saga = %q, want %q", target.Saga, "order_creation")
	}
	if !errors.Is(res.err, context.DeadlineExceeded) {
		t.Error("errors.Is(err, context.DeadlineExceeded) = false, want true")
	}
	if res.exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", res.exec.Status, StatusFailed)
	}
}

func TestExecute_SagaTimeoutRespectingAction(t *testing.T) {
	// A well-behaved Action that checks ctx returns promptly.
	action := func(ctx context.Context, data any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	d := New("order_creation", WithTimeout(20*time.Millisecond)).
		AddStep(Step("A", action, noopCompensate))

	done := make(chan struct{})
	var err error
	go func() {
		_, err = d.Execute(context.Background(), "input")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Execute did not return promptly for a well-behaved Action")
	}

	var target *SagaTimeoutError
	if !errors.As(err, &target) {
		t.Fatalf("expected *SagaTimeoutError, got %T: %v", err, err)
	}
}

func TestExecute_SagaTimeoutTriggersCompensation(t *testing.T) {
	var compensated []string
	d := New("order_creation", WithTimeout(20*time.Millisecond)).
		AddStep(Step("A", noopAction, recordingCompensate("A", &compensated))).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}, noopCompensate))

	exec, err := d.Execute(context.Background(), "input")

	var target *SagaTimeoutError
	if !errors.As(err, &target) {
		t.Fatalf("expected *SagaTimeoutError, got %T: %v", err, err)
	}
	if exec.Status != StatusCompensated {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompensated)
	}
	if len(compensated) != 1 || compensated[0] != "A" {
		t.Errorf("compensated = %v, want [A]", compensated)
	}
}

func TestExecute_NoSagaTimeoutByDefault(t *testing.T) {
	// No WithTimeout: a slow step should not be treated as any kind of
	// timeout failure.
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			time.Sleep(30 * time.Millisecond)
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

// --- Step-level timeout ---

func TestExecute_StepTimeoutExceeded(t *testing.T) {
	action := func(ctx context.Context, data any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	d := New("order_creation").
		AddStep(Step("A", action, noopCompensate, WithStepTimeout(20*time.Millisecond)))

	exec, err := d.Execute(context.Background(), "input")

	var target *StepTimeoutError
	if !errors.As(err, &target) {
		t.Fatalf("expected *StepTimeoutError, got %T: %v", err, err)
	}
	if target.Step != "A" || target.Timeout != 20*time.Millisecond {
		t.Errorf("StepTimeoutError = %+v, unexpected fields", target)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Error("errors.Is(err, context.DeadlineExceeded) = false, want true")
	}
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q", exec.Status, StatusFailed)
	}
}

func TestExecute_StepTimeoutDiscardsLateSuccess(t *testing.T) {
	// Action ignores ctx entirely and "succeeds" well after its
	// configured timeout has elapsed. The engine must not trust that
	// late result.
	action := func(ctx context.Context, data any) (any, error) {
		time.Sleep(60 * time.Millisecond)
		return "too-late", nil
	}
	d := New("order_creation").
		AddStep(Step("A", action, noopCompensate, WithStepTimeout(10*time.Millisecond)))

	exec, err := d.Execute(context.Background(), "input")

	var target *StepTimeoutError
	if !errors.As(err, &target) {
		t.Fatalf("expected *StepTimeoutError, got %T: %v", err, err)
	}
	if exec.Status != StatusFailed {
		t.Errorf("Status = %q, want %q (a late success must not be trusted)", exec.Status, StatusFailed)
	}
}

func TestExecute_StepTimeoutIsRetried(t *testing.T) {
	var attempts int
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		if attempts < 3 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return "ok", nil // 3rd attempt finishes before its own timeout
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 5))).
		AddStep(Step("A", action, noopCompensate, WithStepTimeout(20*time.Millisecond)))

	exec, err := d.Execute(context.Background(), "input")
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if exec.Status != StatusCompleted {
		t.Errorf("Status = %q, want %q", exec.Status, StatusCompleted)
	}
}

func TestExecute_StepTimeoutExhaustsRetries(t *testing.T) {
	var attempts int
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		<-ctx.Done()
		return nil, ctx.Err()
	}
	d := New("order_creation", WithRetryPolicy(FixedDelay(time.Millisecond, 3))).
		AddStep(Step("A", action, noopCompensate, WithStepTimeout(10*time.Millisecond)))

	_, err := d.Execute(context.Background(), "input")

	var target *StepTimeoutError
	if !errors.As(err, &target) {
		t.Fatalf("expected *StepTimeoutError, got %T: %v", err, err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3 (MaxAttempts)", attempts)
	}
}

func TestExecute_SagaTimeoutTakesPrecedenceOverStepRetry(t *testing.T) {
	var attempts int
	action := func(ctx context.Context, data any) (any, error) {
		attempts++
		<-ctx.Done()
		return nil, ctx.Err()
	}
	// Saga timeout (30ms) is shorter than what unlimited step retries
	// would otherwise take (FixedDelay would keep retrying past it).
	d := New("order_creation",
		WithTimeout(30*time.Millisecond),
		WithRetryPolicy(FixedDelay(5*time.Millisecond, 100)),
	).AddStep(Step("A", action, noopCompensate, WithStepTimeout(10*time.Millisecond)))

	_, err := d.Execute(context.Background(), "input")

	// Once the saga-level timeout fires, it must win over the step's own
	// (still retryable) timeout — errors.As should find SagaTimeoutError,
	// not StepTimeoutError, once retries are cut short.
	var sagaTimeout *SagaTimeoutError
	if !errors.As(err, &sagaTimeout) {
		t.Fatalf("expected *SagaTimeoutError to win, got %T: %v", err, err)
	}
	if attempts >= 100 {
		t.Errorf("attempts = %d, retries should have been cut short by the saga timeout", attempts)
	}
}

func TestExecute_NoStepTimeoutByDefault(t *testing.T) {
	d := New("order_creation").
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
			time.Sleep(30 * time.Millisecond)
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

// --- Explicit cancellation state handling ---

func TestExecute_ExternalCancellationDistinctFromTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	d := New("order_creation", WithTimeout(time.Hour)). // long saga timeout: not what's firing
								AddStep(Step("A", noopAction, noopCompensate))

	_, err := d.Execute(ctx, "input")

	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false, err = %v", err)
	}
	var sagaTimeout *SagaTimeoutError
	if errors.As(err, &sagaTimeout) {
		t.Error("an explicit external cancellation must not be reported as a SagaTimeoutError")
	}
}

func TestExecute_SagaTimeoutDistinctFromExternalCancellation(t *testing.T) {
	action := func(ctx context.Context, data any) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	d := New("order_creation", WithTimeout(20*time.Millisecond)).
		AddStep(Step("A", action, noopCompensate))

	_, err := d.Execute(context.Background(), "input") // caller never cancels

	if errors.Is(err, context.Canceled) {
		t.Error("a saga timeout must not report errors.Is(err, context.Canceled) as true")
	}
	var sagaTimeout *SagaTimeoutError
	if !errors.As(err, &sagaTimeout) {
		t.Fatalf("expected *SagaTimeoutError, got %T: %v", err, err)
	}
}

// --- No leaked goroutines ---

func TestExecute_NoLeakedGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var d *Definition
			switch i % 4 {
			case 0: // saga timeout fires
				d = New("s", WithTimeout(5*time.Millisecond)).
					AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}, noopCompensate))
			case 1: // step timeout fires, with retries
				d = New("s", WithRetryPolicy(FixedDelay(time.Millisecond, 3))).
					AddStep(Step("A", func(ctx context.Context, data any) (any, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}, noopCompensate, WithStepTimeout(5*time.Millisecond)))
			case 2: // plain success, no timeouts configured
				d = New("s").AddStep(Step("A", noopAction, noopCompensate))
			case 3: // external cancellation mid-run
				d = New("s").
					AddStep(Step("A", noopAction, noopCompensate)).
					AddStep(Step("B", func(ctx context.Context, data any) (any, error) {
						return data, nil
					}, noopCompensate))
			}
			ctx := context.Background()
			if i%4 == 3 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()
			}
			_, _ = d.Execute(ctx, "input")
		}(i)
	}
	wg.Wait()

	// Timers/contexts clean up asynchronously in places; give them a
	// moment, polling rather than a single fixed sleep.
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		after := runtime.NumGoroutine()
		if after <= before+2 { // small slack for test/runtime bookkeeping goroutines
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("goroutine count grew from %d to %d and did not settle", before, after)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
