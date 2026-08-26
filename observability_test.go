package saga

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// --- Logger (log/slog) ---

func TestWithLogger_EmitsStructuredLines(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	d := New("order_creation", WithLogger(logger)).
		AddStep(Step("reserve_inventory", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	out := buf.String()
	for _, want := range []string{"SAGA_STARTED", "STEP_STARTED", "STEP_COMPLETED", "SAGA_COMPLETED", "reserve_inventory", "order_creation"} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q; full output:\n%s", want, out)
		}
	}
}

func TestWithLogger_FailureLogsAtErrorLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	failingErr := errors.New("insufficient stock")

	d := New("order_creation", WithLogger(logger)).
		AddStep(Step("A", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"level":"ERROR"`) {
		t.Errorf("expected an ERROR-level log line; full output:\n%s", out)
	}
	if !strings.Contains(out, "insufficient stock") {
		t.Errorf("expected the error message in the log output; full output:\n%s", out)
	}
}

func TestWithLogger_NilPanics(t *testing.T) {
	assertPanics(t, "logger must not be nil", func() {
		WithLogger(nil)
	})
}

func TestExecute_NoLoggerConfigured_NoOp(t *testing.T) {
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}

// --- Metrics ---

type recordedCounter struct {
	name   string
	labels []string
}

type recordedDuration struct {
	name    string
	seconds float64
	labels  []string
}

type recordingMetrics struct {
	mu        sync.Mutex
	counters  []recordedCounter
	durations []recordedDuration
}

func (m *recordingMetrics) IncCounter(name string, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters = append(m.counters, recordedCounter{name: name, labels: append([]string(nil), labels...)})
}

func (m *recordingMetrics) ObserveDuration(name string, seconds float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durations = append(m.durations, recordedDuration{name: name, seconds: seconds, labels: append([]string(nil), labels...)})
}

func TestWithMetrics_IncrementsCounterPerEvent(t *testing.T) {
	metrics := &recordingMetrics{}
	d := New("order_creation", WithMetrics(metrics)).
		AddStep(Step("A", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	// SagaStarted, StepStarted, StepCompleted, SagaCompleted = 4 events.
	if len(metrics.counters) != 4 {
		t.Fatalf("len(counters) = %d, want 4: %+v", len(metrics.counters), metrics.counters)
	}
	for _, c := range metrics.counters {
		if c.name != "saga_events_total" {
			t.Errorf("counter name = %q, want %q", c.name, "saga_events_total")
		}
	}
}

func TestWithMetrics_ObservesSagaDuration(t *testing.T) {
	metrics := &recordingMetrics{}
	d := New("order_creation", WithMetrics(metrics)).
		AddStep(Step("A", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if len(metrics.durations) != 1 {
		t.Fatalf("len(durations) = %d, want 1: %+v", len(metrics.durations), metrics.durations)
	}
	if metrics.durations[0].name != "saga_duration_seconds" {
		t.Errorf("duration name = %q, want %q", metrics.durations[0].name, "saga_duration_seconds")
	}
	if metrics.durations[0].seconds < 0 {
		t.Errorf("duration seconds = %v, want >= 0", metrics.durations[0].seconds)
	}
}

func TestWithMetrics_NilPanics(t *testing.T) {
	assertPanics(t, "metrics must not be nil", func() {
		WithMetrics(nil)
	})
}

func TestExecute_NoMetricsConfigured_NoOp(t *testing.T) {
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}

// --- Tracer ---

type recordedSpan struct {
	name  string
	ended bool
}

type recordingTracer struct {
	mu    sync.Mutex
	spans []*recordedSpan
}

func (tr *recordingTracer) StartSpan(ctx context.Context, name string) (context.Context, func()) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	span := &recordedSpan{name: name}
	tr.spans = append(tr.spans, span)
	return ctx, func() {
		tr.mu.Lock()
		defer tr.mu.Unlock()
		span.ended = true
	}
}

func (tr *recordingTracer) names() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	names := make([]string, len(tr.spans))
	for i, s := range tr.spans {
		names[i] = s.name
	}
	return names
}

func TestWithTracer_CreatesSagaAndStepSpans(t *testing.T) {
	tracer := &recordingTracer{}
	d := New("order_creation", WithTracer(tracer)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	names := tracer.names()
	want := []string{"saga:order_creation", "step:A", "step:B"}
	if len(names) != len(want) {
		t.Fatalf("span names = %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("names[%d] = %q, want %q", i, names[i], w)
		}
	}
}

func TestWithTracer_AllSpansEnded(t *testing.T) {
	tracer := &recordingTracer{}
	d := New("order_creation", WithTracer(tracer)).
		AddStep(Step("A", noopAction, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	for _, s := range tracer.spans {
		if !s.ended {
			t.Errorf("span %q was never ended", s.name)
		}
	}
}

func TestWithTracer_CompensationGetsItsOwnSpan(t *testing.T) {
	tracer := &recordingTracer{}
	failingErr := errors.New("boom")
	d := New("order_creation", WithTracer(tracer)).
		AddStep(Step("A", noopAction, noopCompensate)).
		AddStep(Step("B", func(ctx context.Context, data any) (any, error) { return nil, failingErr }, noopCompensate))

	if _, err := d.Execute(context.Background(), "input"); !errors.Is(err, failingErr) {
		t.Fatalf("errors.Is(err, failingErr) = false, err = %v", err)
	}

	names := tracer.names()
	want := []string{"saga:order_creation", "step:A", "step:B", "step:A:compensate"}
	if len(names) != len(want) {
		t.Fatalf("span names = %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("names[%d] = %q, want %q", i, names[i], w)
		}
	}
}

func TestWithTracer_NilPanics(t *testing.T) {
	assertPanics(t, "tracer must not be nil", func() {
		WithTracer(nil)
	})
}

func TestExecute_NoTracerConfigured_NoOp(t *testing.T) {
	d := New("order_creation").AddStep(Step("A", noopAction, noopCompensate))
	if _, err := d.Execute(context.Background(), "input"); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}
