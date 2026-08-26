package saga

import (
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

// Definition is an ordered, named list of steps that together describe a
// saga. Build one with New and Definition.AddStep, then use it to create
// executions (introduced in a later phase).
//
// A Definition is meant to be built once, generally at program startup,
// and then reused to create many executions. AddStep panics on invalid
// input (an empty or duplicate step name, or a nil action) and on any
// call made after the Definition is frozen — these represent programmer
// mistakes in how the saga is defined, not runtime data errors, so they
// fail immediately and loudly rather than being silently swallowed. This
// mirrors how http.ServeMux.Handle panics on a duplicate pattern.
//
// AddStep is not safe for concurrent use, either with itself or with
// Freeze — building a Definition is expected to happen in a single
// goroutine. Freeze itself, however, is safe to call concurrently with
// other Freeze calls (Execute relies on this to freeze the Definition on
// first use, however many goroutines call Execute at once). Once frozen,
// a Definition is read-only and safe for unrestricted concurrent use.
type Definition struct {
	name        string
	steps       []StepDefinition
	stepSet     map[string]bool
	frozen      atomic.Bool
	store       Store
	retryPolicy RetryPolicy
	timeout     time.Duration
	lockTTL     time.Duration
	publisher   EventPublisher
	logger      *slog.Logger
	metrics     Metrics
	tracer      Tracer
}

// New creates a new, empty saga Definition with the given name and
// applies opts (see WithStore, WithRetryPolicy, WithTimeout, WithLockTTL,
// WithEventPublisher, WithLogger, WithMetrics, WithTracer). It panics if
// name is empty.
func New(name string, opts ...Option) *Definition {
	if strings.TrimSpace(name) == "" {
		panic("saga: saga name must not be empty")
	}
	d := &Definition{
		name:        name,
		stepSet:     make(map[string]bool),
		store:       noopStore{},
		retryPolicy: NoRetry(),
		lockTTL:     DefaultLockTTL,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Name returns the saga's name.
func (d *Definition) Name() string {
	return d.name
}

// AddStep appends step to the definition and returns the definition for
// chaining.
//
// It panics if:
//   - the Definition is frozen
//   - step.Name is empty
//   - step.Name duplicates a step already added to this Definition
//   - step.Action is nil
func (d *Definition) AddStep(step StepDefinition) *Definition {
	if d.frozen.Load() {
		panic(fmt.Sprintf("saga: cannot add step %q to frozen saga %q", step.Name, d.name))
	}
	name := strings.TrimSpace(step.Name)
	if name == "" {
		panic(fmt.Sprintf("saga: step name must not be empty (saga %q)", d.name))
	}
	if d.stepSet[step.Name] {
		panic(fmt.Sprintf("saga: duplicate step name %q in saga %q", step.Name, d.name))
	}
	if step.Action == nil {
		panic(fmt.Sprintf("saga: step %q must have a non-nil action (saga %q)", step.Name, d.name))
	}

	d.stepSet[step.Name] = true
	d.steps = append(d.steps, step)
	return d
}

// Steps returns a copy of the steps added so far, in the order they were
// added. Mutating the returned slice does not affect the Definition.
func (d *Definition) Steps() []StepDefinition {
	steps := make([]StepDefinition, len(d.steps))
	copy(steps, d.steps)
	return steps
}

// Freeze marks the Definition as complete. After Freeze, AddStep panics
// on any further call. Freeze is idempotent and safe to call
// concurrently with other Freeze calls.
func (d *Definition) Freeze() {
	d.frozen.Store(true)
}

// Frozen reports whether Freeze has been called.
func (d *Definition) Frozen() bool {
	return d.frozen.Load()
}
