package saga

import (
	"log/slog"
	"time"
)

// Option configures a Definition at construction time. Pass options to
// New.
type Option func(*Definition)

// WithStore configures the Store a Definition's executions are
// persisted to. If not given, executions are not persisted anywhere and
// Execute behaves exactly as it did before Store existed. Panics if
// store is nil.
func WithStore(store Store) Option {
	if store == nil {
		panic("saga: store must not be nil")
	}
	return func(d *Definition) { d.store = store }
}

// WithRetryPolicy configures the default RetryPolicy for every step in
// the saga that does not specify its own via WithStepRetryPolicy. If
// neither is set, a step gets exactly one attempt (NoRetry). Panics if
// policy is nil.
func WithRetryPolicy(policy RetryPolicy) Option {
	if policy == nil {
		panic("saga: retry policy must not be nil")
	}
	return func(d *Definition) { d.retryPolicy = policy }
}

// WithTimeout bounds the total time a single call to Execute may take,
// across every step, retry, and wait combined. If the saga has not
// reached a terminal status within timeout, Execute aborts — compensating
// already-succeeded steps, exactly like any other failure — with a
// *SagaTimeoutError. This is independent of WithStepTimeout, which
// bounds individual step attempts, not the run as a whole.
//
// timeout <= 0 means no saga-level timeout (the default): Execute waits
// as long as ctx and each step's own timeout allow.
func WithTimeout(timeout time.Duration) Option {
	return func(d *Definition) { d.timeout = timeout }
}

// WithLockTTL configures how long an execution lease is held before it
// must be renewed (see Locker). Only relevant when the configured Store
// also implements Locker; ignored otherwise. Defaults to DefaultLockTTL.
func WithLockTTL(ttl time.Duration) Option {
	return func(d *Definition) { d.lockTTL = ttl }
}

// WithEventPublisher configures where lifecycle Events are sent (see
// EventPublisher). If not given, no events are published — Execute and
// RecoveryManager.Recover behave exactly as they did before Event
// existed. Use MultiPublisher to send events to more than one
// destination. Panics if publisher is nil.
func WithEventPublisher(publisher EventPublisher) Option {
	if publisher == nil {
		panic("saga: event publisher must not be nil")
	}
	return func(d *Definition) { d.publisher = publisher }
}

// WithLogger configures structured logging of lifecycle events (the same
// moments that produce an Event; see EventPublisher) to logger. This
// takes a *slog.Logger directly, from the standard library's log/slog
// package, rather than a custom interface — any logging backend with an
// slog adapter (and most have one) works without this library needing to
// depend on it. Panics if logger is nil.
func WithLogger(logger *slog.Logger) Option {
	if logger == nil {
		panic("saga: logger must not be nil")
	}
	return func(d *Definition) { d.logger = logger }
}

// WithMetrics configures lifecycle metrics reporting (see Metrics). If
// not given, no metrics are recorded. Panics if metrics is nil.
func WithMetrics(metrics Metrics) Option {
	if metrics == nil {
		panic("saga: metrics must not be nil")
	}
	return func(d *Definition) { d.metrics = metrics }
}

// WithTracer configures distributed tracing of each Execute (or
// RecoveryManager.Recover resume) call as a single span (see Tracer). If
// not given, no spans are created. Panics if tracer is nil.
func WithTracer(tracer Tracer) Option {
	if tracer == nil {
		panic("saga: tracer must not be nil")
	}
	return func(d *Definition) { d.tracer = tracer }
}
