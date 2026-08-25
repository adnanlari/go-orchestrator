package saga

import "time"

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
