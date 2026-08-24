package saga

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
