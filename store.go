package saga

import "context"

// Store persists Execution records so their state survives beyond a
// single call to Execute. The engine calls Save every time an
// execution's Status or a step's StepStatus changes, and treats a Save
// failure as fatal to that call to Execute — persistence failing
// silently would contradict "durable execution state," so Execute
// returns the error immediately instead of continuing as if nothing were
// wrong.
//
// Implementations must not retain the exact *Execution pointer passed to
// Save, since the engine goes on mutating it afterward; call
// Execution.Clone to take an independent copy before storing it. The
// same applies in reverse for Get: return a Clone, not a live reference
// into internal storage, so a caller mutating the result cannot corrupt
// what is stored.
type Store interface {
	// Save persists exec, creating it if this is the first time
	// exec.ID has been seen, or overwriting the previous record for
	// exec.ID otherwise.
	Save(ctx context.Context, exec *Execution) error
	// Get returns the persisted execution with the given ID. If no such
	// execution exists, it returns an error satisfying
	// errors.Is(err, ErrExecutionNotFound).
	Get(ctx context.Context, id string) (*Execution, error)
}

// Lister is implemented by a Store that can enumerate its own
// executions, which crash recovery needs in order to find work after a
// restart — a plain Store can only be asked about one already-known ID
// at a time. It is a separate interface from Store, rather than folded
// into it, so a Store that genuinely cannot list efficiently (a
// write-mostly audit sink, for example) isn't forced to implement it;
// RecoveryManager simply requires whatever Store it's given to also
// satisfy Lister.
type Lister interface {
	// ListIncomplete returns every persisted execution whose Status is
	// not terminal (see Status.IsTerminal), in no particular order —
	// RecoveryManager sorts the results itself before acting on them.
	ListIncomplete(ctx context.Context) ([]*Execution, error)
}

// noopStore is the Store used when a Definition is not configured
// WithStore. It discards everything, so a Definition behaves exactly as
// it did before Store existed unless a caller opts in.
type noopStore struct{}

// Save implements Store by discarding exec.
func (noopStore) Save(ctx context.Context, exec *Execution) error {
	return nil
}

// Get implements Store by reporting that nothing has ever been saved.
func (noopStore) Get(ctx context.Context, id string) (*Execution, error) {
	return nil, ErrExecutionNotFound
}
