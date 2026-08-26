package saga

import (
	"context"
	"fmt"
	"time"
)

// DefaultLockTTL is the lease duration used when a Store implements
// Locker and no WithLockTTL was configured.
const DefaultLockTTL = 30 * time.Second

// Locker is implemented by a Store that can arbitrate exclusive access
// to an execution, so two workers — for example, two processes each
// running a RecoveryManager against the same Store, or one process's
// live Execute racing a second process's crash recovery of the same
// execution — cannot drive the same execution concurrently.
//
// It is lease-based, not a plain mutex: a lease is held for at most ttl
// and must be renewed (by calling Acquire again as the same owner)
// before it expires to keep holding it. This is deliberate — if the
// process holding a lease crashes without ever calling Release, a plain
// non-expiring lock would leave that execution permanently stuck, which
// is exactly the crash scenario this whole library exists to survive.
// The engine renews its lease automatically every time it persists a
// state change, so an execution that's genuinely still progressing does
// not lose its lease out from under it.
//
// Locker is a separate interface from Store, exactly like Lister, so a
// Store that doesn't need to support concurrent workers isn't forced to
// implement it.
type Locker interface {
	// Acquire attempts to take (or, if owner already holds it, renew)
	// an exclusive lease on the execution identified by id, for ttl
	// from now. It returns (true, nil) if the lease is now held by
	// owner, (false, nil) if it's currently held by a different owner
	// and has not expired, or a non-nil error if the attempt itself
	// failed (a store I/O error, not a lock conflict).
	Acquire(ctx context.Context, id string, owner string, ttl time.Duration) (bool, error)
	// Release gives up owner's lease on id, if it currently holds one.
	// Releasing a lease a different owner holds, or one that no longer
	// exists (e.g. it already expired), is a no-op, not an error.
	Release(ctx context.Context, id string, owner string) error
}

// ExecutionLockedError indicates that another worker currently holds the
// execution lock for this execution (see Locker), so this call did not
// attempt to drive it forward. It is only ever returned when the
// configured Store implements Locker.
type ExecutionLockedError struct {
	ExecutionID string
}

// Error implements the error interface.
func (e *ExecutionLockedError) Error() string {
	return fmt.Sprintf("saga: execution %q is locked by another worker", e.ExecutionID)
}
