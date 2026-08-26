// Package memory provides an in-memory implementation of saga.Store. It
// requires no external server or database, matching the core library's
// goal of working standalone, but data does not survive process
// restart — it exists to give the engine somewhere to persist to during
// development and testing, and as a reference implementation of the
// Store interface that a durable backend (Postgres, Redis, ...) can be
// modeled after.
package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	saga "github.com/adnanlari/go-orchestrator"
)

// Store is an in-memory, concurrency-safe saga.Store. It also implements
// saga.Locker and saga.Lister.
type Store struct {
	mu     sync.RWMutex
	data   map[string]*saga.Execution
	leases map[string]lease
}

type lease struct {
	owner   string
	expires time.Time
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{
		data:   make(map[string]*saga.Execution),
		leases: make(map[string]lease),
	}
}

// Save implements saga.Store.
func (s *Store) Save(ctx context.Context, exec *saga.Execution) error {
	if exec == nil {
		return fmt.Errorf("memory: cannot save a nil execution")
	}
	if exec.ID == "" {
		return fmt.Errorf("memory: cannot save an execution with an empty ID")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[exec.ID] = exec.Clone()
	return nil
}

// Get implements saga.Store.
func (s *Store) Get(ctx context.Context, id string) (*saga.Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	exec, ok := s.data[id]
	if !ok {
		return nil, saga.ErrExecutionNotFound
	}
	return exec.Clone(), nil
}

// ListIncomplete implements saga.Lister.
func (s *Store) ListIncomplete(ctx context.Context) ([]*saga.Execution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*saga.Execution
	for _, exec := range s.data {
		if !exec.Status.IsTerminal() {
			result = append(result, exec.Clone())
		}
	}
	return result, nil
}

// Acquire implements saga.Locker. A lease held by a different owner
// blocks acquisition only until it expires; an expired lease (or one
// that never existed) may be taken by anyone. The same owner calling
// Acquire again before its lease expires renews it for another ttl from
// now, which is how the engine keeps a lease alive for a
// still-genuinely-progressing execution.
func (s *Store) Acquire(ctx context.Context, id string, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if l, ok := s.leases[id]; ok && l.owner != owner && now.Before(l.expires) {
		return false, nil
	}
	s.leases[id] = lease{owner: owner, expires: now.Add(ttl)}
	return true, nil
}

// Release implements saga.Locker. Releasing a lease a different owner
// holds, or one that doesn't exist, is a no-op.
func (s *Store) Release(ctx context.Context, id string, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.leases[id]; ok && l.owner == owner {
		delete(s.leases, id)
	}
	return nil
}
