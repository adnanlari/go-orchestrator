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

	saga "github.com/adnanlari/go-orchestrator"
)

// Store is an in-memory, concurrency-safe saga.Store.
type Store struct {
	mu   sync.RWMutex
	data map[string]*saga.Execution
}

// New returns an empty, ready-to-use Store.
func New() *Store {
	return &Store{data: make(map[string]*saga.Execution)}
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
