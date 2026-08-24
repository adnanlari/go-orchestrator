// Package idgen generates random identifiers for executions and other
// entities that need one. It has no dependency on the public saga types,
// which is what lets it live under internal/ without risking an import
// cycle (see the note in ARCHITECTURE.md on why the state machine could
// not do the same).
package idgen

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a random 128-bit identifier encoded as 32 lowercase hex
// characters. It panics if the system's cryptographic random source
// cannot be read, which does not happen in practice on any supported
// platform.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("idgen: failed to read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
