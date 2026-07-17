// Package workload implements the individual KV operations (write, read) that
// the engine's worker pool executes per job. Each function is a small, hot-path
// call with no allocations beyond what gocb itself requires.
package workload

import (
	"fmt"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/cb-loadgen/internal/generator"
)

// Write upserts a freshly generated document under key and returns nothing but
// an error; callers time the call and record latency themselves so this stays
// allocation-light on the hot path.
func Write(collection *gocb.Collection, key string, index int64, docSize int, durability gocb.DurabilityLevel) error {
	doc := generator.New(index, docSize)
	_, err := collection.Upsert(key, doc, &gocb.UpsertOptions{
		DurabilityLevel: durability,
	})
	if err != nil {
		return fmt.Errorf("upsert %q: %w", key, err)
	}
	return nil
}

// Read fetches the document at key. It returns an error for the caller to
// classify and count; a missing document is treated the same as any other
// error since a read-workload run assumes the keyspace has been seeded by a
// prior write pass.
func Read(collection *gocb.Collection, key string) error {
	_, err := collection.Get(key, nil)
	if err != nil {
		return fmt.Errorf("get %q: %w", key, err)
	}
	return nil
}

// KeyFor derives a deterministic key for a document index, shared by writers
// (to seed the keyspace) and readers (to look up a previously written key).
func KeyFor(index int64) string {
	return fmt.Sprintf("cb-loadgen::%d", index)
}
