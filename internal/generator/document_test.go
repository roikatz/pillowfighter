package generator

import (
	"testing"
	"time"
)

// TestTimestampsAreRandomizedAndOrdered checks the two invariants that matter
// for the generated timestamps: they are spread over the lookback window
// rather than all sharing the run's wall-clock instant, and updatedAt never
// precedes createdAt.
func TestTimestampsAreRandomizedAndOrdered(t *testing.T) {
	const samples = 500

	before := time.Now()
	seenCreated := make(map[time.Time]struct{}, samples)
	seenUpdated := make(map[time.Time]struct{}, samples)

	for i := range samples {
		o := New(int64(i), 0)
		after := time.Now()

		if o.UpdatedAt.Before(o.CreatedAt) {
			t.Fatalf("doc %d: updatedAt %s precedes createdAt %s", i, o.UpdatedAt, o.CreatedAt)
		}
		if o.CreatedAt.Before(before.Add(-createdAtLookback)) {
			t.Fatalf("doc %d: createdAt %s is older than the lookback window", i, o.CreatedAt)
		}
		if o.UpdatedAt.After(after) {
			t.Fatalf("doc %d: updatedAt %s is in the future", i, o.UpdatedAt)
		}

		seenCreated[o.CreatedAt] = struct{}{}
		seenUpdated[o.UpdatedAt] = struct{}{}
	}

	// Randomized timestamps should produce many distinct values; a fixed
	// time.Now() for every document would collapse this to a handful.
	if len(seenCreated) < samples/2 {
		t.Errorf("createdAt looks insufficiently randomized: %d distinct values across %d docs", len(seenCreated), samples)
	}
	if len(seenUpdated) < samples/2 {
		t.Errorf("updatedAt looks insufficiently randomized: %d distinct values across %d docs", len(seenUpdated), samples)
	}
}

// TestPaddingApproachesTargetSize verifies --doc-size padding still works with
// the added timestamp field.
func TestPaddingApproachesTargetSize(t *testing.T) {
	const target = 2048

	o := New(1, target)
	size, err := jsonSize(o)
	if err != nil {
		t.Fatalf("marshaling padded order: %v", err)
	}
	if size < target {
		t.Errorf("padded document is %d bytes, want at least %d", size, target)
	}
}
