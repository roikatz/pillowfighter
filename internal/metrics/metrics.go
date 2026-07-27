// Package metrics provides concurrency-safe operation counters and latency
// histograms for the engine's worker pool.
//
// hdrhistogram.Histogram is NOT safe for concurrent writes, so each worker owns
// its own histogram on the hot path and histograms are merged only once all
// workers finish (or periodically, for progress snapshots). Op/error counts use
// atomic.Int64 since those are cheap enough to update directly from every
// worker on every operation.
package metrics

import (
	"sync"
	"sync/atomic"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const (
	histogramMin     = 1
	histogramMax     = 60_000_000 // 60 seconds, in microseconds
	histogramSigFigs = 3
)

// Counters holds the shared, concurrency-safe op/error totals updated directly
// by every worker via sync/atomic — no locking on the hot path.
type Counters struct {
	Writes atomic.Int64
	Reads  atomic.Int64
	Errors atomic.Int64
}

// Collector owns per-worker histograms and the shared counters for a run. Call
// WorkerHistogram once per worker goroutine, record into it directly on the hot
// path, then call Merge to get combined percentiles (safe to call repeatedly for
// periodic progress, and once more after all workers finish for the final result).
type Collector struct {
	Counters Counters

	mu         sync.Mutex
	histograms []*hdrhistogram.Histogram
}

// NewCollector creates an empty Collector.
func NewCollector() *Collector {
	return &Collector{}
}

// WorkerHistogram allocates and registers a new per-worker histogram. Call this
// once per worker goroutine at startup; the returned histogram must not be
// shared across goroutines.
func (c *Collector) WorkerHistogram() *hdrhistogram.Histogram {
	h := hdrhistogram.New(histogramMin, histogramMax, histogramSigFigs)
	c.mu.Lock()
	c.histograms = append(c.histograms, h)
	c.mu.Unlock()
	return h
}

// Snapshot is a point-in-time view of combined latency percentiles, in
// microseconds, merged from every worker's histogram so far.
type Snapshot struct {
	P50, P90, P95, P99, Max float64
}

// Merge combines every registered worker histogram into a single snapshot.
func (c *Collector) Merge() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.histograms) == 0 {
		return Snapshot{}
	}
	merged := hdrhistogram.New(histogramMin, histogramMax, histogramSigFigs)
	for _, h := range c.histograms {
		merged.Merge(h)
	}
	return Snapshot{
		P50: float64(merged.ValueAtQuantile(50)),
		P90: float64(merged.ValueAtQuantile(90)),
		P95: float64(merged.ValueAtQuantile(95)),
		P99: float64(merged.ValueAtQuantile(99)),
		Max: float64(merged.Max()),
	}
}

// TotalOps returns writes + reads recorded so far.
func (c *Counters) TotalOps() int64 {
	return c.Writes.Load() + c.Reads.Load()
}
