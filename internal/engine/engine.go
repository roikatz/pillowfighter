// Package engine is the reusable load-generation core: a bounded worker pool
// that drives KV operations against Couchbase at a configured concurrency,
// collects metrics, and reports progress. Both the CLI and any future
// non-CLI caller (e.g. an HTTP server) build a config.RunConfig and call
// Run — no load-generation logic lives outside this package.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/cb-loadgen/internal/config"
	"github.com/couchbase/cb-loadgen/internal/connection"
	"github.com/couchbase/cb-loadgen/internal/metrics"
	"github.com/couchbase/cb-loadgen/internal/workload"
)

// defaultKeyspaceSize bounds the key range used for read/mixed workloads when
// the caller drives the run by --duration instead of a fixed --num-docs.
const defaultKeyspaceSize = 100_000

// Progress is a periodic, point-in-time view of a run, emitted on the caller's
// progress channel roughly every ReportInterval.
type Progress struct {
	ElapsedSec    float64
	Ops, Errors   int64
	OpsPerSec     float64
	P50, P95, P99 float64 // microseconds
}

// Result is the final summary of a completed run.
type Result struct {
	Config     config.RunConfig `json:"config"`
	StartedAt  time.Time        `json:"startedAt"`
	FinishedAt time.Time        `json:"finishedAt"`
	Duration   time.Duration    `json:"duration"`
	TotalOps   int64            `json:"totalOps"`
	Writes     int64            `json:"writes"`
	Reads      int64            `json:"reads"`
	Errors     int64            `json:"errors"`
	OpsPerSec  float64          `json:"opsPerSec"`
	P50        float64          `json:"p50Micros"`
	P90        float64          `json:"p90Micros"`
	P95        float64          `json:"p95Micros"`
	P99        float64          `json:"p99Micros"`
	MaxLatency float64          `json:"maxLatencyMicros"`
}

// Run connects to the cluster per cfg, optionally seeds the keyspace for
// read/mixed workloads, optionally warms up, then drives the measured run at
// cfg.Concurrency until cfg.NumDocs operations complete, cfg.Duration
// elapses, or — if cfg.Infinite is set — ctx is cancelled (Ctrl+C/SIGTERM).
// In every mode the document keyspace itself stays finite: keys wrap modulo
// cfg.NumDocs (or a default size if unset), so --infinite never grows the
// dataset unbounded. Progress snapshots are sent on the progress channel
// roughly every cfg.ReportInterval if it is non-nil; Run never closes it.
func Run(ctx context.Context, cfg config.RunConfig, progress chan<- Progress) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, fmt.Errorf("invalid config: %w", err)
	}

	target, err := connection.Connect(cfg)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		if cerr := target.Close(); cerr != nil {
			slog.Warn("closing cluster connection", "error", cerr)
		}
	}()

	durability, err := connection.DurabilityLevel(cfg.Durability)
	if err != nil {
		return Result{}, err
	}

	readPct, _, err := cfg.ReadWriteRatio()
	if err != nil {
		return Result{}, err
	}

	keyspace := cfg.NumDocs
	if keyspace <= 0 {
		keyspace = defaultKeyspaceSize
	}

	if cfg.Workload != config.WorkloadWrite {
		slog.Info("seeding keyspace before measured run", "keys", keyspace, "startIndex", cfg.StartIndex)
		if err := seedKeyspace(ctx, target.Collection, keyspace, cfg.StartIndex, cfg.Concurrency, cfg.DocSize, durability); err != nil {
			return Result{}, fmt.Errorf("seeding keyspace: %w", err)
		}
	}

	if cfg.Warmup > 0 {
		slog.Info("running warmup phase", "duration", cfg.Warmup)
		warmupCtx, cancel := context.WithTimeout(ctx, cfg.Warmup)
		runPhase(warmupCtx, target.Collection, cfg, durability, readPct, keyspace, metrics.NewCollector(), nil)
		cancel()
	}

	collector := metrics.NewCollector()
	runCtx := ctx
	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	started := time.Now()
	runPhase(runCtx, target.Collection, cfg, durability, readPct, keyspace, collector, progress)
	finished := time.Now()

	elapsed := finished.Sub(started)
	snap := collector.Merge()
	writes := collector.Counters.Writes.Load()
	reads := collector.Counters.Reads.Load()
	errs := collector.Counters.Errors.Load()
	total := writes + reads

	opsPerSec := 0.0
	if elapsed.Seconds() > 0 {
		opsPerSec = float64(total) / elapsed.Seconds()
	}

	return Result{
		Config:     cfg,
		StartedAt:  started,
		FinishedAt: finished,
		Duration:   elapsed,
		TotalOps:   total,
		Writes:     writes,
		Reads:      reads,
		Errors:     errs,
		OpsPerSec:  opsPerSec,
		P50:        snap.P50,
		P90:        snap.P90,
		P95:        snap.P95,
		P99:        snap.P99,
		MaxLatency: snap.Max,
	}, nil
}

// runPhase spawns cfg.Concurrency workers pulling job indices from an
// internally-owned channel, executes the configured workload op per job, and
// (if progress is non-nil) emits periodic Progress snapshots until the job
// producer finishes — either cfg.NumDocs jobs have been issued (when
// cfg.Duration is unset) or ctx is done (time-bounded or externally
// cancelled). Blocks until every worker has drained the job channel.
func runPhase(
	ctx context.Context,
	collection *gocb.Collection,
	cfg config.RunConfig,
	durability gocb.DurabilityLevel,
	readPct int,
	keyspace int64,
	collector *metrics.Collector,
	progress chan<- Progress,
) {
	jobs := make(chan int64, cfg.Concurrency*2)

	var wg sync.WaitGroup
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := collector.WorkerHistogram()
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
			for idx := range jobs {
				doRead := cfg.Workload == config.WorkloadRead ||
					(cfg.Workload == config.WorkloadMixed && rng.IntN(100) < readPct)
				keyIdx := cfg.StartIndex + idx%keyspace

				start := time.Now()
				var err error
				if doRead {
					err = workload.Read(collection, workload.KeyFor(keyIdx))
				} else {
					err = workload.Write(collection, workload.KeyFor(keyIdx), keyIdx, cfg.DocSize, durability)
				}
				elapsed := time.Since(start)

				if err != nil {
					collector.Counters.Errors.Add(1)
					continue
				}
				h.RecordValue(elapsed.Microseconds())
				if doRead {
					collector.Counters.Reads.Add(1)
				} else {
					collector.Counters.Writes.Add(1)
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(jobs)
		if cfg.Infinite || (cfg.Duration > 0 && cfg.NumDocs <= 0) {
			// Unbounded: keep producing until ctx fires — either cfg.Duration's
			// timeout, or (for --infinite) the top-level Ctrl+C/SIGTERM context.
			// Keys still wrap modulo keyspace, so the document set stays finite.
			var idx int64
			for {
				select {
				case <-ctx.Done():
					return
				case jobs <- idx:
					idx++
				}
			}
		}
		// Count-bounded: issue exactly cfg.NumDocs jobs, but stop early on cancellation.
		for idx := int64(0); idx < cfg.NumDocs; idx++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- idx:
			}
		}
	}()

	if progress != nil {
		go reportProgress(ctx, done, cfg.ReportInterval, collector, progress)
	}

	wg.Wait()
	close(done)
}

// reportProgress sends a Progress snapshot on progress every interval until
// done is closed or ctx is cancelled, using a start time captured at the first
// tick so ElapsedSec reflects this phase's own runtime.
func reportProgress(ctx context.Context, done <-chan struct{}, interval time.Duration, collector *metrics.Collector, progress chan<- Progress) {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start)
			ops := collector.Counters.TotalOps()
			errs := collector.Counters.Errors.Load()
			snap := collector.Merge()
			opsPerSec := 0.0
			if elapsed.Seconds() > 0 {
				opsPerSec = float64(ops) / elapsed.Seconds()
			}
			select {
			case progress <- Progress{
				ElapsedSec: elapsed.Seconds(),
				Ops:        ops,
				Errors:     errs,
				OpsPerSec:  opsPerSec,
				P50:        snap.P50,
				P95:        snap.P95,
				P99:        snap.P99,
			}:
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}
}

// seedKeyspace writes every key in [startIndex, startIndex+keyspace) once,
// using its own bounded worker pool. Individual write failures are logged and
// counted but do not abort the seed pass; only a context cancellation returns
// an error.
func seedKeyspace(ctx context.Context, collection *gocb.Collection, keyspace, startIndex int64, concurrency int, docSize int, durability gocb.DurabilityLevel) error {
	jobs := make(chan int64, concurrency*2)
	var wg sync.WaitGroup
	var failed atomic.Int64

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if err := workload.Write(collection, workload.KeyFor(idx), idx, docSize, durability); err != nil {
					failed.Add(1)
					slog.Warn("seed write failed", "key", workload.KeyFor(idx), "error", err)
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for idx := startIndex; idx < startIndex+keyspace; idx++ {
			select {
			case <-ctx.Done():
				return
			case jobs <- idx:
			}
		}
	}()

	wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}
	if n := failed.Load(); n > 0 {
		slog.Warn("seed pass completed with failures", "failed", n, "total", keyspace)
	}
	return nil
}
