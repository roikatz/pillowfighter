// Package output writes a completed engine.Result to disk as JSON or CSV.
package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/couchbase/cb-loadgen/internal/engine"
)

// Write encodes result as JSON or CSV (per format) to path, creating any
// missing parent directories.
func Write(format string, path string, result engine.Result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating output directory for %q: %w", path, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating output file %q: %w", path, err)
	}
	defer f.Close()

	switch format {
	case "json":
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			return fmt.Errorf("writing JSON to %q: %w", path, err)
		}
	case "csv":
		w := csv.NewWriter(f)
		header := []string{
			"startedAt", "finishedAt", "durationSec", "totalOps", "writes", "reads",
			"errors", "opsPerSec", "p50Micros", "p90Micros", "p95Micros", "p99Micros", "maxLatencyMicros",
		}
		row := []string{
			result.StartedAt.Format("2006-01-02T15:04:05Z07:00"),
			result.FinishedAt.Format("2006-01-02T15:04:05Z07:00"),
			strconv.FormatFloat(result.Duration.Seconds(), 'f', 3, 64),
			strconv.FormatInt(result.TotalOps, 10),
			strconv.FormatInt(result.Writes, 10),
			strconv.FormatInt(result.Reads, 10),
			strconv.FormatInt(result.Errors, 10),
			strconv.FormatFloat(result.OpsPerSec, 'f', 2, 64),
			strconv.FormatFloat(result.P50, 'f', 2, 64),
			strconv.FormatFloat(result.P90, 'f', 2, 64),
			strconv.FormatFloat(result.P95, 'f', 2, 64),
			strconv.FormatFloat(result.P99, 'f', 2, 64),
			strconv.FormatFloat(result.MaxLatency, 'f', 2, 64),
		}
		if err := w.Write(header); err != nil {
			return fmt.Errorf("writing CSV header to %q: %w", path, err)
		}
		if err := w.Write(row); err != nil {
			return fmt.Errorf("writing CSV row to %q: %w", path, err)
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return fmt.Errorf("flushing CSV to %q: %w", path, err)
		}
	default:
		return fmt.Errorf("unsupported output format %q, expected json|csv", format)
	}

	return nil
}
