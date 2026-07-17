package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/couchbase/cb-loadgen/internal/config"
	"github.com/couchbase/cb-loadgen/internal/engine"
	"github.com/couchbase/cb-loadgen/internal/output"
)

func newRunCommand() *cobra.Command {
	var configFile string

	cmd := &cobra.Command{
		Use:          "run",
		Short:        "Run a KV load-generation workload against a Couchbase cluster",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoad(cmd, configFile)
		},
	}

	def := config.Defaults()
	f := cmd.Flags()
	f.String("connection-string", def.ConnectionString, "Couchbase connection string, e.g. couchbase://localhost")
	f.String("username", def.Username, "Cluster username")
	f.String("password", def.Password, "Cluster password (falls back to $CB_PASSWORD)")
	f.String("bucket", def.Bucket, "Target bucket")
	f.String("scope", def.Scope, "Target scope")
	f.String("collection", def.Collection, "Target collection")

	f.Uint("num-kv-connections", def.NumKVConnections, "KV connections per node (kv_pool_size)")
	f.Duration("kv-timeout", def.KVTimeout, "KV operation timeout")
	f.Bool("compression", def.Compression, "Enable KV compression")
	f.String("durability", string(def.Durability), "Durability level: none|majority|majorityAndPersistOnMaster|persistToMajority")

	f.String("workload", string(def.Workload), "Workload type: write|read|mixed")
	f.String("mix", def.Mix, "Read:write ratio for mixed workload, e.g. 80:20")
	f.Int64("num-docs", def.NumDocs, "Total number of operations to run (0 to run by --duration instead)")
	f.Duration("duration", def.Duration, "Run for this long instead of a fixed op count (0 to run by --num-docs instead)")
	f.Int("doc-size", def.DocSize, "Approximate padded document size in bytes (0 = unpadded)")

	f.Int("concurrency", def.Concurrency, "Number of worker goroutines / in-flight operations")
	f.Duration("warmup", def.Warmup, "Warmup duration before measurement starts (not counted in results)")
	f.Duration("report-interval", def.ReportInterval, "How often to print progress")

	f.String("output", string(def.Output), "Result output format: json|csv")
	f.String("output-file", def.OutputFile, "Result output file path")

	cmd.Flags().StringVar(&configFile, "config", "", "Path to a YAML config file (CLI flags override file values)")

	return cmd
}

func runLoad(cmd *cobra.Command, configFile string) error {
	cfg, err := config.Load(configFile, cmd.Flags())
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	progress := make(chan engine.Progress)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for p := range progress {
			fmt.Printf("[%6.1fs] ops=%d errors=%d ops/sec=%.0f p50=%.0fus p95=%.0fus p99=%.0fus\n",
				p.ElapsedSec, p.Ops, p.Errors, p.OpsPerSec, p.P50, p.P95, p.P99)
		}
	}()

	fmt.Printf("Connecting to %s, workload=%s concurrency=%d ...\n", cfg.ConnectionString, cfg.Workload, cfg.Concurrency)

	result, runErr := engine.Run(ctx, cfg, progress)
	close(progress)
	<-done

	if runErr != nil {
		return fmt.Errorf("run failed: %w", runErr)
	}

	printSummary(result)

	if cfg.OutputFile != "" {
		if err := output.Write(string(cfg.Output), cfg.OutputFile, result); err != nil {
			return fmt.Errorf("writing results: %w", err)
		}
		fmt.Printf("Results written to %s\n", cfg.OutputFile)
	}

	return nil
}

func printSummary(r engine.Result) {
	fmt.Println("\n--- Run Summary ---")
	fmt.Printf("Duration:   %s\n", r.Duration.Round(time.Millisecond))
	fmt.Printf("Total ops:  %d (writes=%d reads=%d errors=%d)\n", r.TotalOps, r.Writes, r.Reads, r.Errors)
	fmt.Printf("Throughput: %.1f ops/sec\n", r.OpsPerSec)
	fmt.Printf("Latency:    p50=%.0fus p90=%.0fus p95=%.0fus p99=%.0fus max=%.0fus\n", r.P50, r.P90, r.P95, r.P99, r.MaxLatency)
}
