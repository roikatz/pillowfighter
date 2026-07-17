// Command cb-loadgen is a Go alternative to cbc-pillowfight: a KV load
// generator for Couchbase driven by a bounded worker pool.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:           "cb-loadgen",
		Short:         "A concurrent Couchbase KV load generator",
		Long:          "cb-loadgen drives write/read/mixed KV workloads against a Couchbase cluster using a bounded worker-pool of goroutines, reporting throughput and latency percentiles.",
		SilenceErrors: true,
	}
	root.AddCommand(newRunCommand())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
