// Package config defines the run configuration shared by the CLI and (later) the HTTP API,
// and loads it from a YAML file, environment variables, and CLI flags (flags win).
package config

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Workload selects which operation mix a run drives against the cluster.
type Workload string

const (
	WorkloadWrite Workload = "write"
	WorkloadRead  Workload = "read"
	WorkloadMixed Workload = "mixed"
)

// Durability maps to a gocb.DurabilityLevel by name.
type Durability string

const (
	DurabilityNone                     Durability = "none"
	DurabilityMajority                 Durability = "majority"
	DurabilityMajorityAndPersistActive Durability = "majorityAndPersistOnMaster"
	DurabilityPersistToMajority        Durability = "persistToMajority"
)

// OutputFormat selects the result-file encoding.
type OutputFormat string

const (
	OutputCSV  OutputFormat = "csv"
	OutputJSON OutputFormat = "json"
)

// RunConfig is the full set of knobs for a load-generation run. It is built from a YAML
// config file merged with environment variables merged with CLI flags (flags take
// precedence), and is also the shape a future HTTP API would accept as a JSON body.
type RunConfig struct {
	// Connection
	ConnectionString string `mapstructure:"connection-string"`
	Username         string `mapstructure:"username"`
	Password         string `mapstructure:"password"`
	Bucket           string `mapstructure:"bucket"`
	Scope            string `mapstructure:"scope"`
	Collection       string `mapstructure:"collection"`

	// gocb / gocbcore tuning
	NumKVConnections uint          `mapstructure:"num-kv-connections"`
	KVTimeout        time.Duration `mapstructure:"kv-timeout"`
	Compression      bool          `mapstructure:"compression"`
	Durability       Durability    `mapstructure:"durability"`

	// Workload
	Workload Workload      `mapstructure:"workload"`
	Mix      string        `mapstructure:"mix"` // "read:write", e.g. "80:20"
	NumDocs  int64         `mapstructure:"num-docs"`
	Duration time.Duration `mapstructure:"duration"`
	// Infinite runs the workload until interrupted (Ctrl+C/SIGTERM) instead of
	// stopping at NumDocs ops or after Duration. NumDocs, if set, still bounds
	// the finite keyspace of documents cycled through; mutually exclusive with
	// Duration.
	Infinite bool `mapstructure:"infinite"`
	// StartIndex offsets every document key index by this amount, so a run
	// can target a fresh, non-overlapping range of keys instead of
	// overwriting a previous run's data (e.g. a second 10M-doc write batch
	// with --start-index 10000000 lands on keys 10000000..19999999).
	StartIndex int64 `mapstructure:"start-index"`
	DocSize    int   `mapstructure:"doc-size"` // approximate padded document size in bytes, 0 = unpadded

	// Execution
	Concurrency    int           `mapstructure:"concurrency"`
	Warmup         time.Duration `mapstructure:"warmup"`
	ReportInterval time.Duration `mapstructure:"report-interval"`

	// Output
	Output     OutputFormat `mapstructure:"output"`
	OutputFile string       `mapstructure:"output-file"`
}

// ReadRatio and WriteRatio parse the "read:write" Mix string into normalized weights.
// Defaults to 0:100 (all writes) if Mix is empty or malformed.
func (c RunConfig) ReadWriteRatio() (readPct, writePct int, err error) {
	if c.Mix == "" {
		return 0, 100, nil
	}
	var r, w int
	if _, err := fmt.Sscanf(c.Mix, "%d:%d", &r, &w); err != nil {
		return 0, 0, fmt.Errorf("invalid --mix %q, expected format \"read:write\" e.g. \"80:20\": %w", c.Mix, err)
	}
	if r < 0 || w < 0 || r+w == 0 {
		return 0, 0, fmt.Errorf("invalid --mix %q: ratio parts must be non-negative and sum > 0", c.Mix)
	}
	return r, w, nil
}

// Validate checks the config for obviously invalid combinations before a run starts.
func (c RunConfig) Validate() error {
	if c.ConnectionString == "" {
		return fmt.Errorf("connection-string is required")
	}
	if c.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0, got %d", c.Concurrency)
	}
	if c.StartIndex < 0 {
		return fmt.Errorf("start-index must be >= 0, got %d", c.StartIndex)
	}
	if c.Infinite {
		if c.Duration > 0 {
			return fmt.Errorf("--infinite cannot be combined with --duration")
		}
	} else if c.NumDocs <= 0 && c.Duration <= 0 {
		return fmt.Errorf("one of num-docs or duration must be set (or use --infinite)")
	}
	switch c.Workload {
	case WorkloadWrite, WorkloadRead, WorkloadMixed:
	default:
		return fmt.Errorf("invalid workload %q, expected write|read|mixed", c.Workload)
	}
	if c.Workload == WorkloadMixed {
		if _, _, err := c.ReadWriteRatio(); err != nil {
			return err
		}
	}
	switch c.Durability {
	case DurabilityNone, DurabilityMajority, DurabilityMajorityAndPersistActive, DurabilityPersistToMajority:
	default:
		return fmt.Errorf("invalid durability %q", c.Durability)
	}
	switch c.Output {
	case OutputCSV, OutputJSON:
	default:
		return fmt.Errorf("invalid output format %q, expected csv|json", c.Output)
	}
	return nil
}

// Defaults returns the baseline configuration used when no file, env, or flag overrides it.
func Defaults() RunConfig {
	return RunConfig{
		ConnectionString: "couchbase://localhost",
		Username:         "Administrator",
		Bucket:           "benchmark",
		Scope:            "_default",
		Collection:       "_default",
		NumKVConnections: 4,
		KVTimeout:        2500 * time.Millisecond,
		Compression:      true,
		Durability:       DurabilityNone,
		Workload:         WorkloadWrite,
		Mix:              "80:20",
		NumDocs:          10000,
		Infinite:         false,
		StartIndex:       0,
		DocSize:          0,
		Concurrency:      64,
		Warmup:           0,
		ReportInterval:   1 * time.Second,
		Output:           OutputJSON,
		OutputFile:       "results/run.json",
	}
}

// Load builds a RunConfig by layering, in increasing precedence: built-in defaults,
// an optional YAML config file, environment variables (CB_LOADGEN_* / CB_PASSWORD),
// and finally the CLI flags that the caller has already parsed via cobra/pflag.
func Load(configFile string, flags *pflag.FlagSet) (RunConfig, error) {
	v := viper.New()

	def := Defaults()
	v.SetDefault("connection-string", def.ConnectionString)
	v.SetDefault("username", def.Username)
	v.SetDefault("password", def.Password)
	v.SetDefault("bucket", def.Bucket)
	v.SetDefault("scope", def.Scope)
	v.SetDefault("collection", def.Collection)
	v.SetDefault("num-kv-connections", def.NumKVConnections)
	v.SetDefault("kv-timeout", def.KVTimeout)
	v.SetDefault("compression", def.Compression)
	v.SetDefault("durability", string(def.Durability))
	v.SetDefault("workload", string(def.Workload))
	v.SetDefault("mix", def.Mix)
	v.SetDefault("num-docs", def.NumDocs)
	v.SetDefault("duration", def.Duration)
	v.SetDefault("infinite", def.Infinite)
	v.SetDefault("start-index", def.StartIndex)
	v.SetDefault("doc-size", def.DocSize)
	v.SetDefault("concurrency", def.Concurrency)
	v.SetDefault("warmup", def.Warmup)
	v.SetDefault("report-interval", def.ReportInterval)
	v.SetDefault("output", string(def.Output))
	v.SetDefault("output-file", def.OutputFile)

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return RunConfig{}, fmt.Errorf("reading config file %q: %w", configFile, err)
		}
	}

	v.SetEnvPrefix("CB_LOADGEN")
	v.AutomaticEnv()
	// CB_PASSWORD (unprefixed) is the documented fallback for the --password flag,
	// checked after CB_LOADGEN_PASSWORD since BindEnv tries names in order.
	_ = v.BindEnv("password", "CB_LOADGEN_PASSWORD", "CB_PASSWORD")

	if flags != nil {
		if err := v.BindPFlags(flags); err != nil {
			return RunConfig{}, fmt.Errorf("binding flags: %w", err)
		}
	}

	var cfg RunConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return RunConfig{}, fmt.Errorf("parsing config: %w", err)
	}

	return cfg, nil
}
