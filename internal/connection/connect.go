// Package connection builds a tuned gocb.Cluster connection from a RunConfig and
// resolves the target bucket/scope/collection.
package connection

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/couchbase/cb-loadgen/internal/config"
)

// Target bundles the connected cluster handle and the resolved collection that
// workload operations run against.
type Target struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Collection *gocb.Collection
}

// Connect opens a cluster connection tuned per cfg (KV connection pool size, KV
// timeout, compression) and resolves the configured bucket/scope/collection.
// Errors are wrapped with enough context to explain what failed without a panic.
func Connect(cfg config.RunConfig) (*Target, error) {
	connStr, err := withKVPoolSize(cfg.ConnectionString, cfg.NumKVConnections)
	if err != nil {
		return nil, fmt.Errorf("building connection string: %w", err)
	}

	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Username: cfg.Username,
		Password: cfg.Password,
		TimeoutsConfig: gocb.TimeoutsConfig{
			KVTimeout: cfg.KVTimeout,
		},
		CompressionConfig: gocb.CompressionConfig{
			Disabled: !cfg.Compression,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to cluster %q as %q: %w", connStr, cfg.Username, err)
	}

	bucket := cluster.Bucket(cfg.Bucket)
	if err := bucket.WaitUntilReady(10*time.Second, nil); err != nil {
		return nil, fmt.Errorf("bucket %q not ready: %w", cfg.Bucket, err)
	}

	scope := cfg.Scope
	if scope == "" {
		scope = "_default"
	}
	collName := cfg.Collection
	if collName == "" {
		collName = "_default"
	}

	collection := bucket.Scope(scope).Collection(collName)

	return &Target{
		Cluster:    cluster,
		Bucket:     bucket,
		Collection: collection,
	}, nil
}

// Close releases the underlying cluster connection.
func (t *Target) Close() error {
	if t == nil || t.Cluster == nil {
		return nil
	}
	return t.Cluster.Close(nil)
}

// withKVPoolSize appends kv_pool_size=<n> to the connection string when numConns > 0,
// which maps to the KV connections-per-node lever (gocbcore's per-node pool size).
func withKVPoolSize(connStr string, numConns uint) (string, error) {
	if numConns == 0 {
		return connStr, nil
	}
	u, err := url.Parse(connStr)
	if err != nil {
		return "", fmt.Errorf("parsing connection string %q: %w", connStr, err)
	}
	q := u.Query()
	q.Set("kv_pool_size", strconv.FormatUint(uint64(numConns), 10))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// DurabilityLevel maps a config.Durability name to its gocb equivalent.
func DurabilityLevel(d config.Durability) (gocb.DurabilityLevel, error) {
	switch d {
	case config.DurabilityNone, "":
		return gocb.DurabilityLevelNone, nil
	case config.DurabilityMajority:
		return gocb.DurabilityLevelMajority, nil
	case config.DurabilityMajorityAndPersistActive:
		return gocb.DurabilityLevelMajorityAndPersistOnMaster, nil
	case config.DurabilityPersistToMajority:
		return gocb.DurabilityLevelPersistToMajority, nil
	default:
		return gocb.DurabilityLevelNone, fmt.Errorf("unknown durability level %q", d)
	}
}
