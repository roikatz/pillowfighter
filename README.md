# cb-loadgen

A Go alternative to `cbc-pillowfight`: a single static binary that drives concurrent
KV (write/read/mixed) load against a Couchbase cluster using the official
[Couchbase Go SDK](https://github.com/couchbase/gocb) (`gocb/v2`), reports
throughput and latency percentiles (p50/p90/p95/p99 via HdrHistogram), and
exports results as JSON or CSV.

## Goal

`cbc-pillowfight` generates KV load with configurable document size, item
count, set/get ratio, thread count, and durability. `cb-loadgen` is a modern
Go equivalent: same job, a CLI that's easy to read, extend, and tune, built on
a **bounded worker pool** of goroutines rather than libcouchbase threads.

The load-generation logic lives entirely in the reusable [`internal/engine`](internal/engine/engine.go)
package behind a single `Run(ctx, cfg, progress)` entry point — the CLI's `run`
command is a thin wrapper around it. This separation exists so a future
non-CLI caller (e.g. an HTTP server driving a web UI) can reuse the exact same
engine with no duplicated logic. **This first pass ships CLI-only** (see
"What's not built yet" below).

## Architecture

```
cmd/cb-loadgen/        cobra CLI: main.go (root), run.go (the `run` subcommand)
internal/config/       RunConfig model + YAML/env/flag loader (viper), validation
internal/connection/   gocb.Connect flow: kv_pool_size, KVTimeout, compression, durability
internal/generator/    realistic e-commerce Order document generator (gofakeit)
internal/workload/     Write / Read op implementations against a gocb.Collection
internal/engine/       the reusable core: bounded worker pool, Run(ctx, cfg, progress)
internal/metrics/      per-worker HdrHistogram + atomic counters, merge-on-demand
internal/output/       JSON/CSV result exporter
config/                config.example.yaml
results/               default destination for exported run results
```

### Concurrency model

A fixed pool of `--concurrency` worker goroutines pull job indices off a
buffered channel and execute KV ops against a shared `gocb.Collection`
(gocb/gocbcore's connection pool is itself concurrency-safe). Each worker owns
its **own** `hdrhistogram.Histogram` — that type is not safe for concurrent
writes — and histograms are merged into one only when computing a progress
snapshot or the final result. Op/error counts use `atomic.Int64` on the hot
path. This is the idiomatic Go translation of "async max-throughput": no
reactive-extensions library, just goroutines + a channel + `sync/atomic`.

## Setup

Requires Go 1.26+. Fetch dependencies and build:

```sh
go mod tidy
make build            # or: go build -o bin/cb-loadgen ./cmd/cb-loadgen
```

## Configuration

Every setting can come from (in increasing precedence): built-in defaults →
a YAML config file (`--config path.yaml`) → CLI flags. See
[`config/config.example.yaml`](config/config.example.yaml) for a full example.

The password is never read from a config file value you'd want to commit —
pass it via `--password` or the `CB_PASSWORD` environment variable.

Run `./bin/cb-loadgen run --help` for the full flag list. Key knobs:

| Flag | Purpose |
|---|---|
| `--connection-string`, `--username`, `--password`, `--bucket`, `--scope`, `--collection` | Connection target |
| `--num-kv-connections` | KV connections per node (`kv_pool_size`) — tune together with `--concurrency` |
| `--kv-timeout`, `--compression`, `--durability` | gocb/gocbcore tuning |
| `--workload write\|read\|mixed`, `--mix "80:20"` | Operation mix (read:write ratio, mixed only) |
| `--num-docs` / `--duration` | Bound the run by op count or wall-clock time (set one) |
| `--concurrency` | Number of worker goroutines / in-flight ops — the primary throughput knob |
| `--doc-size` | Approximate padded document size in bytes |
| `--warmup` | Unmeasured warmup phase before the timed run starts |
| `--output json\|csv`, `--output-file` | Where results land |

For `read` and `mixed` workloads, `cb-loadgen` seeds the keyspace (writes every
key once, unmeasured) before the timed run, since reads need existing
documents. A pure `write` run needs no seeding — it *is* the population pass.

## Running

Against a local cluster with the `benchmark` bucket:

```sh
./bin/cb-loadgen run \
  --connection-string "couchbase://localhost?kv_pool_size=8" \
  --username Administrator --bucket benchmark \
  --workload mixed --mix 80:20 --num-docs 100000 --concurrency 256 \
  --output json --output-file results/run.json
```

Or from the example config file:

```sh
./bin/cb-loadgen run --config config/config.example.yaml --password "$CB_PASSWORD"
```

Progress prints once per `--report-interval`:

```
[   1.0s] ops=48213 errors=0 ops/sec=48213 p50=412us p95=1102us p99=2310us
```

and a final summary plus the exported result file once the run completes:

```
--- Run Summary ---
Duration:   10.02s
Total ops:  512400 (writes=102480 reads=409920 errors=0)
Throughput: 51137.7 ops/sec
Latency:    p50=380us p90=890us p95=1120us p99=2450us max=15300us
Results written to results/run.json
```

## What's not built yet (Tier 4, later pass)

The `serve` command, the HTTP API (`/api/runs`, SSE progress streaming), and
the Next.js web UI are intentionally out of scope for this pass. The engine's
`Run(ctx, cfg, progress)` signature is already the seam that pass would attach
to — no engine rework will be needed to add it.
