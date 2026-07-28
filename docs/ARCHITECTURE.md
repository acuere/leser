# leser architecture

One static binary. No required external services — not Kafka, not Redis, not
ClickHouse, not a broker, not a separate worker fleet. That is the product.

## The picture

```
                            ┌──────────────────────────────────────────────┐
                            │                leser (one process)           │
   Sentry SDKs              │                                              │
  (unmodified) ──DSN──▶  HTTP ingest ──▶  WAL (segmented append log)       │
                            │  │auth LRU        │                          │
                            │  │per-proj        ▼                          │
   Browser ────────▶  UI + REST API      pipeline consumer                 │
                            │  │RBAC            │ parse→group→dedupe       │
                            │  ▼                ▼                          │
                            │  SQLite      event store                     │
                            │  (metadata,  hot row buffer                  │
                            │   issues,      └─▶ Parquet segments          │
                            │   users,           (project,hour)-partitioned│
                            │   audit)           stats + bloom + HLL       │
                            └──────────────────────────────────────────────┘
                                     one directory on disk = everything
```

## What replaces what

| Usually | Here | Why it holds |
|---|---|---|
| Kafka | `internal/wal` — segmented append-only log, CRC32C, group-commit fsync, consumer offset files | On one node, Kafka's durability/replay is a local WAL. Measured: 77k acked-durable appends/sec @640 producers on a laptop; raw streaming 1.3M/sec. |
| Postgres + pgbouncer | `internal/metadata` — SQLite WAL mode, structural single-writer (a channel, not a convention) | Tens of millions of rows, thousands of writes/sec. Not the bottleneck you think. |
| ClickHouse + Snuba | `internal/eventstore` — hot row buffer + zstd Parquet segments partitioned (project, hour), min/max stats, Bloom filters, footer HLL sketches | Pruning is the whole game: measured 46× (488ms full scan → 10.6ms) on 960k rows. A test FAILS if a query opens a segment the statistics could exclude. |
| Redis / Memcached | `internal/cache` — byte-bounded in-process LRU | A cache on one node is a map with a byte budget. |
| Redis rate limiting | `internal/ratelimit` — per-project token buckets, SQLite checkpoint every 30s | Approximate-after-restart is correct enough. |
| Celery + broker | worker pool consuming the WAL *(lands with alerting)* | Task state in the log; a crash loses nothing acknowledged. |
| Relay / nginx | ingest is a route in the binary; put any proxy in front if you want TLS termination | Single-tenant self-hosting has no edge-PoP problem. |

## Backpressure, not buffering

Admission control happens **before** work is accepted:

1. Per-project token bucket → `429` + `Retry-After` + `X-Sentry-Rate-Limits`
   (fairness: one noisy project cannot starve others).
2. WAL consumer lag over threshold → `429` (global shed).
3. Accepted = appended to the WAL and fsynced (group commit) **before** the
   `200`. Acknowledged events are never capacity-dropped afterwards; the
   consumer blocks and lag sheds new load at the edge instead.

Every drop increments `events_dropped_total{reason}` — enumerated reasons,
visible in the control plane. Silent data loss is the cardinal sin of this
software category.

## Crash-only

There is no shutdown-path correctness anywhere:

- WAL: torn tail truncated on every open; the crash suite kills at **every
  byte offset** (~4.7k recovery cycles per run) and asserts a clean prefix.
- Event store: segments written tmp → fsync → rename; a crash leaves the
  complete segment or nothing, and the WAL replays the difference.
- Consumer offsets: atomic rename; at-least-once + `event_id` dedupe.
- SQLite: WAL journal mode; recovery is SQLite's problem, which it solved.

`kill -9` at any instant loses at most unacknowledged in-flight requests.

## What a single node gives up (say it out loud)

- **No HA at Rung 0.** The machine dies → you're down until it restarts.
  Mitigations: fast recovery (WAL scan on boot), one directory to back up,
  SDK-side retry buffering. If you need five nines, you need Rung 4 or a
  different tool.
- **Hundreds of billions of rows** will not match a tuned ClickHouse cluster.
  Below a few billion, well-pruned Parquet is competitive and the operational
  difference is enormous (measured ~2M rows/sec/core full-scan; real queries
  prune to a fraction).
- **Ingest and query scale together** until role separation (Rung 2).
- **No cross-datacenter anything.** Deliberate.

What you gain: one process to monitor, one directory to back up, one place to
look when it breaks, no version-skew matrix between six services, and a
`docker run` that is a complete production deployment.

## Scaling ladder (climb only on signal)

| Rung | What | Climb when |
|---|---|---|
| 0 | one process, one machine (default) | — |
| 1 | bigger box; cold segments to S3 (planned) | disk high-water alerts; p99 ingest latency rising with CPU headroom gone |
| **2** | **`--role=ingest\|worker\|query`, shared `--data-dir` (landed)** | read load disproportionate to write load |
| 3 | project sharding, gossip membership (planned) | a single box cannot hold one project's write rate |
| 4 | per-shard Raft + segment replication (planned, last, optional) | you are Sentry-scale; also consider the pluggable-backend escape hatch |

### Rung 2, honestly

`leser serve --role=ingest`, `--role=worker`, `--role=query` split the single
process into three, **pointed at one shared `--data-dir`** (NFS-class storage
or a single storage node — this rung does not remove that requirement, it
only removes the requirement that ingest/compaction/query CPU live on the
same box). Coordination is entirely through the filesystem: no RPC, no
service mesh between roles.

- **ingest** owns the WAL as its single writer. Accepts envelopes, computes
  backpressure from the worker's on-disk committed offset (`WAL.ConsumerOffset`)
  — no in-memory channel to the worker exists, so lag is read from the same
  file the worker's `Reader.Commit` writes.
- **worker** attaches the WAL read-only and *live-tails* it (`WAL.Refresh`,
  polled every 25ms): a second `*wal.Log` on the same directory has no way to
  see segments another process created except by re-listing the directory,
  so that's what it does. Owns compaction, grouping, and alert delivery.
- **query** never writes the WAL or the event store. It discovers segments
  the worker compacted via `eventstore.Store.Refresh` (polled every 2s) and
  serves the REST API + UI from them plus SQLite (issues/alerts/audit, which
  SQLite's own WAL-mode locking already handles across processes).

**The tradeoff, stated plainly:** a `--role=query` node lags the worker by up
to its poll interval (2s) *plus* the worker's `FlushAge` (default 10s) for
anything still sitting in the worker's hot buffer — worst case ~12s before an
event is visible on a query node, versus effectively immediate on `--role=all`
where the same process wrote it. This is the real cost of role separation;
Rung 2 buys CPU isolation between ingest/compaction/query, not a reduction in
staleness. Verified with three actual separate OS processes sharing one
directory (`robustness/rung2.sh`, runs in CI): an event posted to the ingest
process's port becomes queryable through the query process's port — a
different process, different port — with role isolation confirmed (the query
node accepts no ingest POSTs; the worker node serves no API).

The storage interfaces stay clean enough that a ClickHouse/Kafka backend can
be contributed for genuinely extreme scale. It will never be the default and
its existence never justifies a compromise in the default path.

## Measured numbers (Apple M4 laptop; methodology in docs/spikes/)

| What | Number |
|---|---|
| WAL acked durable appends (640 producers, group commit, full barrier) | 77k/sec |
| WAL raw streaming (batched fsync 2ms) | 1.3M/sec |
| Per-record durable fsync (why batching exists) | 253/sec |
| 1M rows ingested + compacted to Parquet | 0.67s |
| Aggregate (buckets + 4 top-Ns + HLL uniques) over 100k-row window | 39ms |
| Pruned point query, 240k rows / 48 segments | 1.76ms |
| Partition pruning vs full scan (960k rows) | 46× |
| Overload shed (2×+ quota, 64 conns, 15s) | 1.44M clean 429s, 0 hard failures, p99 11.4ms on accepted, RSS flat |
| Chaos: SIGKILL × 6 under live ingest | 100% of acked events survived, zero corruption |

Both robustness suites run in CI on every push (`robustness/chaos.sh`,
`robustness/load.sh`), as does the real-SDK conformance suite
(`conformance/run.sh` — actual sentry-sdk, @sentry/node, sentry-go pointed at
leser by DSN alone).
```
