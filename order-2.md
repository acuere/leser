# Build Prompt: Zero-Dependency Architecture at Scale

> Context for the agent: `<NAME>` v0.0.1 exists and works. This document defines the architecture for v0.1 — the version that has to hold up in production without dragging an operator into distributed-systems administration.

---

## ROLE

You are the principal engineer on `<NAME>`, an open-source error tracking and observability platform. v0.0.1 is shipped and functional. Your job now is to build the storage and processing core that lets a single static binary serve a real production workload, and to build the horizontal scaling path that follows — **without ever requiring the operator to run a third-party service.**

Read Section 1 before writing any code. It defines the constraint that everything else follows from.

---

## 1. THE CONSTRAINT

**No required external services. Ever. At any scale.**

Not Kafka. Not Redis. Not ClickHouse. Not Zookeeper. Not Celery. Not Memcached. Not RabbitMQ. Not NATS. Not Elasticsearch. Not pgbouncer. Not a separate nginx.

The only external thing an operator may *optionally* configure is S3-compatible object storage for cold data — and that is a managed API, not a process they run.

This is not a preference. It is the product. When you find yourself reaching for a broker or an external analytical database, you have made an architectural mistake and must solve the problem in-process instead.

**Why this is achievable:** every one of those services solves a distributed problem. On a single node, the distributed problem does not exist. Kafka's durability and replay are a write-ahead log on local disk — and without a network hop or serialization boundary, the local version is *faster*. Redis's cache is a map. Redis's task broker is a queue in the same WAL. ClickHouse's columnar aggregation is Parquet segments plus a vectorized scanner. The distributed versions are the local versions plus a network, plus a consensus protocol, plus an operator who now has to know what an ISR is.

**The benchmark to remember:** a modern 16-core machine with NVMe should sustain 20,000+ events/second. The overwhelming majority of real deployments do fewer than 100/second. You are not building for the p99.99 org; you are building so the p50 org needs one machine and the p99 org needs three of *your* binary and still zero of anyone else's.

---

## 2. WHAT WE ARE REPLACING, AND WITH WHAT

Build each of these as an internal module with a narrow interface. Each is independently testable.

### 2.1 The log (replaces Kafka)

A segment-based, append-only write-ahead log on local disk. This is the spine of the system — ingest writes to it, everything else consumes from it.

- Fixed-size segment files (e.g. 128MB), named by starting offset, sealed when full.
- Records: `[length][crc32c][timestamp][type][payload]`. CRC verified on every read. A torn tail record on recovery is truncated, not tolerated.
- Append is a sequential write. Batch appends within a small time window (e.g. 2ms) and issue **one** `fsync` per batch — this is what turns 200 writes/sec into 20,000.
- Durability is a per-topic policy: `fsync_always` (financial-grade), `fsync_batched` (default), `fsync_interval` (throughput mode). Document the exact loss window of each. Do not hand-wave fsync semantics; on Linux, know your filesystem's behavior and test it.
- **Consumer groups by offset file.** Each consumer persists a committed offset; restart resumes from it. This gives you Kafka's replay semantics in ~600 lines.
- Multiple independent consumers over one log (grouping, alerting, analytics indexing, webhooks) — fanout without copying.
- Retention by size, age, and "all consumers have passed this offset." Sealed segments are deleted, never rewritten.
- **Backpressure is a function of log lag**, not queue length in memory. If the slowest consumer falls beyond a threshold, ingest starts returning `429` with `Retry-After`. Memory usage must be flat regardless of load.

Test it with deterministic simulation: kill the process at every possible point during a batch append and assert that recovery yields a prefix of the acknowledged writes, never a hole, never a corrupt record.

### 2.2 The metadata store (replaces Postgres + pgbouncer)

Embedded SQLite in WAL mode. Orgs, users, teams, projects, roles, tokens, issue metadata, alert rules, settings.

- Single writer goroutine/thread; unlimited concurrent readers. Enforce this structurally — the write path is a channel, not a shared handle.
- `PRAGMA synchronous=NORMAL` with WAL, `busy_timeout`, `foreign_keys=ON`, periodic `wal_checkpoint(TRUNCATE)`.
- This is good to tens of millions of rows and thousands of writes/second. It is not the bottleneck you think it is.
- Postgres remains available behind the same interface for people who want it for operational reasons (existing backup infrastructure, HA requirements). It is never required, never the default, and never a better experience.

### 2.3 The event store (replaces ClickHouse + Snuba)

A time-partitioned columnar store you own.

- **Hot tier:** recent events (last N hours) in an in-memory row buffer plus the WAL. Reads merge buffer + disk.
- **Warm tier:** a background compactor rolls the buffer into **Parquet segments**, partitioned by `(project_id, hour)`. Columnar, compressed (zstd), with per-column min/max statistics and Bloom filters on high-selectivity columns (`fingerprint`, `release`, `environment`, `user_id`).
- **Cold tier:** segments older than the retention threshold move to S3-compatible object storage if configured, with a local metadata index so queries know what's out there. Optional. Never required.
- **Query engine:** either embed DuckDB (it reads Parquet natively and is genuinely excellent — justify it as a dependency, it's a big one but it's a library, not a service) or write a focused vectorized scanner for the ~15 query shapes the product actually needs. Evaluate both in a spike and report before committing. The bias is toward the focused scanner if the query set really is that narrow, because it removes a large dependency; the bias is toward DuckDB if users will want ad-hoc analytical queries.
- **Segment pruning is the whole game.** A query for `project=X, last 24h, release=1.2.3` must touch only the segments whose statistics can possibly match. Measure and assert the pruning ratio in tests — a query that scans segments it didn't need to is a bug, not a performance nit.
- Aggregations the product needs: counts by time bucket, top-N by tag, unique user counts (HyperLogLog sketches maintained at compaction time, not computed on read), percentile latencies (t-digest, same).

### 2.4 The task system (replaces Celery + Redis broker)

An in-process worker pool consuming from the WAL.

- Fixed-size pool, sized from CPU count, configurable. Bounded. No dynamic spawning.
- Task state lives in the log, so a crash loses nothing that was acknowledged.
- Scheduled/delayed tasks: a persistent timer wheel backed by SQLite, checked on a tick.
- Per-task-type concurrency limits so symbolication cannot starve alerting.
- Retries with exponential backoff and full jitter. Dead-letter after N attempts into a queryable table so failures are visible, not silent.

### 2.5 Cache and rate limiting (replaces Redis + Memcached)

- In-process LRU with a **byte-size** bound, not an entry-count bound. Entry counts lie.
- Rate limiting: per-project token buckets in memory, state checkpointed to SQLite periodically so a restart doesn't reset every quota. Approximate-after-restart is fine and correct enough.
- Deduplication of `event_id`: a rotating Bloom filter over the recent window, with SQLite as the exact backstop for the current hour.

### 2.6 Edge, symbolication, TLS (replaces Relay, Symbolicator, nginx)

- Ingest handling is a route in the main binary. Relay exists so Sentry can deploy edge PoPs globally; single-tenant self-hosting has no such need. If someone wants an edge proxy later, the ingest endpoint is stateless and any proxy works.
- Symbolication (source maps, debug files) runs in the worker pool with a bounded concurrency limit and a strict per-job memory ceiling and timeout. Source map parsing is a well-known OOM vector — cap input size, cap output size, and kill jobs that exceed the deadline.
- TLS via embedded ACME/autocert, plus plain HTTP behind a proxy for people who already have one. No nginx in the deployment story.

---

## 3. THE SCALING LADDER

Each rung must be reachable without introducing a new service, and each must be documented with the concrete signal that tells an operator it's time to climb.

**Rung 0 — one process, one machine.** Default. Target: 20k events/sec sustained on 16 cores + NVMe, flat memory, p99 ingest latency under 10ms. This covers effectively everyone.

**Rung 1 — vertical + tiering.** Bigger box, cold segments to S3. Still one process. Publish honest benchmarks with the hardware, methodology, and the config used. Include the numbers where it *stops* working.

**Rung 2 — role separation, same binary.** `<NAME> serve --role=ingest|worker|query|all`. Ingest nodes append to a shared log; workers and query nodes consume it. Requires shared storage for the log at this rung (NFS-class or a single storage node) — document the tradeoff plainly rather than pretending it's free.

**Rung 3 — sharding by project.** Consistent hashing on `project_id`. Node membership via an embedded gossip library (memberlist-class — a library, not a service). Each node owns a shard's log and segments entirely; queries scatter-gather across shards and merge. Rebalancing moves whole segments, never rows. Any node can serve any API request by proxying to the owner.

**Rung 4 — replication.** Per-shard replication via embedded Raft (a library) for metadata, and segment replication to N peers for event data. This is the last rung and it is optional. If you reach here and it's getting complicated, the honest answer to the operator is "you are now Sentry-scale, and here is our pluggable-backend escape hatch" — not "we bolted on Kafka."

**Escape hatch:** keep the storage interfaces clean enough that someone with genuinely extreme scale can implement a ClickHouse or Kafka backend themselves. Support it as a contributed backend. Never make it the default, never make it required, never let its existence justify a design compromise in the default path.

---

## 4. HONEST TRADEOFFS — WRITE THESE DOWN

Put this in `docs/ARCHITECTURE.md` and in the README. Credibility comes from stating the costs plainly, and it costs you nothing because the costs don't apply to most users.

What a single node gives up:
- **No HA at Rung 0.** One machine dies, you're down until it restarts. Mitigation: fast recovery (measure it, target under 10 seconds), continuous backup, and SDK-side buffering means clients retry. Say this out loud rather than hiding it.
- **Query performance over hundreds of billions of rows** will not match a tuned ClickHouse cluster. Below a few billion, well-pruned Parquet is competitive and the operational difference is enormous.
- **Ingest and query scale together** until Rung 2. If your read load is wildly disproportionate to your write load, you'll climb sooner.
- **No cross-datacenter anything.** Deliberate.

What you gain, and should say just as plainly: one process to monitor, one thing to back up, one place to look when it breaks, no version-skew matrix between six services, no silent event loss because a consumer group rebalanced, and a `docker run` that is a complete production deployment.

---

## 5. NON-NEGOTIABLE ENGINEERING RULES

- **Bound everything.** Every buffer, queue, pool, cache, and concurrent job count has an explicit maximum. Memory under 10x overload must be flat. Write a test that asserts this.
- **Backpressure, never buffering.** Overload means `429` immediately, with fair shedding across projects so one noisy tenant cannot starve others.
- **Crash-only.** No shutdown-path correctness. Recovery-on-startup is exercised every boot; shutdown handlers are exercised never. `kill -9` at any instant must lose only unacknowledged in-flight work and must never corrupt a segment, a WAL, or the SQLite file.
- **Errors are values.** No exceptions or panics crossing module boundaries. Handler-level recovery logs with a trace ID and returns 500; the process survives.
- **Versioned on-disk formats.** Every segment, every WAL record, every Parquet schema carries a version. Never break backward compatibility. Test upgrades by booting N-1, writing data, swapping the binary, and asserting full readability.
- **Deterministic simulation testing** for the whole pipeline: simulated clock, filesystem, and network; every failure reproducible from a printed seed. Inject disk-full, I/O errors, partial writes, fsync failures, clock jumps in both directions, and OOM pressure. (Reference: FoundationDB, TigerBeetle.)
- **Fuzz** the WAL reader, the Parquet reader, the envelope parser, and the query planner in CI. Any finding blocks release.
- **Silent data loss is the cardinal sin** of this software category — it is exactly the failure mode that makes self-hosted Sentry miserable. Emit a `events_dropped_total{reason=...}` metric, surface it in the UI on the project page, and make `<NAME> doctor` report it. If an event is dropped, the operator finds out from us, not from an empty dashboard.

---

## 6. MILESTONES

1. **Spike + decision memo (≤800 words).** Benchmark: local WAL append throughput with batched fsync; Parquet scan throughput with and without segment pruning; DuckDB-embedded vs. custom scanner for our five hottest query shapes. Recommend and justify. Name the three riskiest parts of this build.
2. **WAL.** Segments, CRC, batched fsync, recovery, consumer offsets, retention. Crash-simulation test suite. Benchmark published.
3. **Event store.** Row buffer → Parquet compaction, statistics and Bloom filters, pruning-ratio assertions, tiering to S3.
4. **Query layer.** The product's query shapes, sketches for uniques and percentiles, planner fuzzing, latency benchmarks at 1M / 100M / 1B events.
5. **Task system + cache + rate limiting.** Full removal of any remaining external dependency from v0.0.1. `docker run` with one volume is a complete deployment.
6. **Robustness.** Deterministic simulation harness over the full pipeline, fault injection, sustained load test in CI asserting flat memory and bounded p99 at 2x overload, upgrade tests.
7. **Rung 2.** `--role` flags, shared-log deployment, documented tradeoffs.
8. **Rung 3.** Sharding, gossip membership, scatter-gather queries, segment rebalancing.
9. **Docs.** `ARCHITECTURE.md` with the real diagram and the honest tradeoffs, benchmarks with full methodology, scaling ladder with the signals for each rung, backup/restore, upgrade guide.

---

## 7. HOW TO RESPOND

Start with Milestone 1: run the actual benchmarks, show real numbers from real runs, and write the decision memo. Do not write the memo from priors — measure. Then ask any blocking questions, then begin Milestone 2.

For every milestone: tests alongside the code, executed with output shown, and an honest report of what is stubbed. Never claim something works that you have not run. If a benchmark comes in worse than this document assumes, say so immediately and propose the revision — the architecture serves the numbers, not the other way around.