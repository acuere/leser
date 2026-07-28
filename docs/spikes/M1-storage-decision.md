# Storage-core decision memo (order-2, Milestone 1)

**Status:** WAL benchmark measured on real hardware. DuckDB-vs-scanner resolved by a
hard constraint (below). Parquet segment-pruning benchmark **not yet run** — flagged,
not faked.

## Hardware & method

Apple M4, 10 cores, APFS on NVMe. `bench/walbench` writes framed records
`[u32 len][u32 crc32c][i64 ts][u8 type][payload]`, 512B payload, buffered 1MB, CRC32C
(Castagnoli) computed per record. Throughput is a rate, so sample size is irrelevant;
per-record-fsync modes use 3k records because each is a physical flush. Reproduce:
`cd bench/walbench && go run .`

## Result (one representative run)

| durability policy | events/sec | MB/sec |
|---|---:|---:|
| no-fsync (OS cache only) | 3,462,614 | 1747 |
| **fsync_batched (2ms window)** | **1,378,562** | 695 |
| fsync_batched (every 1k records) | 245,139 | 124 |
| fsync_always (plain `fsync`) | 253 | 0.1 |
| fsync_always (`F_FULLFSYNC`) | 253 | 0.1 |
| batched 2ms + `F_FULLFSYNC` | 1,302,855 | 657 |

## What the numbers decide

1. **Batched fsync is the default, and it clears the target with absurd headroom.**
   1.38M events/sec vs the document's 20k/sec goal — ~65× on a *laptop*. A 16-core
   NVMe server is not the bottleneck; the network and JSON parsing will be.
2. **Batching is the entire trick, quantified.** Per-record durable fsync is **253/sec**.
   The same workload batched over a 2ms window is **1.3M/sec** — a 5,000× swing from one
   design choice. Order-2 §2.1's thesis is confirmed by measurement, not asserted.
3. **macOS durability caveat — resolved by data.** Plain `fsync` and `F_FULLFSYNC`
   produced identical 253/sec, which only happens if `fsync` is already doing the full
   barrier. It is: Go's `os.File.Sync()` maps to `F_FULLFSYNC` on Darwin since Go 1.12.
   So `f.Sync()` gives real, physical durability on macOS with no extra code. We keep an
   explicit `F_FULLFSYNC` path documented for auditability; it is a no-op-difference here.
4. **Financial-grade durability is nearly free *when batched*.** batched-2ms +
   F_FULLFSYNC = 1.30M/sec, ~5% under plain batched. Because one physical flush amortizes
   across thousands of records per window, `fsync_batched` can safely default to the full
   barrier. `fsync_always` (253/sec) is reserved for the rare per-event-critical topic.

**Durability policy decision:**
- `fsync_batched` (default) — ack after the batch's F_FULLFSYNC. Loss window = records
  written-but-not-yet-synced in the open ≤2ms batch, which are **never acked**. A crash
  loses only un-acked in-flight work. Matches order-2 §5 crash-only.
- `fsync_always` — ack after each record's flush. ~250/sec. Financial-grade single-event.
- `fsync_interval` — timer-driven flush, throughput mode, documented loss window = interval.

## Query engine: custom scanner, not DuckDB

Not a preference — a constraint collision. The embedded DuckDB Go binding
(`marcboeker/go-duckdb`) **requires CGO**. order.md hard-constraint #1 is
`CGO_ENABLED=0`, single fully-static binary. Embedding DuckDB forfeits static linking,
reproducible builds, and cross-compiled release artifacts we already ship in CI. That
disqualifies it from the *default* binary. The product's query set is the ~15 narrow
shapes in order-2 §2.3/§4 (counts by time bucket, top-N by tag, HLL uniques, t-digest
percentiles) — narrow enough that a focused vectorized Parquet scanner wins on both the
constraint and the dependency budget. DuckDB stays viable as an optional
contributed/CGO-tagged backend behind the storage interface (the escape hatch), never
default. **Pending measurement:** Parquet scan throughput with/without segment pruning,
to set the pruning-ratio assertions for §2.3. That spike runs before the event-store
milestone; I will not quote pruning numbers until it does.

## Three riskiest parts

1. **WAL crash-correctness under partial writes.** A torn tail on recovery must truncate
   to a clean prefix — never a hole, never a surviving corrupt record. Needs deterministic
   kill-at-every-offset simulation (order-2 §2.1). Get this wrong and it's silent
   corruption, the cardinal sin.
2. **The custom Parquet scanner + pruning.** The whole "replaces ClickHouse" claim rests
   on segment statistics + Bloom filters pruning correctly and measurably. Correctness of
   min/max stats and pruning-ratio assertions is where a subtle bug becomes wrong query
   results, not just slow ones.
3. **Backpressure keyed on log lag, flat memory under overload.** Must shed with `429`
   fairly across projects and keep memory flat at 10× load. Easy to accidentally buffer.

## Blocking question

Confirm scope for the next build step: implement **Milestone 2 (the WAL)** now — segments,
CRC, batched fsync, recovery + crash-simulation suite, consumer offsets, retention — as
`internal/wal`, wired behind an `EventLog` interface. The Parquet event store (M3) follows.
OK to proceed on the WAL?
