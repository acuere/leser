# leser

Open-source, self-hostable **error tracking & observability** — a Sentry
alternative that ships as one static binary. Licensed Apache-2.0.

No Kafka. No Redis. No ClickHouse. No broker, no worker fleet, no nine-service
Compose file. One process, one data directory — by design, at every scale
([architecture & honest tradeoffs](docs/ARCHITECTURE.md)).

> Status: **v0.1.3.** Sentry-compatible ingest — point any official Sentry
> SDK's DSN at it, unmodified (verified against the real Python, Node, and Go
> SDKs, in CI). Deterministic grouping with a golden corpus. Issue lifecycle
> (resolve / regress / merge / split). A real search query language
> (`is:unresolved level:error release:1.2.3`) with keyboard-first navigation.
> Alerting with webhook delivery, retries, and circuit breakers. RBAC with
> build-enforced route permissions, an adversarial cross-tenant test suite,
> and an append-only audit log. A control-plane dashboard embedded in the
> binary. Optional horizontal scaling: `--role=ingest|worker|query` process
> separation, and a consistent-hash + embedded-gossip request-routing layer
> — both verified with real, separate OS processes, not just unit tests.

**60 seconds to first stack trace:** run it, copy the DSN it prints into your
app's Sentry SDK config, trigger an error, open the UI.

## Install

```sh
# curl | sh (Linux/macOS)
curl -fsSL https://raw.githubusercontent.com/acuere/leser/main/scripts/install.sh | sh

# Docker — complete small-team deployment
docker run -p 8080:8080 -v "$PWD/data:/data" ghcr.io/acuere/leser:latest

# from source
make build && ./leser serve
```

Then open http://localhost:8080 (`/healthz`, `/readyz`, `/version`, embedded UI at `/`).
Full guide: [SELF_HOSTING.md](SELF_HOSTING.md).

## Packages & releases

Every `v*` tag runs `.github/workflows/release.yml`, producing:

**Container image** (GitHub Packages / ghcr):

```sh
docker pull ghcr.io/acuere/leser:latest      # or a pinned tag, e.g. :v0.1.3
docker run -p 8080:8080 -v "$PWD/data:/data" ghcr.io/acuere/leser:latest
```

Multi-arch: `linux/amd64`, `linux/arm64`.

**Release artifacts** (attached to each [GitHub Release](https://github.com/acuere/leser/releases)):

| Artifact | What |
|----------|------|
| `leser_linux_amd64`, `leser_linux_arm64` | static Linux binaries |
| `leser_darwin_amd64`, `leser_darwin_arm64` | macOS binaries |
| `checksums.txt` | SHA-256 of every binary (installer verifies against it) |
| `sbom.json` | CycloneDX SBOM |

> The ghcr image is private by default — make it public once in repo
> **Settings → Packages** for anonymous `docker pull`.

## Design

**Language: Go.** The North Star is a 60-second path and a single static
binary with an embedded UI. `embed.FS`, stdlib `net/http`, and
`CGO_ENABLED=0` static links serve that better than Rust's throughput edge.

**No required external services, at any scale** — the actual product, not a
tagline. Every piece a distributed error tracker usually farms out to Kafka,
Redis, ClickHouse, or Celery is instead an in-process module reading and
writing local files:

| Usually | Here |
|---|---|
| Kafka | `internal/wal` — segmented append-only log, batched-fsync group commit, crash-only recovery (tested by killing the process mid-write at every byte offset) |
| Postgres | `internal/metadata` — embedded SQLite, WAL mode, structural single-writer |
| ClickHouse | `internal/eventstore` — Parquet segments partitioned by (project, hour), column stats + Bloom filters + HyperLogLog sketches; a test *fails the build* if a query opens a segment its own statistics could have pruned |
| Celery + broker | `internal/alerts` — bounded worker pool, retries with full jitter, per-endpoint circuit breakers |
| Redis | `internal/cache` (byte-bounded LRU) + `internal/ratelimit` (per-project token buckets, checkpointed to SQLite) |

Full numbers and the honest tradeoffs of that choice: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

**Ingest → grouping → storage, in order:** the Sentry envelope protocol
(`internal/ingest`) durably appends to the WAL and acks — acknowledged data
is never dropped afterward, only backpressured *before* accepting it
(`429 + Retry-After`, keyed on real WAL consumer lag, fair per project). A
consumer parses, deduplicates, and runs the grouping engine
(`internal/grouping` — a pure, deterministic `(event) -> fingerprint`
function locked by a golden corpus across 9 platforms) before writing to the
event store and updating issue state (`internal/metadata`).

**Optional horizontal scaling, verified with real processes, not mocks:**
`--role=ingest|worker|query` splits one process into three over a shared
data directory (`robustness/rung2.sh`); `--cluster-*` flags add embedded
gossip membership (`hashicorp/memberlist`) and consistent-hash request
routing, so a request landing on the wrong node gets proxied to the one that
owns the project — proven with two separate gossiping processes and a
response header that only the *actual* handling node stamps
(`robustness/rung3.sh`). Neither is required; both are opt-in.

## Verification, not just unit tests

Every push runs three CI jobs, each doing something a mocked test can't:

- **`conformance/`** — the real `sentry-sdk`, `@sentry/node`, and `sentry-go`
  packages (via pip/npm/go get, unmodified) send events through the actual
  ingest endpoint; the suite asserts they're stored and grouped correctly. It
  found a real protocol gap on its first run (sentry-go's exception shape).
- **`robustness/`** — `chaos.sh` SIGKILLs the live server at random points
  under load and asserts every acknowledged event survives; `load.sh` runs
  sustained overload and gates on bounded p99, flat memory, and zero silent
  loss; `rung2.sh`/`rung3.sh` boot 2-3 real separate OS processes and prove
  the scaling claims above actually hold across process boundaries.
- **Fuzzing** — the envelope parser (`internal/ingest`) runs under
  `go test -fuzz` in CI; any finding blocks release.

## Layout

```
cmd/leser/          CLI + entrypoint (serve, version, config, user, token, send-test-event)
internal/wal/        segmented append-only log — the ingest spine
internal/eventstore/ Parquet event store — hot buffer, compaction, pruning, aggregates, HLL
internal/metadata/   embedded SQLite — orgs/projects/users/issues/alerts/audit, migrations
internal/grouping/   deterministic fingerprinting engine + golden corpus
internal/ingest/     Sentry envelope protocol, HTTP handler, WAL->store pipeline
internal/search/     issue-stream query language
internal/authz/      permissions, roles, scopes
internal/auth/       Argon2id passwords, sessions, API tokens
internal/api/        the authenticated REST API — one declarative route table
internal/alerts/     alert rules engine, webhook delivery, circuit breakers
internal/cluster/    consistent-hash ring + embedded gossip membership (Rung 3)
internal/cache/      byte-bounded LRU
internal/ratelimit/  per-project token buckets
internal/server/     HTTP server, health endpoints, trace IDs, panic recovery
web/                 embedded control-plane UI (go:embed all:dist)
conformance/         real-SDK conformance suite
robustness/          chaos, load, and multi-process scaling verification
bench/               WAL and load-generation benchmarks
docs/ARCHITECTURE.md full system design + honest tradeoffs
```

## Make targets

`make build | test | vet | fmt | lint | run | dev | repro | conformance | tidy | clean`
