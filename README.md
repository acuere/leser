# leser

Open-source, self-hostable **error tracking & observability** — a Sentry
alternative that ships as one static binary. Licensed Apache-2.0.

> Status: **Milestone 1 (Skeleton)** complete. Not yet ingesting events.

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
docker pull ghcr.io/acuere/leser:latest      # or a pinned tag, e.g. :v0.0.1
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

> First tag must finish CI before these URLs resolve. The ghcr image is created
> private by default — make it public once in repo **Settings → Packages** for
> anonymous `docker pull`.

## Design (language, storage, concurrency)

**Language: Go.** The North Star is a 60-second path and a single static binary
with an embedded UI. Go's `embed.FS`, stdlib `net/http`, and `CGO_ENABLED=0`
static links serve that better than Rust's throughput edge. Pure-Go SQLite
(`modernc.org/sqlite`, Milestone 2) keeps builds CGO-free.

**Storage:** interface-first. Business logic depends on `MetadataStore` /
`EventStore`, never a driver. Default tier = embedded SQLite (WAL) + local
append-only event log. Scale tier = Postgres + ClickHouse, selected by
connection string, no business-logic change.

**Ingest concurrency:** a fixed-capacity channel is the backpressure primitive.
Full channel ⇒ immediate `429 + Retry-After`, never buffer to grow memory. A
fixed worker pool drains: parse → dedupe by `event_id` → fingerprint → store →
ack. Events are durably written before ack (crash-only; recovery-on-startup).

Riskiest parts: (1) grouping/fingerprinting, (2) Sentry wire-protocol
conformance, (3) type-level tenant isolation + build-time route-permission
enforcement.

## Layout

```
cmd/leser/          CLI + entrypoint (serve, version, config)
internal/config/    precedence: flags > env > file > defaults; secret redaction
internal/server/    HTTP server, /healthz /readyz, trace IDs, panic recovery
internal/logging/   slog JSON logger + trace context
internal/buildinfo/ version metadata (ldflags-stamped)
web/                embedded UI (go:embed all:dist)
```

## Make targets

`make build | test | vet | fmt | lint | run | dev | repro | tidy | clean`
