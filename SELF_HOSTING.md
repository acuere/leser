# Self-hosting leser

leser is a single static binary. No external services required for the default
(solo / small-team) tier.

## Install

### curl | sh (Linux / macOS)

```sh
curl -fsSL https://raw.githubusercontent.com/acuere/leser/main/scripts/install.sh | sh
leser serve
```

Pin a version or install dir:

```sh
LESER_VERSION=v0.1.0 LESER_INSTALL="$HOME/.local/bin" \
  sh -c 'curl -fsSL https://raw.githubusercontent.com/acuere/leser/main/scripts/install.sh | sh'
```

### Docker

```sh
docker run -p 8080:8080 -v "$PWD/data:/data" ghcr.io/acuere/leser:latest
```

That single command is a complete production deployment for a small team: SQLite
metadata + local event store live under `/data`.

### From source

```sh
git clone https://github.com/acuere/leser && cd leser
make build && ./leser serve
```

## Configuration

Precedence: **flags > env vars > config file > defaults**. Every option has an
env var.

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--listen` | `LESER_LISTEN` | `:8080` | bind address |
| `--data-dir` | `LESER_DATA_DIR` | `./data` | SQLite + event store dir |
| `--log-level` | `LESER_LOG_LEVEL` | `info` | debug\|info\|warn\|error |
| — | `LESER_PUBLIC_URL` | `http://localhost:8080` | base URL for DSNs |
| — | `LESER_SECRET_KEY` | _(auto)_ | session/token signing key |

Inspect the resolved config (secrets redacted):

```sh
leser config show --effective --json
```

## Health & readiness

- `GET /healthz` — process liveness. Never touches dependencies.
- `GET /readyz` — dependencies reachable + migrations applied. Gate load balancers on this.

## Backup / restore, TLS

- **Backup:** stop the process (or snapshot a filesystem-consistent copy) and
  copy the entire `data/` directory — it is the whole system: SQLite
  metadata, the WAL, and Parquet event segments. `leser backup create` is
  planned but not yet implemented; a directory copy is safe today because
  SQLite is in WAL mode and every other format here is crash-consistent by
  design (see [ARCHITECTURE.md](docs/ARCHITECTURE.md#crash-only)).
- **TLS:** terminate TLS at a reverse proxy (Caddy / nginx / Traefik) in
  front of the listen address. Native TLS (embedded ACME) is planned.

## Scaling beyond one process (Rung 2)

leser never requires an external service, at any scale — see
[ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full ladder and the honest
tradeoffs at each rung. The first rung past a single process:

```sh
# three processes, one shared --data-dir (NFS-class storage or a single
# storage node — this is the actual requirement, not a nice-to-have)
leser serve --role ingest --data-dir /shared/leser --listen :8080
leser serve --role worker --data-dir /shared/leser --listen :8081
leser serve --role query  --data-dir /shared/leser --listen :8082
```

Point client DSNs and your load balancer at the `ingest` role's address(es);
point operators at `query`. `worker` needs no inbound traffic — it only
consumes. Climb to this only when read load is disproportionate to write
load; it buys CPU isolation between ingest/compaction/query, not lower
latency (a `query` node lags the `worker` by its poll interval plus flush
age — full numbers in ARCHITECTURE.md).
