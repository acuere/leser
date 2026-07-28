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

## Backup / restore, TLS, scaling

- **Backup:** stop or snapshot the `data/` directory (SQLite WAL-safe backup lands with the store layer in Milestone 2). `leser backup create` arrives in Milestone 7.
- **TLS:** terminate TLS at a reverse proxy (Caddy / nginx / Traefik) in front of `:8080`. Native TLS is a later addition.
- **Scale tier:** point `LESER_METADATA_DSN` at Postgres and `LESER_EVENTS_DSN` at ClickHouse (Milestone 8) — same binary, no code change.

> Status: Milestone 1 (skeleton) is live. Ingest, storage, and the admin
> bootstrap that prints your first DSN land in Milestone 2.
