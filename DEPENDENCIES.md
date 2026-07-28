# Dependencies

Policy (per HARD CONSTRAINTS §5): every **direct** dependency carries a one-line
justification. Reject anything pulling a large transitive graph. Prefer stdlib.

## Runtime — direct

| Module | Why | Milestone added |
|--------|-----|-----------------|
| _(none)_ | Milestone 1 is 100% Go stdlib: `net/http`, `log/slog`, `embed`, `crypto/rand`, `encoding/json`, `flag`. | — |

## Planned (justified when introduced)

| Module | Why | Milestone |
|--------|-----|-----------|
| `modernc.org/sqlite` | Pure-Go SQLite (no CGO) so `CGO_ENABLED=0` static builds hold. The only realistic zero-CGO embedded SQL option. | 2 (Ingest, SQLite storage) |

Anything not in stdlib gets a row here **before** it enters `go.mod`.
