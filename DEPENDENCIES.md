# Dependencies

Policy (per HARD CONSTRAINTS §5): every **direct** dependency carries a one-line
justification. Reject anything pulling a large transitive graph. Prefer stdlib.

## Runtime — direct

| Module | Why | Milestone added |
|--------|-----|-----------------|
| `github.com/parquet-go/parquet-go` | The pure-Go Parquet implementation (no CGO): columnar segments with native column statistics + Bloom filters, and cold-tier files any external tool can read. Hand-rolling Parquet was evaluated and rejected as a multi-month correctness risk. Its `go.mod` lists test-only modules (sqlmock, lib/pq, kml) that do not link into our binary. | order-2 M3 (event store) |

## Planned (justified when introduced)

| Module | Why | Milestone |
|--------|-----|-----------|
| `modernc.org/sqlite` | Pure-Go SQLite (no CGO) so `CGO_ENABLED=0` static builds hold. The only realistic zero-CGO embedded SQL option. | 2 (Ingest, SQLite storage) |

Anything not in stdlib gets a row here **before** it enters `go.mod`.
