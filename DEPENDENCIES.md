# Dependencies

Policy (per HARD CONSTRAINTS §5): every **direct** dependency carries a one-line
justification. Reject anything pulling a large transitive graph. Prefer stdlib.

## Runtime — direct

| Module | Why | Milestone added |
|--------|-----|-----------------|
| `github.com/parquet-go/parquet-go` | The pure-Go Parquet implementation (no CGO): columnar segments with native column statistics + Bloom filters, and cold-tier files any external tool can read. Hand-rolling Parquet was evaluated and rejected as a multi-month correctness risk. Its `go.mod` lists test-only modules (sqlmock, lib/pq, kml) that do not link into our binary. | order-2 M3 (event store) |
| `modernc.org/sqlite` | Pure-Go SQLite (no CGO) so `CGO_ENABLED=0` static builds hold. The only realistic zero-CGO embedded SQL option. Large transitive graph is the accepted cost of not writing a SQL engine. | order.md M2 (metadata store) |
| `github.com/klauspost/compress` | zstd decoding for the Sentry `Content-Encoding: zstd` ingest path (stdlib has no zstd). Already in the tree transitively via parquet-go; promoting to direct adds zero new code. | order.md M2 (ingest) |
| `golang.org/x/crypto` | Argon2id password hashing (order.md §6 mandate). x/ repos are stdlib-adjacent, Go-team maintained. | order.md M4 (auth) |

Everything else in `go.mod` is `// indirect` — pulled by the four above, not
imported by leser code.

Anything not in stdlib gets a row here **before** it enters `go.mod`.
