# Contributing to leser

## One-command dev environment

```sh
git clone https://github.com/acuere/leser && cd leser
make dev
```

`make dev` runs the server with hot reload if [`air`](https://github.com/air-verse/air)
is installed (`go install github.com/air-verse/air@latest`), falling back to
a plain `go run` otherwise. No Docker, no external services — the binary is
the whole system.

## Everyday commands

```sh
make build      # static binary, CGO_ENABLED=0
make test       # go test ./...
make vet        # go vet ./...
make fmt        # gofmt check (fails if unformatted; fix with gofmt -w .)
make lint       # fmt + vet
make repro      # build twice, assert byte-identical output
make conformance # real Python/Node/Go Sentry SDKs against your local build
```

`internal/wal`, `internal/eventstore`, and `internal/grouping` each carry
their own benchmarks and fuzz targets:

```sh
go test ./internal/wal -run=NONE -bench=. -benchtime=3s
go test ./internal/ingest -run=NONE -fuzz=FuzzParseEnvelope -fuzztime=30s
```

`robustness/*.sh` boot real (sometimes multiple) `leser` processes to verify
crash recovery, sustained load, and multi-process role/cluster behavior —
read a script before running it if you're curious what it actually checks;
each one prints a plain PASS/FAIL with the reasoning inline.

## Before opening a PR

- `make lint && make test` must pass locally.
- New behavior needs a test that would fail without the change — this
  project treats "I ran it once and it looked right" as insufficient
  (see the real-SDK and chaos suites in CI for what "verified" means here).
- If you're touching `internal/grouping`, the golden corpus
  (`internal/grouping/testdata/corpus.json`) will very likely need review:
  regenerate with `go test ./internal/grouping -run TestGoldenCorpus -update`
  and diff the result by hand — a silent change in what groups with what is
  exactly the failure mode that golden-file testing exists to catch.
- Every direct dependency needs a one-line justification in
  [`DEPENDENCIES.md`](DEPENDENCIES.md) before it lands in `go.mod`.
- Keep changes to a single logical concern per PR; this repo's history is
  full of commits that state what was verified and how — match that bar.

## Architecture context

Read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) first. It states plainly
what's built, what's stubbed, and what's explicitly out of scope — most
"why doesn't leser do X" questions are answered there before you write code.

## Reporting bugs / security issues

Open a GitHub issue for ordinary bugs. For anything security-sensitive,
do not open a public issue — see the repository's security policy (or, if
none is configured yet, contact a maintainer directly).
