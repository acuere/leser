# leser — build automation. Single static binary; reproducible builds.

BIN        := leser
PKG        := ./cmd/leser
VERSION    ?= dev
COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS    := -s -w -buildid= \
              -X leser/internal/buildinfo.Version=$(VERSION) \
              -X leser/internal/buildinfo.Commit=$(COMMIT)
GOFLAGS    := -trimpath

export CGO_ENABLED = 0

.PHONY: all build test vet fmt lint run dev repro clean tidy

all: vet test build

## build: compile the static binary
build:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o $(BIN) $(PKG)

## test: run all tests with race detector off (CGO-free) + coverage
test:
	go test ./...

## vet: static analysis
vet:
	go vet ./...

## fmt: format check (fails if unformatted)
fmt:
	@test -z "$$(gofmt -l . | tee /dev/stderr)" || (echo "run: gofmt -w ."; exit 1)

## lint: fmt + vet
lint: fmt vet

## run: build and boot the server
run: build
	./$(BIN) serve

## dev: hot-reload dev loop (falls back to a plain run if 'air' is absent)
dev:
	@command -v air >/dev/null 2>&1 && air || ( \
		echo "note: 'air' not installed; running without hot reload"; \
		go run $(PKG) serve )

## repro: build twice and assert byte-identical output
repro:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o /tmp/$(BIN)-a $(PKG)
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o /tmp/$(BIN)-b $(PKG)
	@a=$$(shasum -a 256 /tmp/$(BIN)-a | awk '{print $$1}'); \
	 b=$$(shasum -a 256 /tmp/$(BIN)-b | awk '{print $$1}'); \
	 echo "A=$$a"; echo "B=$$b"; \
	 [ "$$a" = "$$b" ] && echo "reproducible: OK" || (echo "MISMATCH"; exit 1)

## tidy: sync go.mod
tidy:
	go mod tidy

clean:
	rm -f $(BIN) /tmp/$(BIN)-a /tmp/$(BIN)-b

## conformance: run real Sentry SDKs against ingest
conformance:
	./conformance/run.sh

## upgrade: verify the previous git tag's data survives under the current build
upgrade:
	./robustness/upgrade.sh
