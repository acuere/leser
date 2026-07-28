#!/usr/bin/env bash
# SDK conformance suite (order.md §3): run the REAL Sentry SDKs for Python,
# Node, and Go against leser's ingest — changed DSN only — and assert the
# events were stored and grouped correctly via the public API.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
port="${LESER_CONFORMANCE_PORT:-8199}"
work="$(mktemp -d)"
trap 'kill $srv_pid 2>/dev/null || true; rm -rf "$work"' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

# --- build + boot ---
log "building leser"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser" ./cmd/leser)

log "booting on :$port"
"$work/leser" serve --data-dir "$work/data" --listen "127.0.0.1:$port" --log-level warn \
  > "$work/serve.out" 2>&1 &
srv_pid=$!

for i in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1 && break
  sleep 0.2
  [ "$i" = 50 ] && die "server never became ready"
done

dsn_host="$(grep 'DSN:' "$work/serve.out" | awk '{print $2}' | sed "s|@localhost:8080|@127.0.0.1:$port|")"
admin_pw="$(grep 'Admin:' "$work/serve.out" | awk '{print $4}')"
[ -n "$dsn_host" ] || die "no DSN printed"
log "DSN: $dsn_host"

# --- python: real sentry-sdk ---
log "python: installing sentry-sdk into venv"
python3 -m venv "$work/venv" >/dev/null
"$work/venv/bin/pip" install --quiet sentry-sdk
log "python: sending"
"$work/venv/bin/python" "$here/send_event.py" "$dsn_host"

# --- node: real @sentry/node ---
log "node: installing @sentry/node"
mkdir -p "$work/node" && cp "$here/send_event.mjs" "$work/node/"
(cd "$work/node" && npm init -y >/dev/null 2>&1 && npm install --no-audit --no-fund --loglevel=error @sentry/node >/dev/null)
log "node: sending"
(cd "$work/node" && node send_event.mjs "$dsn_host")

# --- go: real sentry-go ---
log "go: building sender with sentry-go"
cp -r "$here/gosender" "$work/gosender"
(cd "$work/gosender" && go mod init conformance-gosender >/dev/null 2>&1 && \
  go get github.com/getsentry/sentry-go@latest >/dev/null 2>&1 && \
  go build -o sender . )
log "go: sending"
"$work/gosender/sender" "$dsn_host"

# --- assert via the public API ---
log "asserting stored events via API"
sleep 3  # consumer drains WAL

curl -fsS -c "$work/cookies" -X POST "http://127.0.0.1:$port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null

issues="$(curl -fsS -b "$work/cookies" "http://127.0.0.1:$port/api/0/projects/1/issues")"
status="$(curl -fsS -b "$work/cookies" "http://127.0.0.1:$port/api/0/ops/status")"

echo "$issues" | grep -q 'KeyError'   || die "python event not grouped (no KeyError issue): $issues"
echo "$issues" | grep -q 'TypeError'  || die "node event not grouped (no TypeError issue): $issues"
echo "$issues" | grep -q 'invoice'    || die "go event not grouped (no invoice issue): $issues"

count="$(echo "$issues" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"
[ "$count" -eq 3 ] || die "expected exactly 3 issues, got $count"

stored="$(echo "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["events_stored_total"])')"
[ "$stored" -ge 3 ] || die "stored=$stored, want >=3"
drops="$(echo "$status" | python3 -c 'import json,sys; d=json.load(sys.stdin)["events_dropped_total"]; print(sum(d.values()))')"
[ "$drops" -eq 0 ] || die "events dropped during conformance: $status"

# release/environment survived the wire
echo "$issues" >/dev/null
events="$(curl -fsS -b "$work/cookies" "http://127.0.0.1:$port/api/0/projects/1/stats?since=1")"
echo "$events" | grep -q 'conformance-py-1.0' || die "python release missing from aggregates: $events"

log "conformance PASS: python, node, go SDKs all stored + grouped (3 issues, 0 drops)"
