#!/usr/bin/env bash
# Sustained load test with gates (order-2 §6): run ingest at 2x the per-project
# quota, assert p99 bounded on accepted requests, flat server memory, clean
# 429 shedding, and zero silent loss (stored == accepted after drain).
set -euo pipefail

port="${LOAD_PORT:-8229}"
duration="${LOAD_DURATION:-20s}"
conc="${LOAD_CONC:-64}"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d)"
trap 'kill $srv 2>/dev/null || true; wait $srv 2>/dev/null; rm -rf "$work"' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

log "building leser + loadgen"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser" ./cmd/leser)
(cd "$root/bench/loadgen" && go build -o "$work/loadgen" .)

"$work/leser" serve --data-dir "$work/data" --listen "127.0.0.1:$port" --log-level warn \
  > "$work/serve.out" 2>&1 &
srv=$!
for i in $(seq 1 50); do
  curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1 && break
  sleep 0.2
done

key="$(grep 'DSN:' "$work/serve.out" | sed 's|.*http://||;s|@.*||')"
admin_pw="$(grep 'Admin:' "$work/serve.out" | awk '{print $4}')"

log "running load: conc=$conc duration=$duration (server pid $srv)"
# The gate is BOUNDED p99, not laptop-fast p99: shared CI runners are 2-core
# and noisy, so the ceiling is configurable (LOAD_MAX_P99, default 250ms).
"$work/loadgen" -url "http://127.0.0.1:$port" -key "$key" -project 1 \
  -conc "$conc" -duration "$duration" -pid "$srv" \
  -max-p99 "${LOAD_MAX_P99:-250ms}" | tee "$work/load.out"

accepted="$(grep '^accepted:' "$work/load.out" | awk '{print $2}')"

log "draining consumer, then asserting zero silent loss"
sleep 5
curl -fsS -c "$work/ck" -X POST "http://127.0.0.1:$port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null
status="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$port/api/0/ops/status")"
stored="$(echo "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["events_stored_total"])')"
lag="$(echo "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["wal_consumer_lag"])')"

# allow the tail to finish if lag remains
if [ "$lag" -gt 0 ]; then sleep 10; status="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$port/api/0/ops/status")"; stored="$(echo "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["events_stored_total"])')"; fi

log "accepted=$accepted stored=$stored"
[ "$stored" -ge "$accepted" ] || die "SILENT LOSS: accepted=$accepted stored=$stored ($status)"

log "load PASS end-to-end: every accepted event stored"
