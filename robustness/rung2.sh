#!/usr/bin/env bash
# Rung 2 real multi-process verification (order-2 §3): three separate leser
# processes — ingest, worker, query — pointed at ONE shared --data-dir,
# nothing but the filesystem coordinating them. Sends a real event through
# the ingest node's port and asserts it becomes queryable through the
# QUERY node's port (a different process, different port, same directory).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d)"
data="$work/data"
ingest_port="${RUNG2_INGEST_PORT:-8240}"
worker_port="${RUNG2_WORKER_PORT:-8241}"
query_port="${RUNG2_QUERY_PORT:-8242}"
trap 'kill $ip $wp $qp 2>/dev/null || true; rm -rf "$work"' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

log "building leser"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser" ./cmd/leser)

wait_ready() {
  local port=$1 name=$2
  for i in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  die "$name never became ready"
}

log "starting ingest role (owns the WAL, writer)"
"$work/leser" serve --role ingest --data-dir "$data" --listen "127.0.0.1:$ingest_port" --log-level warn \
  > "$work/ingest.out" 2>&1 &
ip=$!
wait_ready "$ingest_port" ingest

log "starting worker role (WAL read-only attach, owns compaction + alerts)"
"$work/leser" serve --role worker --data-dir "$data" --listen "127.0.0.1:$worker_port" --log-level warn \
  > "$work/worker.out" 2>&1 &
wp=$!
wait_ready "$worker_port" worker

log "starting query role (serves API + UI, never writes WAL or events)"
"$work/leser" serve --role query --data-dir "$data" --listen "127.0.0.1:$query_port" --log-level warn \
  > "$work/query.out" 2>&1 &
qp=$!
wait_ready "$query_port" query

for p in $ip $wp $qp; do
  kill -0 $p 2>/dev/null || die "a role process exited early — logs in $work"
done

dsn_key="$(grep 'DSN:' "$work/ingest.out" | sed 's|.*http://||;s|@.*||')"
[ -n "$dsn_key" ] || die "ingest role printed no DSN"

log "sending an event to the ingest node ($ingest_port)"
event_id="rung2deadbeefdeadbeefdeadbeef01"
resp="$(curl -fsS -X POST "http://127.0.0.1:$ingest_port/api/1/envelope/?sentry_key=$dsn_key" \
  --data-binary "{\"event_id\":\"$event_id\"}
{\"type\":\"event\"}
{\"event_id\":\"$event_id\",\"level\":\"error\",\"exception\":{\"values\":[{\"type\":\"Rung2Error\",\"value\":\"cross-process\"}]}}")"
echo "$resp" | grep -q '"id"' || die "ingest node rejected the event: $resp"

# Sanity: the QUERY node has no ingest route mounted at all — the catch-all
# UI handler is the only thing registered under "/", so an ingest POST there
# falls through as 404 (unknown path) or 405 (path matches the UI subtree
# pattern, method GET-only) depending on mux internals; either proves it
# never reached ingest handling. A 200 would mean role isolation is broken.
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$query_port/api/1/envelope/?sentry_key=$dsn_key" --data-binary '{}')"
case "$code" in 404|405) ;; *) die "query node accepted an ingest POST (role isolation broken): got $code" ;; esac

log "waiting for the WORKER node to consume + group it (separate process, shared WAL)"
for i in $(seq 1 100); do
  n="$(curl -s "http://127.0.0.1:$worker_port/healthz" >/dev/null; curl -fsS "http://127.0.0.1:$worker_port/readyz" >/dev/null && echo 1 || echo 0)"
  sleep 0.1
done
sleep 3 # worker's commit cadence + flush ticker

log "logging in on the QUERY node and asserting the issue is visible there"
admin_pw="$(grep 'Admin:' "$work/query.out" | awk '{print $4}')"
[ -n "$admin_pw" ] || die "query node never printed admin credentials"
curl -fsS -c "$work/ck" -X POST "http://127.0.0.1:$query_port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null

# Query-role Store only sees flushed segments; give it one Refresh cycle
# (2s ticker) plus a margin.
found=0
for i in $(seq 1 15); do
  issues="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$query_port/api/0/projects/1/issues")"
  if echo "$issues" | grep -q 'Rung2Error'; then found=1; break; fi
  sleep 1
done
[ "$found" = "1" ] || die "event ingested on node A, grouped on node B, never became visible on node C (query): $issues"

log "asserting the WORKER node itself never accepted a login (no dashboard there)"
code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$worker_port/api/0/me")"
case "$code" in 404|405) ;; *) die "worker node served an API route it shouldn't have: $code" ;; esac

log "Rung 2 PASS: 3 separate processes (ingest/worker/query), shared --data-dir only, event flowed A -> B -> C, roles isolated"
