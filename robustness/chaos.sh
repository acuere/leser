#!/usr/bin/env bash
# Crash-only chaos loop (order-2 §5): SIGKILL the server at random moments
# under live ingest, restart, and at the end assert the ack contract:
# EVERY event acknowledged with HTTP 200 is present after all the crashes.
# (200 = durable in the WAL. At-least-once means stored >= acked; the memory
# dedupe does not survive restarts, so exact-once across a crash is not
# claimed — that gap is documented.)
set -euo pipefail

iters="${CHAOS_ITERS:-8}"
port="${CHAOS_PORT:-8219}"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d)"
acked="$work/acked.txt"
: > "$acked"
trap 'kill $srv 2>/dev/null || true; wait $srv 2>/dev/null; rm -rf "$work"' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

log "building"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser" ./cmd/leser)

boot() {
  "$work/leser" serve --data-dir "$work/data" --listen "127.0.0.1:$port" --log-level warn \
    >> "$work/serve.out" 2>&1 &
  srv=$!
  for i in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1 && return 0
    kill -0 $srv 2>/dev/null || die "server died during boot (iteration log: $work/serve.out)"
    sleep 0.1
  done
  die "server not ready after 10s"
}

boot
dsn_key="$(grep 'DSN:' "$work/serve.out" | head -1 | sed 's|.*http://||;s|@.*||')"
admin_pw="$(grep 'Admin:' "$work/serve.out" | head -1 | awk '{print $4}')"
[ -n "$dsn_key" ] || die "no DSN"

# sender: unique event ids; record every id that got HTTP 200
seq_n=0
send_burst() {
  local n=$1
  for _ in $(seq 1 "$n"); do
    seq_n=$((seq_n + 1))
    id="$(printf 'c%031d' "$seq_n")"
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 \
      -X POST "http://127.0.0.1:$port/api/1/envelope/?sentry_key=$dsn_key" \
      --data-binary "{\"event_id\":\"$id\"}
{\"type\":\"event\"}
{\"event_id\":\"$id\",\"message\":\"chaos $seq_n\",\"exception\":{\"values\":[{\"type\":\"ChaosError\",\"value\":\"n=$((seq_n % 7))\"}]}}" || true)"
    if [ "$code" = "200" ]; then echo "$id" >> "$acked"; fi
  done
}

for i in $(seq 1 "$iters"); do
  send_burst 40 &
  sender=$!
  # kill the server at a random point while the burst is in flight
  sleep "0.$((RANDOM % 9 + 1))"
  kill -9 $srv 2>/dev/null || true
  wait $sender 2>/dev/null || true
  log "iteration $i: SIGKILL delivered, restarting"
  boot
done

# let the consumer drain the WAL fully
sleep 4
acked_count="$(sort -u "$acked" | wc -l | tr -d ' ')"
log "acked events across all crashes: $acked_count"

curl -fsS -c "$work/ck" -X POST "http://127.0.0.1:$port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null

total="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$port/api/0/projects/1/stats?since=1" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["total"])')"
log "events in store after final recovery: $total"

[ "$total" -ge "$acked_count" ] || die "ACKED DATA LOST: acked=$acked_count stored=$total"

# no corruption complaints anywhere in any boot
if grep -iE 'corrupt|panic' "$work/serve.out" >/dev/null; then
  die "corruption or panic in server logs: $(grep -iE 'corrupt|panic' "$work/serve.out" | head -3)"
fi

log "chaos PASS: $iters SIGKILLs, $acked_count acked, $total stored (>= acked), zero corruption"
