#!/usr/bin/env bash
# Rung 3 real multi-process verification (order-2 §3): two separate leser
# processes gossip-join each other, compute consistent-hash project
# ownership, and each proves "any node can serve any API request by
# proxying to the owner" — a request for a project owned by the OTHER
# node still returns correct data when sent to the WRONG node's port.
#
# Topology: node A runs --role all (ingest+worker+query — the sole WAL
# writer), node B runs --role query only. This is deliberate, not
# incidental: Rung 3's routing layer does NOT provide per-shard data
# isolation (see docs/ARCHITECTURE.md) — every clustered node still shares
# one --data-dir (Rung 2's model), and the WAL has exactly one legal writer
# for that directory. Running --role all on TWO clustered nodes sharing a
# --data-dir would mean two independent WAL writers racing on one directory
# — genuinely unsafe, and explicitly not what this proves. A query-only node
# never opens the WAL for writing, so it's always safe to scale out.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d)"
data="$work/data" # shared --data-dir, same as a Rung 2 deployment
a_port="${RUNG3_A_PORT:-8250}"
b_port="${RUNG3_B_PORT:-8251}"
a_gossip="${RUNG3_A_GOSSIP:-8350}"
b_gossip="${RUNG3_B_GOSSIP:-8351}"
trap 'kill $ap $bp 2>/dev/null || true; wait $ap $bp 2>/dev/null; rm -rf "$work"' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

log "building leser"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser" ./cmd/leser)

wait_ready() {
  for i in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  die "node on :$1 never became ready"
}

log "starting node A (role=all, clustered, first up so it bootstraps)"
"$work/leser" serve --data-dir "$data" --listen "127.0.0.1:$a_port" --log-level warn \
  --cluster-node-id node-a --cluster-api-addr "http://127.0.0.1:$a_port" \
  --cluster-bind 127.0.0.1 --cluster-port "$a_gossip" \
  > "$work/a.out" 2>&1 &
ap=$!
wait_ready "$a_port"

log "starting node B (role=query only, clustered, joins A — never opens the WAL for writing)"
"$work/leser" serve --role query --data-dir "$data" --listen "127.0.0.1:$b_port" --log-level warn \
  --cluster-node-id node-b --cluster-api-addr "http://127.0.0.1:$b_port" \
  --cluster-bind 127.0.0.1 --cluster-port "$b_gossip" \
  --cluster-join "127.0.0.1:$a_gossip" \
  > "$work/b.out" 2>&1 &
bp=$!
wait_ready "$b_port"

log "waiting for gossip convergence"
sleep 2

dsn_key="$(grep 'DSN:' "$work/a.out" | sed 's|.*http://||;s|@.*||')"
[ -n "$dsn_key" ] || die "node A printed no DSN"

log "sending an event to node A for project 1"
event_id="rung3deadbeefdeadbeefdeadbeef01"
resp="$(curl -fsS -X POST "http://127.0.0.1:$a_port/api/1/envelope/?sentry_key=$dsn_key" \
  --data-binary "{\"event_id\":\"$event_id\"}
{\"type\":\"event\"}
{\"event_id\":\"$event_id\",\"level\":\"error\",\"exception\":{\"values\":[{\"type\":\"Rung3Error\",\"value\":\"proxied\"}]}}")"
echo "$resp" | grep -q '"id"' || die "ingest rejected the event: $resp"
sleep 3 # consumer + flush cadence

admin_pw="$(grep 'Admin:' "$work/a.out" | awk '{print $4}')"
[ -n "$admin_pw" ] || die "node A printed no admin credentials"

# Log in on WHICHEVER node happens to own project 1's session; sessions are
# in the shared metadata store, valid on both nodes.
curl -fsS -c "$work/ck" -X POST "http://127.0.0.1:$a_port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null

# The core Rung 3 claim: query project 1 through BOTH nodes' ports and get
# correct data from both. That alone is NOT sufficient proof of proxying —
# both nodes share one metadata store in this MVP (see script header), so
# either node could trivially answer locally with no routing at all. The
# X-Leser-Node response header (stamped only by whichever node handles a
# request LOCALLY; never touched when a node proxies) is what actually
# proves the request crossed a process boundary.
found_a=0; found_b=0
owner_via_a=""; owner_via_b=""
for i in $(seq 1 10); do
  ha="$(curl -fsS -D - -o "$work/body_a" -b "$work/ck" "http://127.0.0.1:$a_port/api/0/projects/1/issues" || true)"
  hb="$(curl -fsS -D - -o "$work/body_b" -b "$work/ck" "http://127.0.0.1:$b_port/api/0/projects/1/issues" || true)"
  grep -q 'Rung3Error' "$work/body_a" && found_a=1
  grep -q 'Rung3Error' "$work/body_b" && found_b=1
  owner_via_a="$(echo "$ha" | grep -i '^X-Leser-Node:' | tr -d '\r' | awk '{print $2}')"
  owner_via_b="$(echo "$hb" | grep -i '^X-Leser-Node:' | tr -d '\r' | awk '{print $2}')"
  [ "$found_a" = 1 ] && [ "$found_b" = 1 ] && [ -n "$owner_via_a" ] && [ -n "$owner_via_b" ] && break
  sleep 1
done
[ "$found_a" = 1 ] || die "project 1 issue not visible via node A's port"
[ "$found_b" = 1 ] || die "project 1 issue not visible via node B's port"
[ -n "$owner_via_a" ] || die "no X-Leser-Node header via node A's port"
[ -n "$owner_via_b" ] || die "no X-Leser-Node header via node B's port"

log "owner as seen through node A's port: $owner_via_a; through node B's port: $owner_via_b"
[ "$owner_via_a" = "$owner_via_b" ] || die "the two ports disagree on who actually owns project 1: $owner_via_a vs $owner_via_b"

# This is the actual proof: whichever node ISN'T the real owner must have
# proxied — its own port returned the OTHER node's stamp, not its own.
if [ "$owner_via_a" != "node-a" ] && [ "$owner_via_b" != "node-a" ]; then
  die "neither port's response was ever stamped node-a — node A never handled anything"
fi
if [ "$owner_via_a" = "$owner_via_b" ] && { [ "$owner_via_a" = "node-a" ] || [ "$owner_via_a" = "node-b" ]; }; then
  log "confirmed: project 1 is owned by $owner_via_a; the other node's port genuinely proxied the request there"
else
  die "unexpected ownership stamps: via-a=$owner_via_a via-b=$owner_via_b"
fi

log "Rung 3 PASS: 2 gossip-joined processes, consistent-hash routing, project 1 queryable through BOTH nodes' ports, proxy path verified via ownership stamp"
