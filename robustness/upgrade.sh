#!/usr/bin/env bash
# Upgrade test (order.md §8): boot the PREVIOUS released version, write real
# data through it, stop it cleanly, boot the CURRENT working tree's binary
# against the SAME --data-dir, and assert the old data is fully readable —
# migrations applied, issue grouped by the old binary still queryable, no
# data loss across the version boundary. Uses an actual git tag, built from
# source via a worktree — not a simulated "pretend this is old" version.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(dirname "$here")"
work="$(mktemp -d)"
data="$work/data"
port="${UPGRADE_PORT:-8260}"
trap 'kill $pid 2>/dev/null || true; rm -rf "$work" "$oldwt" 2>/dev/null || true' EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mFAIL:\033[0m %s\n' "$1"; exit 1; }

prev_tag="$(cd "$root" && git tag --sort=-creatordate | sed -n '1p')"
[ -n "$prev_tag" ] || die "no git tags found — nothing to upgrade from"
log "previous release under test: $prev_tag"

oldwt="$(mktemp -d)"
log "checking out $prev_tag into a worktree and building it"
(cd "$root" && git worktree add --detach "$oldwt" "$prev_tag" >/dev/null 2>&1) \
  || die "git worktree add failed for $prev_tag"
(cd "$oldwt" && CGO_ENABLED=0 go build -o "$work/leser-old" ./cmd/leser) \
  || die "building $prev_tag failed"

log "building current working tree"
(cd "$root" && CGO_ENABLED=0 go build -o "$work/leser-new" ./cmd/leser)

wait_ready() {
  for i in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$port/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  die "server on :$port never became ready"
}

log "booting $prev_tag, writing real data through it"
"$work/leser-old" serve --data-dir "$data" --listen "127.0.0.1:$port" --log-level warn \
  > "$work/old.out" 2>&1 &
pid=$!
wait_ready

dsn="$(grep 'DSN:' "$work/old.out" | awk '{print $2}' | sed "s|@localhost:8080|@127.0.0.1:$port|")"
[ -n "$dsn" ] || die "$prev_tag printed no DSN"
admin_pw="$(grep 'Admin:' "$work/old.out" | awk '{print $4}')"
[ -n "$admin_pw" ] || die "$prev_tag printed no admin credentials"

"$work/leser-old" send-test-event --dsn "$dsn" >/dev/null \
  || die "$prev_tag's own send-test-event failed against itself"
sleep 3 # let it consume + flush before we take it down

log "stopping $prev_tag cleanly (SIGTERM — this is an upgrade, not a crash)"
kill -TERM $pid
for i in $(seq 1 50); do kill -0 $pid 2>/dev/null || break; sleep 0.1; done
kill -0 $pid 2>/dev/null && die "$prev_tag did not shut down on SIGTERM"

log "booting the CURRENT build against the SAME --data-dir"
"$work/leser-new" serve --data-dir "$data" --listen "127.0.0.1:$port" --log-level warn \
  > "$work/new.out" 2>&1 &
pid=$!
wait_ready
grep -iE 'corrupt|panic' "$work/new.out" >/dev/null && die "new binary logged corruption/panic on upgrade boot: $(grep -iE 'corrupt|panic' "$work/new.out")"

log "asserting data written by $prev_tag is intact under the current build"
curl -fsS -c "$work/ck" -X POST "http://127.0.0.1:$port/api/0/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"admin@leser.local\",\"password\":\"$admin_pw\"}" >/dev/null \
  || die "login against upgraded data failed — user table not readable"

issues="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$port/api/0/projects/1/issues")"
echo "$issues" | grep -q 'pipeline verification' \
  || die "the issue written by $prev_tag is gone after upgrading: $issues"

status="$(curl -fsS -b "$work/ck" "http://127.0.0.1:$port/api/0/ops/status")"
# events_stored_total is a live per-process counter — it legitimately reads 0
# on the NEW process (nothing left for its own consumer to (re)process once
# the old process already committed that offset). The persistent signal is
# whether a Parquet segment actually exists on disk from the old process.
segments="$(echo "$status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["store_segments"])')"
[ "$segments" -ge 1 ] || die "event store shows 0 on-disk segments after upgrade: $status"

log "asserting the upgraded server still accepts new writes post-upgrade"
"$work/leser-new" send-test-event --dsn "$dsn" >/dev/null \
  || die "current build could not ingest a NEW event after upgrading $prev_tag's data"

log "upgrade PASS: $prev_tag -> current — data, auth, and issue grouping all survived the version boundary"
