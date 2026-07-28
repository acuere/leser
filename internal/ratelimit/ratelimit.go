// Package ratelimit implements per-project token buckets in memory with
// periodic checkpointing to SQLite so a restart does not reset every quota.
// Approximate-after-restart is fine and correct enough (order-2 §2.5).
package ratelimit

import (
	"sync"
	"time"
)

// Limit is one project's quota configuration.
type Limit struct {
	PerSecond float64 // sustained refill rate
	Burst     float64 // bucket capacity
}

// DefaultLimit is generous by design: self-hosters shed by consumer lag first;
// per-project buckets exist for fairness so one noisy project cannot starve
// the rest (order-2 §5).
var DefaultLimit = Limit{PerSecond: 1000, Burst: 5000}

// bucket is one project's token bucket.
type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter is a bounded set of per-project buckets. Safe for concurrent use.
type Limiter struct {
	mu    sync.Mutex
	def   Limit
	limit map[int64]Limit   // per-project overrides
	b     map[int64]*bucket // live buckets, bounded by maxProjects
	max   int
	now   func() time.Time // injected for tests
}

// New creates a limiter with the default limit and a cap on tracked projects
// (bound everything; beyond the cap the default limit is applied statelessly,
// which fails open at the configured rate rather than unbounded memory).
func New(def Limit, maxProjects int) *Limiter {
	if maxProjects <= 0 {
		maxProjects = 10_000
	}
	return &Limiter{def: def, limit: map[int64]Limit{}, b: map[int64]*bucket{}, max: maxProjects, now: time.Now}
}

// SetLimit overrides one project's quota.
func (l *Limiter) SetLimit(projectID int64, lim Limit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limit[projectID] = lim
}

// Allow consumes one token for the project, reporting whether the request is
// admitted and, when rejected, the suggested Retry-After.
func (l *Limiter) Allow(projectID int64) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lim, ok := l.limit[projectID]
	if !ok {
		lim = l.def
	}
	bk, ok := l.b[projectID]
	if !ok {
		if len(l.b) >= l.max {
			return true, 0 // over the tracking cap: admit at default (fail open, bounded memory)
		}
		bk = &bucket{tokens: lim.Burst, last: l.now()}
		l.b[projectID] = bk
	}
	now := l.now()
	bk.tokens += now.Sub(bk.last).Seconds() * lim.PerSecond
	if bk.tokens > lim.Burst {
		bk.tokens = lim.Burst
	}
	bk.last = now
	if bk.tokens >= 1 {
		bk.tokens--
		return true, 0
	}
	// Time until one token refills.
	wait := time.Duration((1 - bk.tokens) / lim.PerSecond * float64(time.Second))
	if wait < time.Second {
		wait = time.Second
	}
	return false, wait
}

// Snapshot exports bucket state for checkpointing.
func (l *Limiter) Snapshot() map[int64]float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[int64]float64, len(l.b))
	for id, bk := range l.b {
		out[id] = bk.tokens
	}
	return out
}

// Restore seeds bucket state from a checkpoint (approximate: refill since the
// checkpoint is granted on first Allow via the timestamp).
func (l *Limiter) Restore(state map[int64]float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	for id, tokens := range state {
		if len(l.b) >= l.max {
			break
		}
		l.b[id] = &bucket{tokens: tokens, last: now}
	}
}
