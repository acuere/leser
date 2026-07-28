package ratelimit

import (
	"testing"
	"time"
)

// fakeClock advances manually.
type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time { return f.t }

func newTestLimiter(lim Limit) (*Limiter, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	l := New(lim, 100)
	l.now = clk.now
	return l, clk
}

func TestBurstThenReject(t *testing.T) {
	l, _ := newTestLimiter(Limit{PerSecond: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow(1); !ok {
			t.Fatalf("burst request %d rejected", i)
		}
	}
	ok, retry := l.Allow(1)
	if ok {
		t.Fatal("6th request must be rejected")
	}
	if retry <= 0 {
		t.Fatal("retry-after must be positive")
	}
}

func TestRefill(t *testing.T) {
	l, clk := newTestLimiter(Limit{PerSecond: 10, Burst: 5})
	for i := 0; i < 5; i++ {
		l.Allow(1)
	}
	if ok, _ := l.Allow(1); ok {
		t.Fatal("empty bucket admitted")
	}
	clk.t = clk.t.Add(500 * time.Millisecond) // +5 tokens
	for i := 0; i < 5; i++ {
		if ok, _ := l.Allow(1); !ok {
			t.Fatalf("refilled request %d rejected", i)
		}
	}
	if ok, _ := l.Allow(1); ok {
		t.Fatal("bucket should be empty again")
	}
}

func TestProjectIsolation(t *testing.T) {
	l, _ := newTestLimiter(Limit{PerSecond: 1, Burst: 2})
	l.Allow(1)
	l.Allow(1)
	if ok, _ := l.Allow(1); ok {
		t.Fatal("project 1 should be limited")
	}
	// Project 2 unaffected — fairness.
	if ok, _ := l.Allow(2); !ok {
		t.Fatal("project 2 starved by project 1")
	}
}

func TestSnapshotRestore(t *testing.T) {
	l, clk := newTestLimiter(Limit{PerSecond: 10, Burst: 5})
	l.Allow(1)
	l.Allow(1) // 3 tokens left
	snap := l.Snapshot()

	l2 := New(Limit{PerSecond: 10, Burst: 5}, 100)
	l2.now = clk.now
	l2.Restore(snap)
	for i := 0; i < 3; i++ {
		if ok, _ := l2.Allow(1); !ok {
			t.Fatalf("restored token %d rejected", i)
		}
	}
	if ok, _ := l2.Allow(1); ok {
		t.Fatal("restored bucket over-admitted")
	}
}

func TestTrackingCapFailsOpen(t *testing.T) {
	l, _ := newTestLimiter(Limit{PerSecond: 1, Burst: 1})
	l.max = 2
	l.Allow(1)
	l.Allow(2)
	// Beyond cap: admitted (bounded memory beats hard failure).
	if ok, _ := l.Allow(3); !ok {
		t.Fatal("over-cap project must fail open")
	}
}
