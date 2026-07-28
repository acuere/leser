package eventstore

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestHLLAccuracy(t *testing.T) {
	for _, n := range []int{100, 5_000, 200_000} {
		h := NewHLL()
		for i := 0; i < n; i++ {
			h.Add(fmt.Sprintf("user-%d", i))
		}
		est := float64(h.Estimate())
		errPct := math.Abs(est-float64(n)) / float64(n) * 100
		if errPct > 5 { // p=12 → ~1.6% standard error; 5% is a generous gate
			t.Errorf("n=%d: estimate %.0f, error %.1f%% > 5%%", n, est, errPct)
		}
	}
}

func TestHLLMergeAndRoundtrip(t *testing.T) {
	a, b := NewHLL(), NewHLL()
	for i := 0; i < 1000; i++ {
		a.Add(fmt.Sprintf("a-%d", i))
		b.Add(fmt.Sprintf("b-%d", i))
	}
	enc := a.MarshalText()
	a2, err := UnmarshalHLL(enc)
	if err != nil {
		t.Fatal(err)
	}
	if a2.Estimate() != a.Estimate() {
		t.Fatal("roundtrip changed estimate")
	}
	a2.Merge(b)
	est := float64(a2.Estimate())
	if math.Abs(est-2000)/2000 > 0.05 {
		t.Errorf("merged estimate %.0f, want ~2000", est)
	}
	// Idempotent re-adds must not inflate.
	for i := 0; i < 1000; i++ {
		a2.Add(fmt.Sprintf("a-%d", i))
	}
	if math.Abs(float64(a2.Estimate())-2000)/2000 > 0.05 {
		t.Error("re-adding known values inflated the estimate")
	}
}

func TestAggregate(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	// 3 hours × 120 events, flushed; plus 30 hot rows in hour 0.
	fill(t, s, []int64{1}, 3, 120)
	for i := 0; i < 30; i++ {
		if err := s.Append(mkEvent(1, 0, 200+i)); err != nil {
			t.Fatal(err)
		}
	}

	st, err := s.Aggregate(Query{ProjectID: 1}, hourNanos, 10)
	if err != nil {
		t.Fatal(err)
	}
	if st.Total != 390 {
		t.Fatalf("total %d want 390", st.Total)
	}
	if len(st.Buckets) != 3 {
		t.Fatalf("buckets %d want 3", len(st.Buckets))
	}
	if st.Buckets[0].Count != 150 || st.Buckets[1].Count != 120 || st.Buckets[2].Count != 120 {
		t.Fatalf("bucket counts: %+v", st.Buckets)
	}
	// Top levels: error/warning/info split of 390 rows.
	if len(st.TopBy["level"]) != 3 {
		t.Fatalf("levels: %+v", st.TopBy["level"])
	}
	// Unique users: mkEvent uses i%100 across 0..119 and 200..229 → u0..u99 = 100.
	if st.UniqueUsers < 95 || st.UniqueUsers > 105 {
		t.Fatalf("unique users %d want ~100", st.UniqueUsers)
	}
	// Filtered aggregate stays consistent.
	st2, err := s.Aggregate(Query{ProjectID: 1, Level: "error", TimeMin: baseTS, TimeMax: baseTS + hourNanos - 1}, hourNanos, 5)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Total == 0 || st2.Total >= st.Total {
		t.Fatalf("filtered total %d", st2.Total)
	}
}

func TestAggregateTenantScopeRequired(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir, Options{})
	fill(t, s, []int64{1, 2}, 1, 10)
	st, err := s.Aggregate(Query{ProjectID: 2}, hourNanos, 5)
	if err != nil {
		t.Fatal(err)
	}
	if st.Total != 10 {
		t.Fatalf("tenant leak in aggregate: total %d want 10", st.Total)
	}
}

// BenchmarkAggregate1M is the order-2 M4 latency benchmark at the 1M scale
// (100M/1B require the perf rig, not a laptop test suite — deferred honestly).
func BenchmarkAggregate1M(b *testing.B) {
	dir := b.TempDir()
	s, err := Open(dir, Options{FlushRows: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	const (
		projects = 4
		hours    = 10
		perSeg   = 25_000 // 4×10×25k = 1M rows
	)
	start := time.Now()
	step := hourNanos / perSeg // keep every row inside its hour bucket
	for p := int64(1); p <= projects; p++ {
		for h := 0; h < hours; h++ {
			for i := 0; i < perSeg; i++ {
				e := mkEvent(p, h, i)
				e.Timestamp = baseTS + int64(h)*hourNanos + int64(i)*step
				if err := s.Append(e); err != nil {
					b.Fatal(err)
				}
			}
			if err := s.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Logf("ingested 1M rows in %v (%d segments)", time.Since(start).Round(time.Millisecond), s.SegmentCount())

	q := Query{ProjectID: 2, TimeMin: baseTS + 2*hourNanos, TimeMax: baseTS + 6*hourNanos - 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := s.Aggregate(q, hourNanos, 10)
		if err != nil {
			b.Fatal(err)
		}
		if st.Total != 4*perSeg {
			b.Fatalf("total %d", st.Total)
		}
	}
}
