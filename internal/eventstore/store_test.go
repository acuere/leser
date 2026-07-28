package eventstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var baseTS = time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC).UnixNano()

// mkEvent builds a deterministic event in the given project and hour bucket.
func mkEvent(proj int64, hour int, i int) Event {
	return Event{
		EventID:     fmt.Sprintf("e-%d-%d-%d", proj, hour, i),
		ProjectID:   proj,
		Timestamp:   baseTS + int64(hour)*hourNanos + int64(i)*int64(time.Second),
		Level:       []string{"error", "warning", "info"}[i%3],
		Fingerprint: fmt.Sprintf("fp-%d", i%50),
		Release:     fmt.Sprintf("1.%d.0", i%4),
		Environment: "production",
		UserID:      fmt.Sprintf("u%d", i%100),
		Message:     "boom",
		Payload:     []byte(`{"exception":"boom"}`),
	}
}

func fill(t *testing.T, s *Store, projects []int64, hours, perHour int) {
	t.Helper()
	for _, p := range projects {
		for h := 0; h < hours; h++ {
			for i := 0; i < perHour; i++ {
				if err := s.Append(mkEvent(p, h, i)); err != nil {
					t.Fatalf("append: %v", err)
				}
			}
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

func collect(t *testing.T, s *Store, q Query) []Event {
	t.Helper()
	var out []Event
	if err := s.Scan(q, func(e Event) error { out = append(out, e); return nil }); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

func TestRoundtripAndPartitionLayout(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1, 2}, 3, 100) // 2 projects × 3 hours × 100

	if got := s.SegmentCount(); got != 6 {
		t.Fatalf("segments: got %d want 6 (one per project-hour)", got)
	}
	// Layout: p<project>/<YYYYMMDDHH>/*.parquet
	found := 0
	filepath.WalkDir(dir, func(path string, d os.DirEntry, _ error) error {
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			found++
		}
		return nil
	})
	if found != 6 {
		t.Fatalf("parquet files on disk: got %d want 6", found)
	}

	got := collect(t, s, Query{ProjectID: 1})
	if len(got) != 300 {
		t.Fatalf("project 1 rows: got %d want 300", len(got))
	}
	for _, e := range got {
		if e.ProjectID != 1 {
			t.Fatalf("tenant leak: got project %d", e.ProjectID)
		}
	}
}

// TestPruningRatio is the order-2 §2.3 acceptance: a query for one project and
// one hour must open exactly one of many segments. Scanning a segment the
// statistics could have excluded is a bug, not a performance nit.
func TestPruningRatio(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1, 2, 3, 4, 5, 6}, 4, 200) // 24 segments

	q := Query{
		ProjectID: 3,
		TimeMin:   baseTS + 2*hourNanos,
		TimeMax:   baseTS + 3*hourNanos - 1,
	}
	got := collect(t, s, q)
	if len(got) != 200 {
		t.Fatalf("rows: got %d want 200", len(got))
	}
	considered, opened := s.PruneStats()
	if considered != 24 {
		t.Fatalf("considered: got %d want 24", considered)
	}
	if opened != 1 {
		t.Fatalf("PRUNING BUG: opened %d segments, the statistics allowed exactly 1", opened)
	}
}

func TestHotBufferMergesWithSegments(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1}, 1, 50) // 50 flushed
	for i := 50; i < 80; i++ {    // 30 hot, unflushed
		if err := s.Append(mkEvent(1, 0, i)); err != nil {
			t.Fatal(err)
		}
	}
	got := collect(t, s, Query{ProjectID: 1})
	if len(got) != 80 {
		t.Fatalf("merged rows: got %d want 80 (50 warm + 30 hot)", len(got))
	}
}

func TestBufferFullBackpressure(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{FlushRows: 10, MaxBufferRows: 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := s.Append(mkEvent(1, 0, i)); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := s.Append(mkEvent(1, 0, 99)); err != ErrBufferFull {
		t.Fatalf("got %v, want ErrBufferFull", err)
	}
	// After flush, appends resume.
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(mkEvent(1, 0, 100)); err != nil {
		t.Fatalf("append after flush: %v", err)
	}
}

func TestReopenRecoversSegments(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{7}, 2, 100)

	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := s2.SegmentCount(); got != 2 {
		t.Fatalf("recovered segments: got %d want 2", got)
	}
	got := collect(t, s2, Query{ProjectID: 7})
	if len(got) != 200 {
		t.Fatalf("rows after reopen: got %d want 200", len(got))
	}
}

func TestTmpFileFromCrashIsIgnoredOnReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1}, 1, 10)
	// Simulate a crash mid-compaction: a stray .tmp next to a good segment.
	part := s.partitionDir(1, baseTS)
	if err := os.WriteFile(filepath.Join(part, "deadbeef-000099.parquet.tmp"), []byte("torn"), 0o640); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("reopen with tmp present: %v", err)
	}
	if got := s2.SegmentCount(); got != 1 {
		t.Fatalf("segments: got %d want 1", got)
	}
}

func TestPredicateFilters(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1}, 1, 120)

	got := collect(t, s, Query{ProjectID: 1, Release: "1.2.0"})
	if len(got) != 30 { // i%4==2 of 120
		t.Fatalf("release filter: got %d want 30", len(got))
	}
	got = collect(t, s, Query{ProjectID: 1, Fingerprint: "fp-7", Level: "warning"})
	for _, e := range got {
		if e.Fingerprint != "fp-7" || e.Level != "warning" {
			t.Fatalf("filter leak: %+v", e)
		}
	}
	// Nonexistent release: bloom filter should let zero rows through.
	got = collect(t, s, Query{ProjectID: 1, Release: "9.9.9"})
	if len(got) != 0 {
		t.Fatalf("phantom rows: %d", len(got))
	}
}

func TestQueryRequiresTenantScope(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Scan(Query{}, func(Event) error { return nil }); err == nil {
		t.Fatal("scan without ProjectID must fail")
	}
}

func TestLimitBoundsResults(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1}, 1, 500)
	got := collect(t, s, Query{ProjectID: 1, Limit: 42})
	if len(got) != 42 {
		t.Fatalf("limit: got %d want 42", len(got))
	}
}

func BenchmarkScanPruned(b *testing.B) {
	dir := b.TempDir()
	s, err := Open(dir, Options{FlushRows: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	for _, p := range []int64{1, 2, 3, 4, 5, 6, 7, 8} {
		for h := 0; h < 6; h++ {
			for i := 0; i < 5000; i++ {
				if err := s.Append(mkEvent(p, h, i)); err != nil {
					b.Fatal(err)
				}
			}
		}
	}
	if err := s.Flush(); err != nil { // 48 segments, 240k rows
		b.Fatal(err)
	}
	q := Query{ProjectID: 5, TimeMin: baseTS + 3*hourNanos, TimeMax: baseTS + 4*hourNanos - 1, Limit: 1 << 30}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n := 0
		if err := s.Scan(q, func(Event) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
		if n != 5000 {
			b.Fatalf("rows %d", n)
		}
	}
}
