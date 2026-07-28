package eventstore

import "testing"

// TestCrossProcessRefresh simulates Rung 2's query role: a second Store
// handle on the same directory that never writes, discovering segments the
// first (worker-role) Store compacts, purely via Refresh() + shared storage.
func TestCrossProcessRefresh(t *testing.T) {
	dir := t.TempDir()
	writer, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, writer, []int64{1}, 1, 100) // flushed: one segment

	reader, err := Open(dir, Options{}) // "query role": opened after first flush
	if err != nil {
		t.Fatal(err)
	}
	if got := reader.SegmentCount(); got != 1 {
		t.Fatalf("initial open: got %d segments, want 1", got)
	}

	// Writer compacts more AFTER the reader already opened.
	fill(t, writer, []int64{1}, 1, 50) // second hour, second segment (baseTS+hourNanos)
	if got := reader.SegmentCount(); got != 1 {
		t.Fatalf("reader saw new segment without calling Refresh: %d", got)
	}

	if err := reader.Refresh(); err != nil {
		t.Fatal(err)
	}
	if got := reader.SegmentCount(); got != 2 {
		t.Fatalf("after refresh: got %d segments, want 2", got)
	}
	got := collect(t, reader, Query{ProjectID: 1})
	if len(got) != 150 {
		t.Fatalf("reader rows after refresh: got %d want 150", len(got))
	}

	// Refresh is idempotent: no duplicate segments on repeated calls.
	for i := 0; i < 3; i++ {
		if err := reader.Refresh(); err != nil {
			t.Fatal(err)
		}
	}
	if got := reader.SegmentCount(); got != 2 {
		t.Fatalf("refresh duplicated segments: %d", got)
	}
}

// TestRefreshDoesNotRediscoverOwnWrites: a Store that writes its own segments
// (worker role) must not re-add them to itself via Refresh.
func TestRefreshDoesNotRediscoverOwnWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fill(t, s, []int64{1}, 2, 20)
	before := s.SegmentCount()
	if err := s.Refresh(); err != nil {
		t.Fatal(err)
	}
	if got := s.SegmentCount(); got != before {
		t.Fatalf("Refresh duplicated self-written segments: before=%d after=%d", before, got)
	}
}
