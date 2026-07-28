package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// testPayload builds a deterministic payload for record i, varying in size.
func testPayload(i int) []byte {
	n := (i * 37 % 200) // 0..199 bytes
	p := make([]byte, n+8)
	binary.LittleEndian.PutUint64(p, uint64(i))
	for j := 8; j < len(p); j++ {
		p[j] = byte(i + j)
	}
	return p
}

func mustOpen(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l
}

func appendN(t *testing.T, l *Log, start, n int) {
	t.Helper()
	ctx := context.Background()
	for i := start; i < start+n; i++ {
		off, err := l.Append(ctx, 1, testPayload(i))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Fatalf("append %d: got offset %d", i, off)
		}
	}
}

// readAll drains a fresh reader and verifies payload integrity against index.
func readAll(t *testing.T, l *Log, consumer string) int {
	t.Helper()
	r, err := l.NewReader(consumer)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer r.Close()
	count := 0
	for {
		rec, err := r.Next()
		if err == ErrNoRecord {
			return count
		}
		if err != nil {
			t.Fatalf("next after %d records: %v", count, err)
		}
		i := int(binary.LittleEndian.Uint64(rec.Payload))
		if !bytes.Equal(rec.Payload, testPayload(i)) {
			t.Fatalf("record %d (offset %d): payload mismatch", i, rec.Offset)
		}
		count++
	}
}

func TestRoundtripAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	// Tiny segments force many rolls.
	l := mustOpen(t, dir, Options{SegmentBytes: 512, BatchWindow: time.Millisecond})
	appendN(t, l, 0, 200)
	if got := readAll(t, l, "rt"); got != 200 {
		t.Fatalf("read %d want 200", got)
	}
	segs, _ := listSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(segs))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReopenContinuesOffsets(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	appendN(t, l, 0, 10)
	l.Close()
	l = mustOpen(t, dir, Options{})
	appendN(t, l, 10, 10) // offsets must continue at 10
	if got := readAll(t, l, "ro"); got != 20 {
		t.Fatalf("read %d want 20", got)
	}
	l.Close()
}

// TestCrashAtEveryByte is the order-2 §2.1 requirement: cut the segment file at
// every possible byte length (simulating kill -9 mid-write at every point) and
// assert recovery always yields an intact prefix of the written records — never
// a hole, never a corrupt record, and the log stays appendable.
func TestCrashAtEveryByte(t *testing.T) {
	src := t.TempDir()
	l := mustOpen(t, src, Options{BatchWindow: time.Microsecond})
	const n = 40
	appendN(t, l, 0, n)
	l.Close()

	segPath := filepath.Join(src, segmentName(0))
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatal(err)
	}

	// Compute record boundaries: ends[i] = byte length at which records 0..i
	// are fully present.
	var ends []int64
	pos := int64(segHeaderLen)
	f, _ := os.Open(segPath)
	for {
		_, next, err := scanRecord(f, pos, int64(len(data)), 16<<20, 0)
		if err != nil {
			break
		}
		ends = append(ends, next)
		pos = next
	}
	f.Close()
	if len(ends) != n {
		t.Fatalf("boundary scan found %d records, want %d", len(ends), n)
	}

	expectAt := func(cut int) int {
		c := 0
		for _, e := range ends {
			if e <= int64(cut) {
				c++
			}
		}
		return c
	}

	for cut := 0; cut <= len(data); cut++ {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, segmentName(0)), data[:cut], 0o640); err != nil {
			t.Fatal(err)
		}
		lg, err := Open(dir, Options{BatchWindow: time.Microsecond})
		if err != nil {
			t.Fatalf("cut=%d: open: %v", cut, err)
		}
		want := expectAt(cut)
		got := readAll(t, lg, "crash")
		if got != want {
			t.Fatalf("cut=%d: recovered %d records, want prefix of %d", cut, got, want)
		}
		// Sampled: the recovered log must accept appends and read them back.
		if cut%53 == 0 || cut == len(data) {
			ctx := context.Background()
			off, err := lg.Append(ctx, 2, []byte("post-crash"))
			if err != nil {
				t.Fatalf("cut=%d: append after recovery: %v", cut, err)
			}
			if off != uint64(want) {
				t.Fatalf("cut=%d: post-crash offset %d, want %d", cut, off, want)
			}
		}
		lg.Close()
	}
}

// TestMidFileCorruption: a flipped byte inside an earlier record must truncate
// the log at that record (clean prefix), never expose corrupt data.
func TestMidFileCorruption(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{BatchWindow: time.Microsecond})
	appendN(t, l, 0, 30)
	l.Close()

	segPath := filepath.Join(dir, segmentName(0))
	data, _ := os.ReadFile(segPath)
	// Find start of record 10 and flip a byte inside its body.
	pos := int64(segHeaderLen)
	f, _ := os.Open(segPath)
	for i := 0; i < 10; i++ {
		_, next, err := scanRecord(f, pos, int64(len(data)), 16<<20, 0)
		if err != nil {
			t.Fatal(err)
		}
		pos = next
	}
	f.Close()
	data[pos+recHeaderLen+3] ^= 0xFF
	if err := os.WriteFile(segPath, data, 0o640); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, Options{})
	if got := readAll(t, l, "corrupt"); got != 10 {
		t.Fatalf("recovered %d records, want clean prefix of 10", got)
	}
	l.Close()
}

func TestConsumerCommitResume(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{BatchWindow: time.Microsecond})
	appendN(t, l, 0, 20)

	r, err := l.NewReader("grp")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if _, err := r.Next(); err != nil {
			t.Fatalf("next %d: %v", i, err)
		}
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	r.Close()
	l.Close()

	// Reopen everything: consumption must resume at 12.
	l = mustOpen(t, dir, Options{})
	r, err = l.NewReader("grp")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Offset != 12 {
		t.Fatalf("resumed at offset %d, want 12", rec.Offset)
	}
	if lag := r.Lag(); lag != 7 { // 20 total, position now 13
		t.Fatalf("lag %d, want 7", lag)
	}
	r.Close()
	l.Close()
}

func TestRetention(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{SegmentBytes: 400, BatchWindow: time.Microsecond})
	appendN(t, l, 0, 100)

	// A consumer still at offset 0 must block all deletion.
	n, err := l.Retain(RetentionPolicy{MaxBytes: 1, MinRetainOffset: 0}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("deleted %d segments despite MinRetainOffset=0", n)
	}

	// All consumers past everything + tiny size budget: sealed segments go.
	n, err = l.Retain(RetentionPolicy{MaxBytes: 1, MinRetainOffset: 100}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("expected segment deletions")
	}
	if l.OldestOffset() == 0 {
		t.Fatal("oldest offset did not advance")
	}
	// A brand-new reader starts at the new oldest, not at 0.
	r, err := l.NewReader("late")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if rec.Offset != l.OldestOffset() {
		t.Fatalf("late reader got offset %d, want %d", rec.Offset, l.OldestOffset())
	}
	r.Close()
	l.Close()
}

func TestConcurrentAppends(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	const (
		workers = 8
		each    = 200
	)
	var wg sync.WaitGroup
	offsets := make([][]uint64, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			for i := 0; i < each; i++ {
				off, err := l.Append(ctx, 1, []byte(fmt.Sprintf("w%d-%d", w, i)))
				if err != nil {
					t.Errorf("w%d append: %v", w, err)
					return
				}
				offsets[w] = append(offsets[w], off)
			}
		}(w)
	}
	wg.Wait()

	seen := map[uint64]bool{}
	for w := range offsets {
		for _, o := range offsets[w] {
			if seen[o] {
				t.Fatalf("offset %d assigned twice", o)
			}
			seen[o] = true
		}
	}
	if len(seen) != workers*each {
		t.Fatalf("got %d unique offsets, want %d", len(seen), workers*each)
	}
	if l.LatestOffset() != workers*each {
		t.Fatalf("latest %d, want %d", l.LatestOffset(), workers*each)
	}
	l.Close()
}

func TestTooLargeRejected(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{MaxRecordBytes: 1024})
	_, err := l.Append(context.Background(), 1, make([]byte, 2048))
	if err != ErrTooLarge {
		t.Fatalf("got %v, want ErrTooLarge", err)
	}
	l.Close()
}

func TestAppendAfterCloseFails(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{})
	l.Close()
	if _, err := l.Append(context.Background(), 1, []byte("x")); err != ErrClosed {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

// BenchmarkAppendBatched measures ACKNOWLEDGED durable appends/sec (group
// commit: ack comes after the batch fsync). Unlike the raw streaming spike,
// throughput here = producer-concurrency × fsync-rate, so it is benchmarked at
// several producer counts. Ingest concurrency in production is the number of
// in-flight HTTP requests, i.e. large.
func BenchmarkAppendBatched(b *testing.B) {
	for _, mult := range []int{1, 8, 64} { // × GOMAXPROCS producers
		b.Run(fmt.Sprintf("producers-x%d", mult), func(b *testing.B) {
			dir := b.TempDir()
			l, err := Open(dir, Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			payload := make([]byte, 512)
			ctx := context.Background()
			b.SetParallelism(mult)
			start := time.Now()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := l.Append(ctx, 1, payload); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(b.N)/time.Since(start).Seconds(), "events/sec")
		})
	}
}
