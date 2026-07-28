package wal

import (
	"context"
	"testing"
	"time"
)

// These tests simulate Rung 2 role separation: two independent *Log handles
// on the same directory, one writer-owned, one ReadOnly-attached — exactly
// what an ingest process and a worker process do when pointed at shared
// storage. There is no in-process communication between them; everything
// flows through the filesystem, same as it would across a real NFS mount.

func TestReadOnlyAttachBeforeAnyData(t *testing.T) {
	dir := t.TempDir()
	ro, err := Open(dir, Options{ReadOnly: true, RefreshInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("attach to empty dir: %v", err)
	}
	defer ro.Close()
	if ro.LatestOffset() != 0 || ro.OldestOffset() != 0 {
		t.Fatalf("empty attach: latest=%d oldest=%d", ro.LatestOffset(), ro.OldestOffset())
	}
	if _, err := ro.Append(context.Background(), 1, []byte("x")); err != ErrReadOnly {
		t.Fatalf("append on read-only: got %v want ErrReadOnly", err)
	}
}

func TestCrossProcessLiveTail(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 600, BatchWindow: time.Microsecond})
	defer w.Close()

	// Reader attaches BEFORE the writer produces anything (order independent).
	ro, err := Open(dir, Options{ReadOnly: true, RefreshInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	appendN(t, w, 0, 60) // small SegmentBytes forces multiple segment rolls

	waitForOffset(t, ro, 60)
	if got := readAll(t, ro, "crossproc"); got != 60 {
		t.Fatalf("cross-process reader saw %d records, want 60", got)
	}

	// Keep writing after the reader already drained; the reader must keep
	// tailing live, including across further segment rolls.
	appendN(t, w, 60, 40)
	waitForOffset(t, ro, 100)
	got := readAll(t, ro, "crossproc2")
	if got != 100 {
		t.Fatalf("after further writes: read %d, want 100", got)
	}
}

func TestConsumerOffsetVisibleAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{BatchWindow: time.Microsecond})
	defer w.Close()
	appendN(t, w, 0, 10)

	if _, ok, err := w.ConsumerOffset("worker"); err != nil || ok {
		t.Fatalf("uncommitted consumer: ok=%v err=%v", ok, err)
	}

	ro, err := Open(dir, Options{ReadOnly: true, RefreshInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	waitForOffset(t, ro, 10)
	r, err := ro.NewReader("worker")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Commit(); err != nil {
		t.Fatal(err)
	}
	r.Close()

	// The WRITER-side Log (a different *Log, standing in for the ingest
	// process) reads the same committed offset purely via the filesystem —
	// this is the lag computation ingest uses for backpressure without any
	// in-process channel to the worker.
	off, ok, err := w.ConsumerOffset("worker")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || off != 7 {
		t.Fatalf("writer-side view of committed offset: ok=%v off=%d, want true/7", ok, off)
	}
	lag := w.LatestOffset() - off
	if lag != 3 {
		t.Fatalf("computed lag %d, want 3 (10 total - 7 committed)", lag)
	}
}

func TestReadOnlySurvivesSegmentRollRace(t *testing.T) {
	// Tight segment size + fast concurrent writes maximizes the odds of the
	// reader observing a segment mid-roll (torn header) — refreshLoop must
	// treat that as "not yet" rather than corruption.
	dir := t.TempDir()
	w := mustOpen(t, dir, Options{SegmentBytes: 300, BatchWindow: time.Microsecond})
	defer w.Close()

	ro, err := Open(dir, Options{ReadOnly: true, RefreshInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()

	done := make(chan struct{})
	go func() { appendN(t, w, 0, 300); close(done) }()
	<-done

	waitForOffset(t, ro, 300)
	if got := readAll(t, ro, "race"); got != 300 {
		t.Fatalf("got %d records, want 300 (no corruption tolerated)", got)
	}
}

// waitForOffset polls a ReadOnly log's LatestOffset until it reaches want or
// times out — standing in for "the worker process eventually sees it".
func waitForOffset(t *testing.T, l *Log, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if l.LatestOffset() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for offset %d, stuck at %d", want, l.LatestOffset())
}
