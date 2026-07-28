// Package wal is a segment-based append-only write-ahead log on local disk.
// It is the spine of the system (order-2 §2.1): ingest appends, every other
// subsystem consumes. Single writer goroutine, batched fsync, crash-only
// recovery, consumer offsets, retention. No external services.
package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// SyncPolicy selects the durability/throughput tradeoff. Loss windows are
// documented per policy; in every policy an Append that returned nil error is
// durable except where stated.
type SyncPolicy int

const (
	// SyncBatched (default): appends are grouped over BatchWindow and one
	// fsync is issued per batch; Append returns only after that fsync.
	// Loss window: none for acknowledged appends.
	// Measured: ~1.3M appends/sec @512B on M4/NVMe (docs/spikes).
	SyncBatched SyncPolicy = iota
	// SyncAlways: one fsync per record. Loss window: none. ~250 appends/sec.
	SyncAlways
	// SyncInterval: acks after write (page cache), fsync at most every
	// BatchWindow. Loss window: up to BatchWindow of acknowledged appends.
	SyncInterval
)

// Options configure a Log. Zero values take documented defaults. Everything
// is bounded (order-2 §5).
type Options struct {
	// SegmentBytes seals a segment when it would exceed this size. Default 128MB.
	SegmentBytes int64
	// MaxRecordBytes rejects larger appends. Default 16MB.
	MaxRecordBytes int
	// MaxBatchBytes flushes a batch early when it grows past this. Default 4MB.
	MaxBatchBytes int
	// MaxPending bounds queued append requests; further Appends block (the
	// caller's context enforces the deadline). Default 4096.
	MaxPending int
	// Policy is the durability policy. Default SyncBatched.
	Policy SyncPolicy
	// BatchWindow is the batch/interval duration. Default 2ms.
	BatchWindow time.Duration
}

func (o *Options) defaults() {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 128 << 20
	}
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = 16 << 20
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = 4 << 20
	}
	if o.MaxPending <= 0 {
		o.MaxPending = 4096
	}
	if o.BatchWindow <= 0 {
		o.BatchWindow = 2 * time.Millisecond
	}
}

// ErrClosed is returned by operations on a closed Log.
var ErrClosed = errors.New("wal: closed")

// ErrTooLarge is returned for appends exceeding MaxRecordBytes.
var ErrTooLarge = errors.New("wal: record exceeds max size")

// segMeta describes one on-disk segment.
type segMeta struct {
	base    uint64 // offset of first record
	records uint64 // record count (maintained for active; scanned for sealed)
	bytes   int64  // current file size
}

// end returns one past the last offset in the segment.
func (s segMeta) end() uint64 { return s.base + s.records }

type appendReq struct {
	typ     byte
	payload []byte
	resp    chan appendResp
}

type appendResp struct {
	offset uint64
	err    error
}

// Log is an append-only segmented WAL. Safe for concurrent use. All writes
// funnel through one goroutine — single-writer discipline is structural.
type Log struct {
	dir  string
	opts Options

	reqs chan appendReq
	quit chan struct{} // closed by Close to stop the writer
	done chan struct{} // closed when writer loop exits

	mu     sync.RWMutex // guards fields below
	segs   []segMeta    // ascending by base; last is active
	active *os.File
	next   uint64 // next offset to assign
	closed bool

	lastSync time.Time // SyncInterval bookkeeping (writer goroutine only)
}

// Open opens (or creates) a log in dir, recovering the active segment by
// truncating any torn tail. Recovery runs on every open.
func Open(dir string, opts Options) (*Log, error) {
	opts.defaults()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	l := &Log{
		dir:  dir,
		opts: opts,
		reqs: make(chan appendReq, opts.MaxPending),
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	go l.writeLoop()
	return l, nil
}

// load scans existing segments, recovers the tail, and opens the active file.
func (l *Log) load() error {
	bases, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	if len(bases) == 0 {
		f, err := createSegment(l.dir, 0)
		if err != nil {
			return err
		}
		l.active = f
		l.segs = []segMeta{{base: 0, bytes: segHeaderLen}}
		l.next = 0
		return nil
	}
	// Sealed segments: count records by scan (cheap sequential read; also
	// validates that base offsets chain correctly).
	for i, base := range bases {
		f, err := os.Open(filepath.Join(l.dir, segmentName(base)))
		if err != nil {
			return err
		}
		hdrBase, err := readSegmentHeader(f)
		if err != nil {
			// A torn header can only be the last (crash during roll): drop it.
			f.Close()
			if i == len(bases)-1 {
				if rmErr := os.Remove(filepath.Join(l.dir, segmentName(base))); rmErr != nil {
					return rmErr
				}
				break
			}
			return fmt.Errorf("wal: segment %d: %w", base, err)
		}
		if hdrBase != base {
			f.Close()
			return fmt.Errorf("wal: segment %d header claims base %d", base, hdrBase)
		}
		n, validSize, err := scanSegment(f, l.opts.MaxRecordBytes)
		f.Close()
		if err != nil {
			return err
		}
		l.segs = append(l.segs, segMeta{base: base, records: n, bytes: validSize})
	}
	if len(l.segs) == 0 { // the only segment had a torn header
		f, err := createSegment(l.dir, 0)
		if err != nil {
			return err
		}
		l.active = f
		l.segs = []segMeta{{base: 0, bytes: segHeaderLen}}
		l.next = 0
		return nil
	}
	// Validate the offset chain: each sealed segment must hand off exactly to
	// the next base. A gap means a sealed segment lost acknowledged records —
	// that is corruption we must refuse to run on, never silently skip.
	for i := 0; i < len(l.segs)-1; i++ {
		if l.segs[i].end() != l.segs[i+1].base {
			return fmt.Errorf("wal: %w: segment %d ends at %d but next base is %d",
				ErrCorrupt, l.segs[i].base, l.segs[i].end(), l.segs[i+1].base)
		}
	}
	// Reopen the last segment writable, truncate any torn tail, position at end.
	last := &l.segs[len(l.segs)-1]
	f, err := os.OpenFile(filepath.Join(l.dir, segmentName(last.base)), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if fi, serr := f.Stat(); serr == nil && fi.Size() > last.bytes {
		if terr := f.Truncate(last.bytes); terr != nil {
			f.Close()
			return terr
		}
	}
	if _, err := f.Seek(last.bytes, 0); err != nil {
		f.Close()
		return err
	}
	l.active = f
	l.next = last.end()
	return nil
}

// Append writes one record and returns its logical offset. It blocks until the
// record is acknowledged per the sync policy, or ctx is done. On ctx
// cancellation before submission, nothing was written; after submission the
// record may still be durably appended (at-least-once semantics — consumers
// deduplicate by event_id downstream).
func (l *Log) Append(ctx context.Context, typ byte, payload []byte) (uint64, error) {
	if len(payload)+bodyFixedLen > l.opts.MaxRecordBytes {
		return 0, ErrTooLarge
	}
	l.mu.RLock()
	closed := l.closed
	l.mu.RUnlock()
	if closed {
		return 0, ErrClosed
	}
	req := appendReq{typ: typ, payload: payload, resp: make(chan appendResp, 1)}
	select {
	case l.reqs <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-l.done:
		return 0, ErrClosed
	}
	select {
	case r := <-req.resp:
		return r.offset, r.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-l.done:
		// Writer exited with this request still queued: it was never written.
		return 0, ErrClosed
	}
}

// writeLoop is the single writer. It batches requests within BatchWindow (or
// until MaxBatchBytes), writes them, syncs per policy, then acknowledges.
func (l *Log) writeLoop() {
	defer close(l.done)
	var batch []appendReq
	buf := make([]byte, 0, 64<<10)
	for {
		var req appendReq
		select {
		case req = <-l.reqs:
		case <-l.quit:
			return
		}
		batch = append(batch[:0], req)
		batchBytes := recHeaderLen + bodyFixedLen + len(req.payload)

		if l.opts.Policy == SyncBatched || l.opts.Policy == SyncInterval {
			deadline := time.NewTimer(l.opts.BatchWindow)
		collect:
			for batchBytes < l.opts.MaxBatchBytes {
				select {
				case r := <-l.reqs:
					batch = append(batch, r)
					batchBytes += recHeaderLen + bodyFixedLen + len(r.payload)
				case <-deadline.C:
					break collect
				}
			}
			deadline.Stop()
		}

		offsets, err := l.writeBatch(batch, &buf)
		for i, r := range batch {
			if err != nil {
				r.resp <- appendResp{err: err}
			} else {
				r.resp <- appendResp{offset: offsets[i]}
			}
		}
	}
}

// writeBatch appends all records in batch, rolling segments as needed, and
// syncs according to policy. Returns the assigned offsets.
func (l *Log) writeBatch(batch []appendReq, buf *[]byte) ([]uint64, error) {
	offsets := make([]uint64, len(batch))
	now := time.Now().UnixNano()
	for i, r := range batch {
		rec := appendRecord(*buf, now, r.typ, r.payload)
		*buf = rec[:0]

		l.mu.Lock()
		active := &l.segs[len(l.segs)-1]
		if active.bytes+int64(len(rec)) > l.opts.SegmentBytes && active.records > 0 {
			if err := l.roll(); err != nil {
				l.mu.Unlock()
				return nil, err
			}
			active = &l.segs[len(l.segs)-1]
		}
		f := l.active
		l.mu.Unlock()

		if _, err := f.Write(rec); err != nil {
			return nil, fmt.Errorf("wal: append: %w", err)
		}

		l.mu.Lock()
		active = &l.segs[len(l.segs)-1]
		active.bytes += int64(len(rec))
		active.records++
		offsets[i] = l.next
		l.next++
		l.mu.Unlock()

		if l.opts.Policy == SyncAlways {
			if err := f.Sync(); err != nil {
				return nil, fmt.Errorf("wal: fsync: %w", err)
			}
		}
	}

	switch l.opts.Policy {
	case SyncBatched:
		if err := l.active.Sync(); err != nil {
			return nil, fmt.Errorf("wal: fsync: %w", err)
		}
	case SyncInterval:
		if time.Since(l.lastSync) >= l.opts.BatchWindow {
			if err := l.active.Sync(); err != nil {
				return nil, fmt.Errorf("wal: fsync: %w", err)
			}
			l.lastSync = time.Now()
		}
	}
	return offsets, nil
}

// roll seals the active segment (sync + close) and creates the next one.
// Caller holds l.mu.
func (l *Log) roll() error {
	if err := l.active.Sync(); err != nil {
		return err
	}
	if err := l.active.Close(); err != nil {
		return err
	}
	base := l.next
	f, err := createSegment(l.dir, base)
	if err != nil {
		return err
	}
	l.active = f
	l.segs = append(l.segs, segMeta{base: base, bytes: segHeaderLen})
	return nil
}

// LatestOffset returns the next offset to be assigned (== count of records
// ever appended, absent truncation-by-retention of history semantics).
func (l *Log) LatestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.next
}

// OldestOffset returns the first offset still on disk (moves up as retention
// deletes sealed segments).
func (l *Log) OldestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segs[0].base
}

// segmentFor returns the metadata of the segment containing offset.
func (l *Log) segmentFor(offset uint64) (segMeta, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	i := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].end() > offset })
	if i == len(l.segs) || offset < l.segs[i].base {
		return segMeta{}, false
	}
	return l.segs[i], true
}

// Close stops the writer and closes files. Best-effort only — correctness
// comes from recovery-on-open, not from shutdown (crash-only design).
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()
	close(l.quit)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.active.Sync(); err != nil {
		l.active.Close()
		return err
	}
	return l.active.Close()
}
