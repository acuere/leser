package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// ErrNoRecord is returned by Next when the reader has caught up with the log.
var ErrNoRecord = errors.New("wal: no record available")

// Reader is an independent consumer over the log with a persisted committed
// offset — Kafka-style consumer-group semantics on an offset file (order-2
// §2.1). Multiple Readers fan out over one log without copying data.
//
// Delivery is at-least-once: after a crash, records since the last Commit are
// redelivered. Downstream deduplicates (events by event_id).
//
// A Reader is not safe for concurrent use; each consumer owns one.
type Reader struct {
	log  *Log
	name string

	mu        sync.Mutex
	pos       uint64 // next offset to read
	committed uint64 // last durably committed position (== next to redeliver)

	// current open segment being read, lazily positioned
	f       *os.File
	fBase   uint64
	fOffset uint64 // logical offset the file cursor bytePos corresponds to
	bytePos int64
}

// offsetDir returns the directory holding consumer offset files.
func (l *Log) offsetDir() string { return filepath.Join(l.dir, "consumers") }

// NewReader opens (or resumes) the named consumer. A new name starts at the
// oldest retained offset. Names must be path-safe.
func (l *Log) NewReader(name string) (*Reader, error) {
	if name == "" || strings.ContainsAny(name, "/\\") {
		return nil, fmt.Errorf("wal: invalid consumer name %q", name)
	}
	if err := os.MkdirAll(l.offsetDir(), 0o750); err != nil {
		return nil, err
	}
	r := &Reader{log: l, name: name}
	off, err := readOffsetFile(filepath.Join(l.offsetDir(), name+".offset"))
	switch {
	case err == nil:
		r.pos, r.committed = off, off
	case os.IsNotExist(err):
		r.pos = l.OldestOffset()
		r.committed = r.pos
	default:
		return nil, err
	}
	// If retention already deleted data past the stored offset, jump forward.
	if oldest := l.OldestOffset(); r.pos < oldest {
		r.pos = oldest
	}
	return r, nil
}

// Next returns the record at the reader's position and advances it (in memory
// only — call Commit to persist). Returns ErrNoRecord at the head of the log.
func (r *Reader) Next() (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pos >= r.log.LatestOffset() {
		return Record{}, ErrNoRecord
	}
	if err := r.position(r.pos); err != nil {
		return Record{}, err
	}
	fi, err := r.f.Stat()
	if err != nil {
		return Record{}, err
	}
	rec, next, err := scanRecord(r.f, r.bytePos, fi.Size(), r.log.opts.MaxRecordBytes, r.fOffset)
	if err != nil {
		// Between LatestOffset and the file there should always be a valid
		// record; a torn read here means we raced the writer's buffered write.
		// Report as no-record; the record will be readable after its ack.
		return Record{}, ErrNoRecord
	}
	r.bytePos = next
	r.fOffset++
	r.pos++
	return rec, nil
}

// position ensures r.f is the segment containing offset with bytePos pointing
// at that record, scanning forward as needed.
func (r *Reader) position(offset uint64) error {
	seg, ok := r.log.segmentFor(offset)
	if !ok {
		return fmt.Errorf("wal: offset %d not on disk (oldest %d)", offset, r.log.OldestOffset())
	}
	// Reuse the open segment when the target is at or ahead of the cursor.
	if r.f != nil && r.fBase == seg.base && offset >= r.fOffset {
		return r.skipTo(offset)
	}
	if r.f != nil {
		r.f.Close()
		r.f = nil
	}
	f, err := os.Open(filepath.Join(r.log.dir, segmentName(seg.base)))
	if err != nil {
		return err
	}
	base, err := readSegmentHeader(f)
	if err != nil || base != seg.base {
		f.Close()
		return fmt.Errorf("wal: reopen segment %d: %w", seg.base, err)
	}
	r.f, r.fBase, r.fOffset, r.bytePos = f, seg.base, seg.base, segHeaderLen
	return r.skipTo(offset)
}

// skipTo advances the cursor to the target offset within the open segment.
func (r *Reader) skipTo(offset uint64) error {
	fi, err := r.f.Stat()
	if err != nil {
		return err
	}
	for r.fOffset < offset {
		_, next, err := scanRecord(r.f, r.bytePos, fi.Size(), r.log.opts.MaxRecordBytes, r.fOffset)
		if err != nil {
			return fmt.Errorf("wal: %w scanning to offset %d in segment %d", ErrCorrupt, offset, r.fBase)
		}
		r.bytePos = next
		r.fOffset++
	}
	return nil
}

// ConsumerOffset reads a named consumer's last committed offset without
// attaching a Reader — used by a writer-side process (ingest role, Rung 2)
// to compute a worker's lag purely from shared-directory state, with no
// in-process communication (order-2 §5: backpressure is a function of log
// lag). ok is false if the consumer has never committed.
func (l *Log) ConsumerOffset(name string) (offset uint64, ok bool, err error) {
	off, err := readOffsetFile(filepath.Join(l.offsetDir(), name+".offset"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return off, true, nil
}

// Commit durably persists the reader's current position (atomic tmp+rename).
// After a crash, consumption resumes from the last committed position.
func (r *Reader) Commit() error {
	r.mu.Lock()
	pos := r.pos
	r.mu.Unlock()
	if err := writeOffsetFile(filepath.Join(r.log.offsetDir(), r.name+".offset"), pos); err != nil {
		return err
	}
	r.mu.Lock()
	r.committed = pos
	r.mu.Unlock()
	return nil
}

// Lag returns how many records this reader is behind the log head.
func (r *Reader) Lag() uint64 {
	r.mu.Lock()
	pos := r.pos
	r.mu.Unlock()
	head := r.log.LatestOffset()
	if pos >= head {
		return 0
	}
	return head - pos
}

// Close releases the reader's file handle. It does not commit.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f != nil {
		err := r.f.Close()
		r.f = nil
		return err
	}
	return nil
}

// Offset file format: [8B magic "LESROFF1"][u64 offset][u32 crc32c(offset)].
// Written to a temp file, fsynced, renamed — atomic on POSIX.
const offMagic = "LESROFF1"

func writeOffsetFile(path string, offset uint64) error {
	var b [20]byte
	copy(b[0:8], offMagic)
	binary.LittleEndian.PutUint64(b[8:16], offset)
	binary.LittleEndian.PutUint32(b[16:20], crc32.Checksum(b[8:16], castagnoli))
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(b[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readOffsetFile(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(b) != 20 || string(b[0:8]) != offMagic {
		return 0, fmt.Errorf("wal: %w: offset file %s", ErrCorrupt, path)
	}
	off := binary.LittleEndian.Uint64(b[8:16])
	if crc32.Checksum(b[8:16], castagnoli) != binary.LittleEndian.Uint32(b[16:20]) {
		return 0, fmt.Errorf("wal: %w: offset file %s crc", ErrCorrupt, path)
	}
	return off, nil
}
