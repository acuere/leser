package wal

import (
	"os"
	"path/filepath"
	"time"
)

// RetentionPolicy bounds disk usage. Zero values disable that bound. Sealed
// segments are deleted whole, never rewritten (order-2 §2.1). The active
// segment is never deleted.
type RetentionPolicy struct {
	// MaxBytes: total on-disk size cap; oldest sealed segments go first.
	MaxBytes int64
	// MaxAge: sealed segments whose newest record is older are deleted.
	MaxAge time.Duration
	// MinRetainOffset: never delete records at or above this offset — callers
	// pass the minimum committed offset across all consumers so a slow
	// consumer's unread data is preserved.
	MinRetainOffset uint64
}

// Retain applies the policy and returns the number of segments deleted. Only
// entire sealed segments whose every record is (a) below MinRetainOffset and
// (b) beyond MaxAge or over the size budget are removed.
func (l *Log) Retain(p RetentionPolicy, now time.Time) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var total int64
	for _, s := range l.segs {
		total += s.bytes
	}

	deleted := 0
	// Walk oldest-first; the last segment is active and untouchable.
	for len(l.segs) > 1 {
		s := l.segs[0]
		if s.end() > p.MinRetainOffset {
			break // a consumer still needs records in this segment
		}
		overSize := p.MaxBytes > 0 && total > p.MaxBytes
		overAge := false
		if p.MaxAge > 0 {
			if newest, err := l.segmentNewestTime(s); err == nil {
				overAge = now.Sub(newest) > p.MaxAge
			}
		}
		if !overSize && !overAge {
			break
		}
		if err := os.Remove(filepath.Join(l.dir, segmentName(s.base))); err != nil {
			return deleted, err
		}
		total -= s.bytes
		l.segs = l.segs[1:]
		deleted++
	}
	return deleted, nil
}

// segmentNewestTime returns the timestamp of the last record in a sealed
// segment (scan; sealed segments are immutable so this could be cached later).
func (l *Log) segmentNewestTime(s segMeta) (time.Time, error) {
	f, err := os.Open(filepath.Join(l.dir, segmentName(s.base)))
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}
	var last Record
	pos := int64(segHeaderLen)
	off := s.base
	for {
		rec, next, err := scanRecord(f, pos, fi.Size(), l.opts.MaxRecordBytes, off)
		if err != nil {
			break
		}
		last = rec
		pos = next
		off++
	}
	return time.Unix(0, last.Time), nil
}
