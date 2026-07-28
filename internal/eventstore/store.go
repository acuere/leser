// Package eventstore is the time-partitioned columnar event store (order-2
// §2.3): a bounded in-memory row buffer (hot tier) compacted into Parquet
// segments partitioned by (project_id, hour) with column statistics and Bloom
// filters (warm tier). Reads merge buffer + segments with aggressive segment
// pruning. Durability of the hot tier comes from the WAL upstream — on crash,
// the ingest pipeline replays unflushed events from its WAL consumer offset.
package eventstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
)

// SchemaVersion is stamped into every segment's footer metadata. On-disk
// formats are versioned; readers refuse versions they don't know (order-2 §5).
const SchemaVersion = 1

const hourNanos = int64(time.Hour)

// Event is the columnar row schema, v1. Payload carries the full original
// event JSON (zstd-compressed by the column codec); the other columns exist to
// be filtered and aggregated without touching Payload.
type Event struct {
	EventID     string `parquet:"event_id"`
	ProjectID   int64  `parquet:"project_id"`
	Timestamp   int64  `parquet:"timestamp"` // unix nanos
	Level       string `parquet:"level,dict"`
	Fingerprint string `parquet:"fingerprint,dict"`
	Release     string `parquet:"release,dict"`
	Environment string `parquet:"environment,dict"`
	UserID      string `parquet:"user_id"`
	Message     string `parquet:"message"`
	Payload     []byte `parquet:"payload,zstd"`
}

// Options bound the store. Zero values take defaults; everything is bounded.
type Options struct {
	// FlushRows triggers compaction when the buffer reaches this many rows.
	// Default 50k.
	FlushRows int
	// MaxBufferRows is the hard cap; Append returns ErrBufferFull beyond it
	// (backpressure, never growth). Default 2×FlushRows.
	MaxBufferRows int
	// FlushAge compacts a non-empty buffer older than this even if small.
	// Enforced by the caller's ticker via MaybeFlush. Default 10s.
	FlushAge time.Duration
	// QueryLimitDefault caps result rows when the query asks for no limit.
	// Default 10k.
	QueryLimitDefault int
}

func (o *Options) defaults() {
	if o.FlushRows <= 0 {
		o.FlushRows = 50_000
	}
	if o.MaxBufferRows <= 0 {
		o.MaxBufferRows = 2 * o.FlushRows
	}
	if o.FlushAge <= 0 {
		o.FlushAge = 10 * time.Second
	}
	if o.QueryLimitDefault <= 0 {
		o.QueryLimitDefault = 10_000
	}
}

// ErrBufferFull signals the hot tier is at capacity; callers shed load (429)
// rather than buffer more (order-2 §5).
var ErrBufferFull = errors.New("eventstore: buffer full")

// segMeta is one immutable on-disk segment. Pruning metadata lives here so a
// query never opens a file it can rule out.
type segMeta struct {
	path      string
	projectID int64
	hour      int64 // hour bucket start, unix nanos
	minTS     int64
	maxTS     int64
	rows      int64
}

// Store is safe for concurrent use.
type Store struct {
	dir  string
	opts Options

	mu         sync.RWMutex
	buf        []Event
	bufSince   time.Time // when the oldest buffered row arrived
	segs       []segMeta // sorted by (projectID, hour, path)
	flushMu    sync.Mutex
	flushCount uint64

	// counters for pruning-ratio assertions and self-observability
	statsMu       sync.Mutex
	statSegsTotal uint64 // segments considered across queries
	statSegsOpen  uint64 // segments actually opened
}

// Open loads segment metadata from dir (walking partition directories and
// reading footer metadata) and returns a ready store.
func Open(dir string, opts Options) (*Store, error) {
	opts.defaults()
	s := &Store{dir: dir, opts: opts}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	if err := s.loadSegments(); err != nil {
		return nil, err
	}
	return s, nil
}

// partitionDir returns the directory for a (project, hour) partition.
func (s *Store) partitionDir(project int64, hour int64) string {
	t := time.Unix(0, hour).UTC()
	return filepath.Join(s.dir, fmt.Sprintf("p%d", project), t.Format("2006010215"))
}

// parsePartition extracts (project, hour) back out of a partition path.
func (s *Store) parsePartition(rel string) (int64, int64, bool) {
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 2 || !strings.HasPrefix(parts[0], "p") {
		return 0, 0, false
	}
	proj, err := strconv.ParseInt(parts[0][1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	t, err := time.ParseInLocation("2006010215", parts[1], time.UTC)
	if err != nil {
		return 0, 0, false
	}
	return proj, t.UnixNano(), true
}

// loadSegments walks the store directory and rebuilds segment metadata from
// paths and parquet footers. Runs on every open — recovery is the boot path.
func (s *Store) loadSegments() error {
	return filepath.WalkDir(s.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return err
		}
		rel, rerr := filepath.Rel(s.dir, path)
		if rerr != nil {
			return rerr
		}
		proj, hour, ok := s.parsePartition(rel)
		if !ok {
			return nil // foreign file; ignore
		}
		meta, merr := readSegmentMeta(path, proj, hour)
		if merr != nil {
			// A torn segment (crash mid-compaction before rename) must not
			// exist: compaction writes tmp + renames. A .parquet that fails to
			// open is corruption — refuse loudly rather than silently skip.
			return fmt.Errorf("eventstore: segment %s: %w", rel, merr)
		}
		s.segs = append(s.segs, meta)
		return nil
	})
}

// readSegmentMeta opens a segment footer and extracts pruning metadata.
func readSegmentMeta(path string, proj, hour int64) (segMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return segMeta{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return segMeta{}, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return segMeta{}, err
	}
	if v, ok := pf.Lookup("leser_schema_version"); !ok || v != strconv.Itoa(SchemaVersion) {
		return segMeta{}, fmt.Errorf("unsupported schema version %q", v)
	}
	m := segMeta{path: path, projectID: proj, hour: hour, rows: pf.NumRows()}
	minS, ok1 := pf.Lookup("leser_min_ts")
	maxS, ok2 := pf.Lookup("leser_max_ts")
	if !ok1 || !ok2 {
		return segMeta{}, errors.New("missing timestamp bounds metadata")
	}
	if m.minTS, err = strconv.ParseInt(minS, 10, 64); err != nil {
		return segMeta{}, err
	}
	if m.maxTS, err = strconv.ParseInt(maxS, 10, 64); err != nil {
		return segMeta{}, err
	}
	return m, nil
}

// Append adds events to the hot buffer. Returns ErrBufferFull at the hard cap.
// Durability is the WAL's job upstream; this is the queryable tier.
func (s *Store) Append(events ...Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buf)+len(events) > s.opts.MaxBufferRows {
		return ErrBufferFull
	}
	if len(s.buf) == 0 {
		s.bufSince = time.Now()
	}
	s.buf = append(s.buf, events...)
	return nil
}

// NeedsFlush reports whether the buffer has hit the row threshold or age.
func (s *Store) NeedsFlush(now time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.buf) >= s.opts.FlushRows {
		return true
	}
	return len(s.buf) > 0 && now.Sub(s.bufSince) >= s.opts.FlushAge
}

// Flush compacts the entire buffer into Parquet segments, one per
// (project, hour) partition present in the buffer. Crash-safety: each segment
// is written to a temp file, fsynced, then renamed — a crash leaves either the
// complete segment or nothing (plus the WAL upstream for replay).
func (s *Store) Flush() error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	rows := s.buf
	s.buf = nil
	s.mu.Unlock()
	if len(rows) == 0 {
		return nil
	}

	// Group by partition.
	type key struct {
		proj int64
		hour int64
	}
	parts := map[key][]Event{}
	for _, e := range rows {
		k := key{e.ProjectID, e.Timestamp - e.Timestamp%hourNanos}
		parts[k] = append(parts[k], e)
	}

	var newSegs []segMeta
	for k, evs := range parts {
		meta, err := s.writeSegment(k.proj, k.hour, evs)
		if err != nil {
			// Put every row back so nothing is lost from the queryable tier;
			// the caller retries the flush.
			s.mu.Lock()
			s.buf = append(rows, s.buf...) //nolint:makezero
			s.mu.Unlock()
			return err
		}
		newSegs = append(newSegs, meta)
	}

	s.mu.Lock()
	s.segs = append(s.segs, newSegs...)
	sort.Slice(s.segs, func(i, j int) bool {
		a, b := s.segs[i], s.segs[j]
		if a.projectID != b.projectID {
			return a.projectID < b.projectID
		}
		if a.hour != b.hour {
			return a.hour < b.hour
		}
		return a.path < b.path
	})
	s.mu.Unlock()
	return nil
}

// writeSegment writes one partition's rows as a Parquet file with statistics,
// Bloom filters, and versioned footer metadata.
func (s *Store) writeSegment(proj, hour int64, evs []Event) (segMeta, error) {
	sort.Slice(evs, func(i, j int) bool { return evs[i].Timestamp < evs[j].Timestamp })
	minTS, maxTS := evs[0].Timestamp, evs[len(evs)-1].Timestamp

	dir := s.partitionDir(proj, hour)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return segMeta{}, err
	}
	s.flushCount++
	final := filepath.Join(dir, fmt.Sprintf("%016x-%06d.parquet", minTS, s.flushCount))
	tmp := final + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return segMeta{}, err
	}
	w := parquet.NewGenericWriter[Event](f,
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "fingerprint"),
			parquet.SplitBlockFilter(10, "release"),
			parquet.SplitBlockFilter(10, "user_id"),
		),
		parquet.KeyValueMetadata("leser_schema_version", strconv.Itoa(SchemaVersion)),
		parquet.KeyValueMetadata("leser_min_ts", strconv.FormatInt(minTS, 10)),
		parquet.KeyValueMetadata("leser_max_ts", strconv.FormatInt(maxTS, 10)),
	)
	if _, err := w.Write(evs); err != nil {
		f.Close()
		os.Remove(tmp)
		return segMeta{}, err
	}
	if err := w.Close(); err != nil {
		f.Close()
		os.Remove(tmp)
		return segMeta{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return segMeta{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return segMeta{}, err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return segMeta{}, err
	}
	return segMeta{path: final, projectID: proj, hour: hour, minTS: minTS, maxTS: maxTS, rows: int64(len(evs))}, nil
}

// PruneStats reports cumulative (considered, opened) segment counts across all
// queries — the pruning-ratio observability hook.
func (s *Store) PruneStats() (considered, opened uint64) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	return s.statSegsTotal, s.statSegsOpen
}

// SegmentCount returns the number of on-disk segments.
func (s *Store) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segs)
}

// BufferLen returns the current hot-buffer row count.
func (s *Store) BufferLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.buf)
}
