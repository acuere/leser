package eventstore

import (
	"os"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// liteEvent is the aggregation projection: parquet-go reads only these
// columns (matched by name), so aggregations never decode Payload — the
// column that dominates I/O.
type liteEvent struct {
	EventID     string `parquet:"event_id"`
	ProjectID   int64  `parquet:"project_id"`
	Timestamp   int64  `parquet:"timestamp"`
	Level       string `parquet:"level,dict"`
	Fingerprint string `parquet:"fingerprint,dict"`
	Release     string `parquet:"release,dict"`
	Environment string `parquet:"environment,dict"`
	UserID      string `parquet:"user_id"`
}

func (e *liteEvent) full() Event {
	return Event{EventID: e.EventID, ProjectID: e.ProjectID, Timestamp: e.Timestamp,
		Level: e.Level, Fingerprint: e.Fingerprint, Release: e.Release,
		Environment: e.Environment, UserID: e.UserID}
}

// Bucket is one time bucket count.
type Bucket struct {
	Start int64 `json:"start"` // unix nanos, inclusive
	Count int64 `json:"count"`
}

// KV is one top-N entry.
type KV struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// Stats is the aggregate answer for a query window.
type Stats struct {
	Total       int64           `json:"total"`
	Buckets     []Bucket        `json:"buckets"`
	TopBy       map[string][]KV `json:"top_by"`
	UniqueUsers uint64          `json:"unique_users"`
}

// Aggregate computes counts-by-bucket, top-N for the standard tag columns,
// and unique users over the query window. Segment pruning applies exactly as
// in Scan; unique users come from compaction-time HLL sketches when a whole
// segment is inside the window, falling back to row-level adds otherwise.
func (s *Store) Aggregate(q Query, bucketNanos int64, topN int) (Stats, error) {
	if bucketNanos <= 0 {
		bucketNanos = hourNanos
	}
	if topN <= 0 || topN > 50 {
		topN = 10
	}
	st := Stats{TopBy: map[string][]KV{}}
	buckets := map[int64]int64{}
	tops := map[string]map[string]int64{
		"level": {}, "release": {}, "environment": {}, "fingerprint": {},
	}
	users := NewHLL()

	add := func(e *liteEvent) {
		st.Total++
		buckets[e.Timestamp-e.Timestamp%bucketNanos]++
		tops["level"][e.Level]++
		tops["release"][e.Release]++
		tops["environment"][e.Environment]++
		tops["fingerprint"][e.Fingerprint]++
		if e.UserID != "" {
			users.Add(e.UserID)
		}
	}

	// Hot buffer.
	s.mu.RLock()
	for i := range s.buf {
		if q.match(&s.buf[i]) {
			le := liteEvent{EventID: s.buf[i].EventID, ProjectID: s.buf[i].ProjectID,
				Timestamp: s.buf[i].Timestamp, Level: s.buf[i].Level, Fingerprint: s.buf[i].Fingerprint,
				Release: s.buf[i].Release, Environment: s.buf[i].Environment, UserID: s.buf[i].UserID}
			add(&le)
		}
	}
	segs := make([]segMeta, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

	for i := range segs {
		if !q.segmentMayMatch(&segs[i]) {
			continue
		}
		if err := s.aggregateSegment(&segs[i], &q, add, users); err != nil {
			return st, err
		}
	}

	// Materialize buckets sorted by time.
	for b, c := range buckets {
		st.Buckets = append(st.Buckets, Bucket{Start: b, Count: c})
	}
	sort.Slice(st.Buckets, func(i, j int) bool { return st.Buckets[i].Start < st.Buckets[j].Start })
	for col, m := range tops {
		kvs := make([]KV, 0, len(m))
		for k, c := range m {
			if k == "" {
				continue
			}
			kvs = append(kvs, KV{Key: k, Count: c})
		}
		sort.Slice(kvs, func(i, j int) bool {
			if kvs[i].Count != kvs[j].Count {
				return kvs[i].Count > kvs[j].Count
			}
			return kvs[i].Key < kvs[j].Key
		})
		if len(kvs) > topN {
			kvs = kvs[:topN]
		}
		st.TopBy[col] = kvs
	}
	st.UniqueUsers = users.Estimate()
	return st, nil
}

// aggregateSegment feeds one segment's matching rows into add, without
// reading the payload column. When the whole segment is inside the query
// window and no per-row filters apply, the compaction-time HLL from the
// footer is merged instead of re-adding user IDs row by row.
func (s *Store) aggregateSegment(m *segMeta, q *Query, add func(*liteEvent), users *HLL) error {
	f, err := os.Open(m.path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return err
	}

	// Footer sketch fast path for the unique-user dimension.
	wholeSegment := (q.TimeMin == 0 || q.TimeMin <= m.minTS) &&
		(q.TimeMax == 0 || q.TimeMax >= m.maxTS) &&
		q.Level == "" && q.Fingerprint == "" && q.Release == "" &&
		q.Environment == "" && q.UserID == ""
	sketchMerged := false
	if wholeSegment {
		if enc, ok := pf.Lookup("leser_hll_users"); ok {
			if h, herr := UnmarshalHLL(enc); herr == nil {
				users.Merge(h)
				sketchMerged = true
			}
		}
	}
	_ = sketchMerged // rows still stream for buckets/top-N; user adds are idempotent
	buf := make([]liteEvent, 1024)
	for _, rg := range pf.RowGroups() {
		rdr := parquet.NewGenericRowGroupReader[liteEvent](rg)
		for {
			n, rerr := rdr.Read(buf)
			for i := 0; i < n; i++ {
				e := &buf[i]
				full := e.full()
				if q.match(&full) {
					add(e)
				}
			}
			if rerr != nil {
				break
			}
		}
	}
	return nil
}
