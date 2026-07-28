package eventstore

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

// Query is the predicate set for the product's query shapes. Zero values mean
// "no constraint" except ProjectID, which is required — every read is tenant-
// scoped, there is no cross-project scan path at this layer.
type Query struct {
	ProjectID   int64
	TimeMin     int64 // unix nanos, inclusive; 0 = unbounded
	TimeMax     int64 // unix nanos, inclusive; 0 = unbounded
	Level       string
	Fingerprint string
	Release     string
	Environment string
	UserID      string
	Limit       int // 0 = Options.QueryLimitDefault
}

// match applies the row-level predicate.
func (q *Query) match(e *Event) bool {
	if e.ProjectID != q.ProjectID {
		return false
	}
	if q.TimeMin != 0 && e.Timestamp < q.TimeMin {
		return false
	}
	if q.TimeMax != 0 && e.Timestamp > q.TimeMax {
		return false
	}
	if q.Level != "" && e.Level != q.Level {
		return false
	}
	if q.Fingerprint != "" && e.Fingerprint != q.Fingerprint {
		return false
	}
	if q.Release != "" && e.Release != q.Release {
		return false
	}
	if q.Environment != "" && e.Environment != q.Environment {
		return false
	}
	if q.UserID != "" && e.UserID != q.UserID {
		return false
	}
	return true
}

// segmentMayMatch is the segment-level pruning decision from manifest metadata
// alone — no file open. Pruning is the whole game (order-2 §2.3).
func (q *Query) segmentMayMatch(m *segMeta) bool {
	if m.projectID != q.ProjectID {
		return false
	}
	if q.TimeMin != 0 && m.maxTS < q.TimeMin {
		return false
	}
	if q.TimeMax != 0 && m.minTS > q.TimeMax {
		return false
	}
	return true
}

// Scan runs the query, merging the hot buffer with pruned segment scans, and
// calls fn for each matching event until the limit. Results are not globally
// time-ordered across tiers; callers sort the (bounded) result set.
func (s *Store) Scan(q Query, fn func(Event) error) error {
	if q.ProjectID == 0 {
		return fmt.Errorf("eventstore: query requires ProjectID (tenant scope)")
	}
	limit := q.Limit
	if limit <= 0 {
		limit = s.opts.QueryLimitDefault
	}
	emitted := 0

	// Hot tier: snapshot matching buffer rows under read lock.
	s.mu.RLock()
	var hot []Event
	for i := range s.buf {
		if q.match(&s.buf[i]) {
			hot = append(hot, s.buf[i])
			if len(hot) >= limit {
				break
			}
		}
	}
	segs := make([]segMeta, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()

	for i := range hot {
		if emitted >= limit {
			return nil
		}
		if err := fn(hot[i]); err != nil {
			return err
		}
		emitted++
	}

	// Warm tier: prune, then scan survivors.
	considered, opened := uint64(0), uint64(0)
	defer func() {
		s.statsMu.Lock()
		s.statSegsTotal += considered
		s.statSegsOpen += opened
		s.statsMu.Unlock()
	}()

	for i := range segs {
		considered++
		if !q.segmentMayMatch(&segs[i]) {
			continue
		}
		if emitted >= limit {
			return nil
		}
		opened++
		n, err := s.scanSegment(&segs[i], &q, limit-emitted, fn)
		if err != nil {
			return err
		}
		emitted += n
	}
	return nil
}

// scanSegment scans one Parquet segment with row-group statistics and Bloom
// filter pruning, then row-level filtering.
func (s *Store) scanSegment(m *segMeta, q *Query, limit int, fn func(Event) error) (int, error) {
	f, err := os.Open(m.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, fmt.Errorf("eventstore: open %s: %w", m.path, err)
	}
	schema := pf.Schema()
	tsCol, _ := schema.Lookup("timestamp")
	fpCol, _ := schema.Lookup("fingerprint")
	relCol, _ := schema.Lookup("release")
	uidCol, _ := schema.Lookup("user_id")

	emitted := 0
	buf := make([]Event, 1024)
	for _, rg := range pf.RowGroups() {
		if emitted >= limit {
			break
		}
		chunks := rg.ColumnChunks()

		// Row-group pruning: timestamp min/max from the column index.
		if q.TimeMin != 0 || q.TimeMax != 0 {
			if ci, _ := chunks[tsCol.ColumnIndex].ColumnIndex(); ci != nil && ci.NumPages() > 0 {
				rgMin := ci.MinValue(0).Int64()
				rgMax := ci.MaxValue(ci.NumPages() - 1).Int64()
				if (q.TimeMax != 0 && rgMin > q.TimeMax) || (q.TimeMin != 0 && rgMax < q.TimeMin) {
					continue
				}
			}
		}
		// Bloom filter pruning on high-selectivity equality predicates.
		skip := false
		for _, bf := range []struct {
			col  parquet.LeafColumn
			want string
		}{
			{fpCol, q.Fingerprint},
			{relCol, q.Release},
			{uidCol, q.UserID},
		} {
			if bf.want == "" {
				continue
			}
			filter := chunks[bf.col.ColumnIndex].BloomFilter()
			if filter == nil {
				continue
			}
			if ok, ferr := filter.Check(parquet.ValueOf(bf.want)); ferr == nil && !ok {
				skip = true
				break
			}
		}
		if skip {
			continue
		}

		rdr := parquet.NewGenericRowGroupReader[Event](rg)
		for emitted < limit {
			n, rerr := rdr.Read(buf)
			for i := 0; i < n && emitted < limit; i++ {
				if q.match(&buf[i]) {
					if err := fn(buf[i]); err != nil {
						return emitted, err
					}
					emitted++
				}
			}
			if rerr != nil {
				break // io.EOF ends the row group
			}
		}
	}
	return emitted, nil
}
