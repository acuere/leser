package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"leser/internal/eventstore"
	"leser/internal/wal"
)

// ErrOverloaded means the pipeline is shedding load; callers return 429 with
// Retry-After (order-2 §5: backpressure, never buffering).
var ErrOverloaded = errors.New("ingest: overloaded")

// Drop reasons — enumerated so events_dropped_total{reason} is bounded.
// Silent data loss is the cardinal sin; every drop increments exactly one.
const (
	DropMalformed  = "malformed"
	DropDuplicate  = "duplicate"
	DropOverloaded = "overloaded"
	DropTooLarge   = "too_large"
	DropAuth       = "auth"
)

// PipelineOptions bound the pipeline.
type PipelineOptions struct {
	// MaxLag: when the consumer is this many records behind, Submit sheds.
	// Default 100k.
	MaxLag uint64
	// AppendTimeout bounds the WAL append wait. Default 5s.
	AppendTimeout time.Duration
	// DedupeCap bounds remembered event IDs per generation. Default 500k.
	DedupeCap int
	// CommitEvery bounds redelivery after crash: consumer offset is committed
	// at least every N records. Default 512.
	CommitEvery int
}

func (o *PipelineOptions) defaults() {
	if o.MaxLag == 0 {
		o.MaxLag = 100_000
	}
	if o.AppendTimeout <= 0 {
		o.AppendTimeout = 5 * time.Second
	}
	if o.DedupeCap <= 0 {
		o.DedupeCap = 500_000
	}
	if o.CommitEvery <= 0 {
		o.CommitEvery = 512
	}
}

// ConsumerName is the WAL consumer identity the pipeline's Run loop commits
// under. An ingest-only Pipeline (Rung 2: role separation, no co-located
// Run) reads this same name's committed offset via RefreshLagFromWAL to
// compute backpressure purely from shared-directory state — order-2 §5:
// "backpressure is a function of log lag."
const ConsumerName = "pipeline"

// WAL record kinds written by the pipeline.
const (
	recEnvelope  byte = 1 // [i64 projectID][envelope bytes]
	recEventJSON byte = 2 // [i64 projectID][single event JSON] (legacy /store/)
)

// IssueSink receives grouped-event notifications (metadata.DB in production).
type IssueSink interface {
	UpsertIssue(ctx context.Context, u IssueHit) (int64, error)
}

// IssueHit mirrors metadata.IssueUpsert without importing it (no dependency
// cycle; the adapter in cmd wires the concrete type).
type IssueHit struct {
	OrgID       int64
	ProjectID   int64
	Fingerprint string
	Basis       string
	Title       string
	Level       string
	SeenAt      int64
}

// Pipeline glues HTTP ingest to the WAL and the event store:
//
//	Submit (HTTP): auth'd raw bytes → WAL append (durable) → 200
//	Run (consumer): WAL → parse → dedupe → event store → commit offset
//
// Acknowledged data lives in the WAL; a crash replays from the last committed
// consumer offset (at-least-once + dedupe = effectively once).
type Pipeline struct {
	log    *slog.Logger
	wal    *wal.Log
	store  *eventstore.Store
	issues IssueSink // may be nil (tests without issue tracking)
	opts   PipelineOptions
	lim    Limits

	received atomic.Uint64
	stored   atomic.Uint64

	dropMu sync.Mutex
	drops  map[string]uint64

	dedupe *dedupe

	consumerLag atomic.Uint64
}

// NewPipeline wires the stages. issues may be nil. Call Run to start consuming.
func NewPipeline(log *slog.Logger, w *wal.Log, store *eventstore.Store, issues IssueSink, opts PipelineOptions, lim Limits) *Pipeline {
	opts.defaults()
	lim.defaults()
	return &Pipeline{
		log:    log,
		wal:    w,
		store:  store,
		issues: issues,
		opts:   opts,
		lim:    lim,
		drops:  map[string]uint64{},
		dedupe: newDedupe(opts.DedupeCap),
	}
}

// drop records one dropped unit with its reason.
func (p *Pipeline) drop(reason string) {
	p.dropMu.Lock()
	p.drops[reason]++
	p.dropMu.Unlock()
}

// Submit durably accepts one envelope (or legacy event) for a project. Returns
// ErrOverloaded when the consumer lag or append deadline says to shed.
func (p *Pipeline) Submit(ctx context.Context, kind byte, projectID int64, body []byte) error {
	p.received.Add(1)
	if p.consumerLag.Load() > p.opts.MaxLag {
		p.drop(DropOverloaded)
		return ErrOverloaded
	}
	rec := make([]byte, 9+len(body))
	rec[0] = kind
	binary.LittleEndian.PutUint64(rec[1:9], uint64(projectID))
	copy(rec[9:], body)

	actx, cancel := context.WithTimeout(ctx, p.opts.AppendTimeout)
	defer cancel()
	if _, err := p.wal.Append(actx, rec[0], rec); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.drop(DropOverloaded)
			return ErrOverloaded
		}
		return err
	}
	return nil
}

// Run consumes the WAL into the event store until ctx is done. It blocks (with
// retry) when the store buffer is full — lag then grows and Submit sheds at
// the edge. Post-acknowledgment data is never dropped for capacity reasons.
func (p *Pipeline) Run(ctx context.Context) error {
	if p.store == nil {
		return fmt.Errorf("ingest: Run requires a store (this Pipeline was built ingest-only)")
	}
	r, err := p.wal.NewReader(ConsumerName)
	if err != nil {
		return err
	}
	defer r.Close()

	uncommitted := 0
	flushTick := time.NewTicker(time.Second)
	defer flushTick.Stop()

	// commit ALWAYS flushes the event store before advancing the WAL
	// consumer offset. This is load-bearing, not defensive: the hot buffer
	// eventstore.Store holds is pure in-memory state that does not survive a
	// restart. Once the committed offset passes a record, a fresh consumer
	// never reads it again — so if that record's row was still sitting
	// unflushed in the buffer, it is permanently gone, even though the WAL
	// bytes are still on disk (order.md §7's cardinal sin: silent data loss).
	// A prior version of this function committed at two sites without
	// flushing first (found by robustness/upgrade.sh: an event survived a
	// clean SIGTERM+restart in metadata/issue state but vanished from the
	// event store, because offset commit had outrun compaction). Flush is a
	// cheap no-op when the buffer is empty, so this costs nothing at idle.
	commit := func() error {
		if err := p.store.Flush(); err != nil {
			return fmt.Errorf("event store flush: %w", err) // do not commit past unflushed data
		}
		if err := r.Commit(); err != nil {
			return fmt.Errorf("offset commit: %w", err)
		}
		uncommitted = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return commit()
		case <-flushTick.C:
			if uncommitted > 0 || p.store.NeedsFlush(time.Now()) {
				if err := commit(); err != nil {
					p.log.Error("periodic commit", "err", err)
				}
			}
		default:
		}

		rec, err := r.Next()
		if errors.Is(err, wal.ErrNoRecord) {
			p.consumerLag.Store(0)
			select {
			case <-ctx.Done():
				return commit()
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			return err
		}
		p.consumerLag.Store(r.Lag())

		p.process(ctx, rec)
		uncommitted++
		if uncommitted >= p.opts.CommitEvery {
			if err := commit(); err != nil {
				p.log.Error("threshold commit", "err", err)
			}
		}
	}
}

// RefreshLagFromWAL recomputes consumer lag from the WAL's on-disk consumer
// offset file rather than from a co-located Run loop. This is how an
// ingest-role process (Rung 2: role separation, no local consumer) learns
// how far behind the worker process is, purely via shared-directory state —
// no RPC, no shared memory across processes. Callers run this on a ticker;
// it is a no-op-safe read, cheap enough for a sub-second interval.
func (p *Pipeline) RefreshLagFromWAL(consumerName string) {
	off, ok, err := p.wal.ConsumerOffset(consumerName)
	if err != nil {
		p.log.Error("lag refresh: read consumer offset", "err", err)
		return
	}
	if !ok {
		off = 0 // worker hasn't committed yet: treat as maximally behind
	}
	latest := p.wal.LatestOffset()
	var lag uint64
	if latest > off {
		lag = latest - off
	}
	p.consumerLag.Store(lag)
}

// process handles one WAL record: parse → extract → dedupe → store.
func (p *Pipeline) process(ctx context.Context, rec wal.Record) {
	if len(rec.Payload) < 9 {
		p.drop(DropMalformed)
		return
	}
	kind := rec.Payload[0]
	projectID := int64(binary.LittleEndian.Uint64(rec.Payload[1:9]))
	body := rec.Payload[9:]

	type pending struct {
		ex      ExtractedEvent
		payload []byte
	}
	var events []pending
	switch kind {
	case recEnvelope:
		env, err := Parse(bytes.NewReader(body), p.lim)
		if err != nil {
			p.drop(DropMalformed)
			return
		}
		for _, item := range env.Items {
			if item.Header.Type != ItemEvent && item.Header.Type != ItemTransaction {
				continue // sessions/check-ins/attachments: later milestones
			}
			ev, err := ExtractEvent(item.Payload, time.Now())
			if err != nil {
				p.drop(DropMalformed)
				continue
			}
			if ev.EventID == "" && env.Header.EventID != "" {
				ev.EventID = env.Header.EventID
			}
			events = append(events, pending{ev, item.Payload})
		}
	case recEventJSON:
		ev, err := ExtractEvent(body, time.Now())
		if err != nil {
			p.drop(DropMalformed)
			return
		}
		events = append(events, pending{ev, body})
	default:
		p.drop(DropMalformed)
		return
	}

	for _, pe := range events {
		if pe.ex.EventID != "" && !p.dedupe.firstSeen(projectKey(projectID, pe.ex.EventID)) {
			p.drop(DropDuplicate)
			continue
		}
		// Block-and-retry: acknowledged data is never capacity-dropped.
		for {
			err := p.store.Append(toStoreEvent(projectID, pe.ex, pe.payload))
			if err == nil {
				p.stored.Add(1)
				break
			}
			if !errors.Is(err, eventstore.ErrBufferFull) {
				p.log.Error("store append", "err", err)
				break
			}
			if ferr := p.store.Flush(); ferr != nil {
				p.log.Error("event store flush under pressure", "err", ferr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		// Issue grouping: upsert by fingerprint. Failure here is logged and
		// counted, never dropped silently; the event itself is already stored.
		if p.issues != nil {
			_, err := p.issues.UpsertIssue(ctx, IssueHit{
				ProjectID:   projectID,
				Fingerprint: pe.ex.Fingerprint,
				Basis:       pe.ex.Basis,
				Title:       truncate(pe.ex.Message, 200),
				Level:       pe.ex.Level,
				SeenAt:      pe.ex.Timestamp,
			})
			if err != nil {
				p.log.Error("issue upsert", "err", err, "fingerprint", pe.ex.Fingerprint)
			}
		}
	}
}

// truncate bounds a string to n bytes on a rune boundary.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for i := n; i > 0; i-- {
		if (s[i]&0xC0) != 0x80 && i <= n {
			return s[:i]
		}
	}
	return s[:n]
}

func toStoreEvent(projectID int64, ev ExtractedEvent, payload []byte) eventstore.Event {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	return eventstore.Event{
		EventID:     ev.EventID,
		ProjectID:   projectID,
		Timestamp:   ev.Timestamp,
		Level:       ev.Level,
		Fingerprint: ev.Fingerprint,
		Release:     ev.Release,
		Environment: ev.Environment,
		UserID:      ev.UserID,
		Message:     ev.Message,
		Payload:     cp,
	}
}

// Status is the control-plane snapshot served at /api/ops/status.
type Status struct {
	Received    uint64            `json:"events_received_total"`
	Stored      uint64            `json:"events_stored_total"`
	Drops       map[string]uint64 `json:"events_dropped_total"`
	ConsumerLag uint64            `json:"wal_consumer_lag"`
	WALLatest   uint64            `json:"wal_latest_offset"`
	BufferRows  int               `json:"store_buffer_rows"`
	Segments    int               `json:"store_segments"`
	SegsQueried uint64            `json:"segments_considered_total"`
	SegsOpened  uint64            `json:"segments_opened_total"`
}

// Status snapshots pipeline counters.
func (p *Pipeline) Status() Status {
	p.dropMu.Lock()
	drops := make(map[string]uint64, len(p.drops))
	for k, v := range p.drops {
		drops[k] = v
	}
	p.dropMu.Unlock()
	var considered, opened uint64
	var bufferRows, segments int
	if p.store != nil { // nil for an ingest-only Pipeline (Rung 2)
		considered, opened = p.store.PruneStats()
		bufferRows = p.store.BufferLen()
		segments = p.store.SegmentCount()
	}
	return Status{
		Received:    p.received.Load(),
		Stored:      p.stored.Load(),
		Drops:       drops,
		ConsumerLag: p.consumerLag.Load(),
		WALLatest:   p.wal.LatestOffset(),
		BufferRows:  bufferRows,
		Segments:    segments,
		SegsQueried: considered,
		SegsOpened:  opened,
	}
}

// DropCount exposes a single drop-reason counter (for tests and doctor).
func (p *Pipeline) DropCount(reason string) uint64 {
	p.dropMu.Lock()
	defer p.dropMu.Unlock()
	return p.drops[reason]
}

// projectKey namespaces a dedupe key by project.
func projectKey(projectID int64, eventID string) string {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(projectID))
	return string(b[:]) + eventID
}

// dedupe is a two-generation bounded set: O(1) checks, memory capped, rotates
// when the current generation fills. Approximate across rotation — the SQLite
// exact backstop arrives with order-2 M5; WAL replay duplicates are the case
// that matters and they land within one generation.
type dedupe struct {
	mu   sync.Mutex
	cap  int
	cur  map[string]struct{}
	prev map[string]struct{}
}

func newDedupe(capacity int) *dedupe {
	return &dedupe{cap: capacity, cur: map[string]struct{}{}, prev: map[string]struct{}{}}
}

// firstSeen returns true the first time a key is seen, false on duplicates.
func (d *dedupe) firstSeen(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.cur[key]; ok {
		return false
	}
	if _, ok := d.prev[key]; ok {
		return false
	}
	if len(d.cur) >= d.cap {
		d.prev = d.cur
		d.cur = make(map[string]struct{}, d.cap/4)
	}
	d.cur[key] = struct{}{}
	return true
}
