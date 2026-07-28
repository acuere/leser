package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"leser/internal/eventstore"
	"leser/internal/logging"
	"leser/internal/metadata"
	"leser/internal/wal"
)

// --- envelope parser ---

func TestParseEnvelopeBasic(t *testing.T) {
	event := `{"event_id":"abc","message":"boom"}`
	raw := fmt.Sprintf("{\"event_id\":\"abc\"}\n{\"type\":\"event\",\"length\":%d}\n%s\n", len(event), event)
	env, err := Parse(strings.NewReader(raw), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Items) != 1 || env.Items[0].Header.Type != "event" {
		t.Fatalf("items: %+v", env.Items)
	}
	if string(env.Items[0].Payload) != event {
		t.Fatalf("payload mismatch: %q", env.Items[0].Payload)
	}
}

func TestParseEnvelopeNoLengthItem(t *testing.T) {
	raw := "{}\n{\"type\":\"event\"}\n{\"message\":\"implicit length\"}\n"
	env, err := Parse(strings.NewReader(raw), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Items) != 1 || !strings.Contains(string(env.Items[0].Payload), "implicit") {
		t.Fatalf("items: %+v", env.Items)
	}
}

func TestParseEnvelopeUnknownTypeSkipped(t *testing.T) {
	raw := "{}\n{\"type\":\"weird_future_thing\"}\n{\"x\":1}\n{\"type\":\"event\"}\n{\"m\":2}\n"
	env, err := Parse(strings.NewReader(raw), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if len(env.Items) != 1 || env.Items[0].Header.Type != "event" {
		t.Fatalf("unknown type not skipped: %+v", env.Items)
	}
}

func TestParseEnvelopeBounds(t *testing.T) {
	// Item count bound.
	var sb strings.Builder
	sb.WriteString("{}\n")
	for i := 0; i < 60; i++ {
		sb.WriteString("{\"type\":\"event\"}\n{}\n")
	}
	if _, err := Parse(strings.NewReader(sb.String()), Limits{MaxItems: 50}); err == nil {
		t.Fatal("expected item-count violation")
	}
	// Declared length beyond cap.
	raw := "{}\n{\"type\":\"event\",\"length\":99999999}\nx\n"
	if _, err := Parse(strings.NewReader(raw), Limits{MaxItemBytes: 1024}); err == nil {
		t.Fatal("expected length violation")
	}
	// Truncated declared payload.
	raw = "{}\n{\"type\":\"event\",\"length\":100}\nshort"
	if _, err := Parse(strings.NewReader(raw), Limits{}); err == nil {
		t.Fatal("expected truncation error")
	}
}

func FuzzParseEnvelope(f *testing.F) {
	f.Add([]byte("{}\n{\"type\":\"event\",\"length\":2}\n{}\n"))
	f.Add([]byte("{\"event_id\":\"a\"}\n{\"type\":\"session\"}\n{}\n"))
	f.Add([]byte(""))
	f.Add([]byte("\n\n\n"))
	f.Add(bytes.Repeat([]byte("{\"type\":\"event\"}\n"), 100))
	f.Fuzz(func(t *testing.T, data []byte) {
		// Must never panic and never allocate beyond bounds.
		env, err := Parse(bytes.NewReader(data), Limits{MaxEnvelopeBytes: 1 << 20, MaxItems: 20, MaxItemBytes: 1 << 16})
		if err == nil && len(env.Items) > 20 {
			t.Fatal("item bound violated")
		}
	})
}

// --- event extraction ---

func TestExtractEvent(t *testing.T) {
	now := time.Now()
	payload := []byte(`{"event_id":"DEAD-BEEF","timestamp":"2026-07-28T10:00:00Z","level":"warning",
		"release":"1.2.3","environment":"prod","user":{"id":"u1"},
		"exception":{"values":[{"type":"ValueError","value":"bad input"}]}}`)
	ev, err := ExtractEvent(payload, now)
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventID != "deadbeef" {
		t.Errorf("event id: %q", ev.EventID)
	}
	if ev.Message != "ValueError: bad input" {
		t.Errorf("message: %q", ev.Message)
	}
	if ev.Fingerprint == "" {
		t.Error("fingerprint empty")
	}
	// Same exception → same fingerprint (deterministic).
	ev2, _ := ExtractEvent(payload, now.Add(time.Hour))
	if ev2.Fingerprint != ev.Fingerprint {
		t.Error("fingerprint not deterministic")
	}
}

// --- pipeline end to end ---

type fakeKeys struct{ key metadata.ProjectKey }

func (f fakeKeys) LookupKey(_ context.Context, pub string) (metadata.ProjectKey, error) {
	if pub == f.key.PublicKey {
		return f.key, nil
	}
	return metadata.ProjectKey{}, metadata.ErrNotFound
}

func newTestStack(t *testing.T) (*Pipeline, *Handler, *eventstore.Store, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	w, err := wal.Open(dir+"/wal", wal.Options{BatchWindow: time.Microsecond})
	if err != nil {
		t.Fatal(err)
	}
	store, err := eventstore.Open(dir+"/events", eventstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	pipe := NewPipeline(logging.New("error"), w, store, nil, PipelineOptions{}, Limits{})
	h := NewHandler(pipe, fakeKeys{metadata.ProjectKey{ProjectID: 42, OrgID: 1, PublicKey: "goodkey", Active: true}}, Limits{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = pipe.Run(ctx) }()
	// Await the consumer before TempDir cleanup: Run commits its offset file
	// on ctx.Done and must not race directory removal.
	t.Cleanup(func() { cancel(); <-done; w.Close() })
	return pipe, h, store, cancel
}

func postEnvelope(h *Handler, project, key, body string, encode func([]byte) ([]byte, string)) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	h.Register(mux)
	data := []byte(body)
	enc := ""
	if encode != nil {
		data, enc = encode(data)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/"+project+"/envelope/", bytes.NewReader(data))
	req.Header.Set("X-Sentry-Auth", "Sentry sentry_key="+key+", sentry_version=7")
	if enc != "" {
		req.Header.Set("Content-Encoding", enc)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func envBody(eventID, msg string) string {
	event := fmt.Sprintf(`{"event_id":%q,"message":%q,"timestamp":"2026-07-28T12:00:00Z"}`, eventID, msg)
	return fmt.Sprintf("{\"event_id\":%q}\n{\"type\":\"event\",\"length\":%d}\n%s\n", eventID, len(event), event)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached in 5s")
}

func TestIngestEndToEnd(t *testing.T) {
	pipe, h, store, _ := newTestStack(t)

	rr := postEnvelope(h, "42", "goodkey", envBody("11111111111111111111111111111111", "e2e boom"), nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	waitFor(t, func() bool { return pipe.Status().Stored == 1 })

	var got []eventstore.Event
	err := store.Scan(eventstore.Query{ProjectID: 42}, func(e eventstore.Event) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Message != "e2e boom" {
		t.Fatalf("stored: %+v", got)
	}
}

func TestIngestGzip(t *testing.T) {
	pipe, h, _, _ := newTestStack(t)
	rr := postEnvelope(h, "42", "goodkey", envBody("22222222222222222222222222222222", "gz"), func(b []byte) ([]byte, string) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		gw.Write(b)
		gw.Close()
		return buf.Bytes(), "gzip"
	})
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	waitFor(t, func() bool { return pipe.Status().Stored == 1 })
}

func TestIngestAuthRejections(t *testing.T) {
	pipe, h, _, _ := newTestStack(t)
	// Wrong key.
	if rr := postEnvelope(h, "42", "badkey", envBody("3", "x"), nil); rr.Code != http.StatusForbidden {
		t.Fatalf("wrong key: %d", rr.Code)
	}
	// Right key, wrong project.
	if rr := postEnvelope(h, "41", "goodkey", envBody("3", "x"), nil); rr.Code != http.StatusForbidden {
		t.Fatalf("cross-project key: %d", rr.Code)
	}
	// No key at all.
	mux := http.NewServeMux()
	h.Register(mux)
	req := httptest.NewRequest(http.MethodPost, "/api/42/envelope/", strings.NewReader("{}"))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no key: %d", rr.Code)
	}
	if pipe.DropCount(DropAuth) != 3 {
		t.Fatalf("auth drops: %d want 3", pipe.DropCount(DropAuth))
	}
}

func TestIngestDedupe(t *testing.T) {
	pipe, h, _, _ := newTestStack(t)
	body := envBody("44444444444444444444444444444444", "dup")
	for i := 0; i < 3; i++ {
		if rr := postEnvelope(h, "42", "goodkey", body, nil); rr.Code != http.StatusOK {
			t.Fatalf("send %d: %d", i, rr.Code)
		}
	}
	waitFor(t, func() bool { return pipe.DropCount(DropDuplicate) == 2 })
	if pipe.Status().Stored != 1 {
		t.Fatalf("stored %d want 1 (dedupe by event_id)", pipe.Status().Stored)
	}
}

func TestIngestBackpressure429(t *testing.T) {
	pipe, h, _, _ := newTestStack(t)
	// Force shed by faking a huge consumer lag.
	pipe.consumerLag.Store(pipe.opts.MaxLag + 1)
	rr := postEnvelope(h, "42", "goodkey", envBody("5", "x"), nil)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d want 429", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" || rr.Header().Get("X-Sentry-Rate-Limits") == "" {
		t.Fatal("backpressure headers missing")
	}
}

func TestLegacyStoreEndpoint(t *testing.T) {
	pipe, h, store, _ := newTestStack(t)
	mux := http.NewServeMux()
	h.Register(mux)
	event := `{"event_id":"66666666666666666666666666666666","message":"legacy"}`
	req := httptest.NewRequest(http.MethodPost, "/api/42/store/?sentry_key=goodkey", strings.NewReader(event))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	waitFor(t, func() bool { return pipe.Status().Stored == 1 })
	n := 0
	store.Scan(eventstore.Query{ProjectID: 42}, func(eventstore.Event) error { n++; return nil })
	if n != 1 {
		t.Fatalf("stored rows %d", n)
	}
}
