package alerts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"leser/internal/logging"
)

// staticRules is a fixed RuleSource.
type staticRules struct{ rules []Rule }

func (s staticRules) EnabledRules(context.Context, int64) ([]Rule, error) {
	return s.rules, nil
}

func newEngine(t *testing.T, rules []Rule, opts Options) (*Engine, context.CancelFunc) {
	t.Helper()
	opts.HTTPTimeout = 2 * time.Second
	e := New(logging.New("error"), staticRules{rules}, opts)
	e.sleep = func(time.Duration) {} // no real backoff waits in tests
	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)
	t.Cleanup(cancel)
	return e, cancel
}

func waitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestNewIssueDelivery(t *testing.T) {
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Add(1)
	}))
	defer srv.Close()

	e, _ := newEngine(t, []Rule{{ID: 1, Name: "new", Condition: CondNewIssue, WebhookURL: srv.URL, Enabled: true}}, Options{})
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 10, Title: "boom", IsNew: true})
	waitCond(t, "delivery", func() bool { return got.Load() == 1 })

	// Non-matching activity: no delivery.
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 11, IsNew: false})
	time.Sleep(50 * time.Millisecond)
	if got.Load() != 1 {
		t.Fatalf("non-matching activity delivered: %d", got.Load())
	}
}

func TestCooldownDedup(t *testing.T) {
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { got.Add(1) }))
	defer srv.Close()

	e, _ := newEngine(t, []Rule{{ID: 1, Name: "reg", Condition: CondRegression, WebhookURL: srv.URL, Enabled: true}},
		Options{Cooldown: time.Hour})
	for i := 0; i < 5; i++ {
		e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 7, Regressed: true})
	}
	waitCond(t, "first delivery", func() bool { return got.Load() >= 1 })
	time.Sleep(50 * time.Millisecond)
	if got.Load() != 1 {
		t.Fatalf("cooldown failed: %d deliveries for the same (rule,issue)", got.Load())
	}
	// A different issue is not deduped.
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 8, Regressed: true})
	waitCond(t, "second issue delivery", func() bool { return got.Load() == 2 })
}

func TestFrequencyFiresOnceAtCrossing(t *testing.T) {
	var got atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { got.Add(1) }))
	defer srv.Close()

	e, _ := newEngine(t, []Rule{{ID: 1, Name: "freq", Condition: CondFrequency, Threshold: 3, WebhookURL: srv.URL, Enabled: true}},
		Options{Cooldown: time.Millisecond})
	for seen := int64(1); seen <= 5; seen++ {
		e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 9, TimesSeen: seen})
		time.Sleep(2 * time.Millisecond) // let cooldown lapse so only the condition gates
	}
	time.Sleep(100 * time.Millisecond)
	if got.Load() != 1 {
		t.Fatalf("frequency fired %d times, want exactly 1 (at crossing)", got.Load())
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	e, _ := newEngine(t, []Rule{{ID: 1, Name: "dead", Condition: CondNewIssue, WebhookURL: srv.URL, Enabled: true}},
		Options{MaxAttempts: 3, BreakerFails: 100}) // breaker out of the way
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 1, IsNew: true})
	waitCond(t, "dead letter", func() bool { return len(e.DeadLetters()) == 1 })
	if attempts.Load() != 3 {
		t.Fatalf("attempts %d, want 3", attempts.Load())
	}
	dl := e.DeadLetters()[0]
	if dl.Rule.Name != "dead" || dl.Error == "" {
		t.Fatalf("dead letter malformed: %+v", dl)
	}
}

func TestCircuitBreakerOpens(t *testing.T) {
	var attempts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	e, _ := newEngine(t, []Rule{{ID: 1, Name: "cb", Condition: CondNewIssue, WebhookURL: srv.URL, Enabled: true}},
		Options{MaxAttempts: 2, BreakerFails: 2, BreakerReset: time.Hour, Cooldown: time.Millisecond})

	// First notification: 2 attempts, breaker opens after the 2nd failure.
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 1, IsNew: true})
	waitCond(t, "first dead letter", func() bool { return len(e.DeadLetters()) == 1 })
	base := attempts.Load()

	// Second notification: breaker open → no HTTP attempts at all.
	time.Sleep(5 * time.Millisecond)
	e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 2, IsNew: true})
	waitCond(t, "second dead letter", func() bool { return len(e.DeadLetters()) == 2 })
	if attempts.Load() != base {
		t.Fatalf("breaker leaked %d HTTP attempts while open", attempts.Load()-base)
	}
}

func TestQueueFullDropsCounted(t *testing.T) {
	// No workers running: queue fills, further evaluations drop + count.
	e := New(logging.New("error"), staticRules{[]Rule{{ID: 1, Name: "q", Condition: CondNewIssue, WebhookURL: "http://127.0.0.1:1", Enabled: true}}},
		Options{QueueDepth: 2, Cooldown: time.Nanosecond})
	e.sleep = func(time.Duration) {}
	for i := int64(0); i < 5; i++ {
		e.Evaluate(context.Background(), IssueActivity{ProjectID: 1, IssueID: 100 + i, IsNew: true})
	}
	e.mu.Lock()
	dropped := e.Dropped
	e.mu.Unlock()
	if dropped != 3 {
		t.Fatalf("dropped %d, want 3 (queue depth 2)", dropped)
	}
}
