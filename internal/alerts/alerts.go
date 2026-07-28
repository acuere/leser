// Package alerts is the alert rules engine (order.md milestone 6): rules
// evaluate issue activity; notifications go out through a bounded worker pool
// with retries (exponential backoff + full jitter), a per-endpoint circuit
// breaker, and per-(rule,issue) dedup windows. A dead webhook must never
// degrade ingest — everything here is async and bounded.
package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Condition types.
const (
	CondNewIssue   = "new_issue"  // first occurrence of a group
	CondRegression = "regression" // resolved issue saw a new event
	CondFrequency  = "frequency"  // times_seen crossed Threshold
)

// Rule is one alert rule. Condition and channel are simple and explicit —
// no DSL, no plugin marketplace (anti-goals).
type Rule struct {
	ID        int64  `json:"id"`
	OrgID     int64  `json:"org_id"`
	ProjectID int64  `json:"project_id"`
	Name      string `json:"name"`
	Condition string `json:"condition"` // one of the Cond* constants
	Threshold int64  `json:"threshold"` // frequency only
	// WebhookURL is the delivery channel (email/Slack/PagerDuty are shaped
	// the same way and land as additional channel types).
	WebhookURL string `json:"webhook_url"`
	Enabled    bool   `json:"enabled"`
}

// IssueActivity is the engine's input: one issue update from the pipeline.
type IssueActivity struct {
	OrgID     int64
	ProjectID int64
	IssueID   int64
	Title     string
	Level     string
	Status    string
	TimesSeen int64
	IsNew     bool
	Regressed bool
}

// RuleSource supplies enabled rules for a project (metadata.DB in prod).
type RuleSource interface {
	EnabledRules(ctx context.Context, projectID int64) ([]Rule, error)
}

// Options bound the engine.
type Options struct {
	Workers      int           // notification workers; default 4
	QueueDepth   int           // pending notifications; default 1024 (full = drop + count)
	Cooldown     time.Duration // per (rule,issue) dedup window; default 5m
	MaxAttempts  int           // delivery attempts before dead-letter; default 5
	BreakerFails int           // consecutive failures to open the breaker; default 5
	BreakerReset time.Duration // open duration; default 60s
	HTTPTimeout  time.Duration // per delivery attempt; default 10s
}

func (o *Options) defaults() {
	if o.Workers <= 0 {
		o.Workers = 4
	}
	if o.QueueDepth <= 0 {
		o.QueueDepth = 1024
	}
	if o.Cooldown <= 0 {
		o.Cooldown = 5 * time.Minute
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.BreakerFails <= 0 {
		o.BreakerFails = 5
	}
	if o.BreakerReset <= 0 {
		o.BreakerReset = 60 * time.Second
	}
	if o.HTTPTimeout <= 0 {
		o.HTTPTimeout = 10 * time.Second
	}
}

// notification is one queued delivery.
type notification struct {
	rule Rule
	act  IssueActivity
}

// DeadLetter records a delivery that exhausted its attempts.
type DeadLetter struct {
	Rule     Rule
	Activity IssueActivity
	Error    string
	At       time.Time
}

// Engine evaluates rules and delivers notifications.
type Engine struct {
	log   *slog.Logger
	rules RuleSource
	opts  Options
	queue chan notification
	done  chan struct{}

	client *http.Client

	mu       sync.Mutex
	cooldown map[string]time.Time // (ruleID,issueID) -> last fired
	breaker  map[string]*breakerState
	dead     []DeadLetter // bounded ring (also exposed for tests/UI)

	Delivered uint64
	Dropped   uint64              // queue-full drops (counted, never silent)
	sleep     func(time.Duration) // injected for tests
}

type breakerState struct {
	fails     int
	openUntil time.Time
}

// New creates the engine; call Run to start workers.
func New(log *slog.Logger, rules RuleSource, opts Options) *Engine {
	opts.defaults()
	return &Engine{
		log:      log,
		rules:    rules,
		opts:     opts,
		queue:    make(chan notification, opts.QueueDepth),
		done:     make(chan struct{}),
		client:   &http.Client{Timeout: opts.HTTPTimeout},
		cooldown: map[string]time.Time{},
		breaker:  map[string]*breakerState{},
		sleep:    time.Sleep,
	}
}

// Run starts the worker pool and blocks until ctx is done.
func (e *Engine) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < e.opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case n := <-e.queue:
					e.deliver(ctx, n)
				}
			}
		}()
	}
	wg.Wait()
	close(e.done)
}

// Evaluate matches an issue activity against the project's rules and enqueues
// notifications. Non-blocking: a full queue drops (counted) rather than
// stalling the ingest pipeline.
func (e *Engine) Evaluate(ctx context.Context, act IssueActivity) {
	rules, err := e.rules.EnabledRules(ctx, act.ProjectID)
	if err != nil {
		e.log.Error("alert rules load", "err", err)
		return
	}
	for _, r := range rules {
		if !e.matches(r, act) {
			continue
		}
		if !e.cooldownOK(r.ID, act.IssueID) {
			continue
		}
		select {
		case e.queue <- notification{rule: r, act: act}:
		default:
			e.mu.Lock()
			e.Dropped++
			e.mu.Unlock()
			e.log.Error("alert queue full, notification dropped",
				"rule", r.Name, "issue", act.IssueID)
		}
	}
}

// matches applies the rule condition.
func (e *Engine) matches(r Rule, act IssueActivity) bool {
	switch r.Condition {
	case CondNewIssue:
		return act.IsNew
	case CondRegression:
		return act.Regressed
	case CondFrequency:
		return r.Threshold > 0 && act.TimesSeen == r.Threshold // fire once at crossing
	default:
		return false
	}
}

// cooldownOK enforces the per-(rule,issue) dedup window.
func (e *Engine) cooldownOK(ruleID, issueID int64) bool {
	key := fmt.Sprintf("%d:%d", ruleID, issueID)
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	if last, ok := e.cooldown[key]; ok && now.Sub(last) < e.opts.Cooldown {
		return false
	}
	// Bound the map: opportunistic sweep when large.
	if len(e.cooldown) > 100_000 {
		for k, v := range e.cooldown {
			if now.Sub(v) > e.opts.Cooldown {
				delete(e.cooldown, k)
			}
		}
	}
	e.cooldown[key] = now
	return true
}

// deliver posts the webhook with retries + jitter, respecting the breaker.
func (e *Engine) deliver(ctx context.Context, n notification) {
	payload, _ := json.Marshal(map[string]any{
		"rule":       n.rule.Name,
		"condition":  n.rule.Condition,
		"project_id": n.act.ProjectID,
		"issue_id":   n.act.IssueID,
		"title":      n.act.Title,
		"level":      n.act.Level,
		"status":     n.act.Status,
		"times_seen": n.act.TimesSeen,
	})

	var lastErr error
	for attempt := 0; attempt < e.opts.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if !e.breakerAllow(n.rule.WebhookURL) {
			lastErr = errors.New("circuit breaker open")
			// Breaker open: wait out the reset window rather than burning attempts.
			e.sleep(e.opts.BreakerReset / 2)
			continue
		}
		err := e.post(ctx, n.rule.WebhookURL, payload)
		e.breakerRecord(n.rule.WebhookURL, err == nil)
		if err == nil {
			e.mu.Lock()
			e.Delivered++
			e.mu.Unlock()
			return
		}
		lastErr = err
		// Exponential backoff with FULL jitter: sleep U(0, base*2^attempt).
		base := 250 * time.Millisecond
		max := float64(base) * float64(uint(1)<<uint(attempt))
		e.sleep(time.Duration(rand.Float64() * max))
	}

	e.mu.Lock()
	e.dead = append(e.dead, DeadLetter{Rule: n.rule, Activity: n.act, Error: lastErr.Error(), At: time.Now()})
	if len(e.dead) > 1000 { // bounded dead-letter ring
		e.dead = e.dead[len(e.dead)-1000:]
	}
	e.mu.Unlock()
	e.log.Error("alert delivery dead-lettered", "rule", n.rule.Name, "err", lastErr)
}

// post performs one webhook attempt.
func (e *Engine) post(ctx context.Context, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "leser-alerts")
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", resp.StatusCode)
	}
	return nil
}

// breakerAllow reports whether the endpoint's circuit admits a request.
func (e *Engine) breakerAllow(url string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.breaker[url]
	if !ok {
		return true
	}
	return time.Now().After(b.openUntil)
}

// breakerRecord updates breaker state after an attempt.
func (e *Engine) breakerRecord(url string, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	b := e.breaker[url]
	if b == nil {
		b = &breakerState{}
		e.breaker[url] = b
	}
	if ok {
		b.fails = 0
		b.openUntil = time.Time{}
		return
	}
	b.fails++
	if b.fails >= e.opts.BreakerFails {
		b.openUntil = time.Now().Add(e.opts.BreakerReset)
		b.fails = 0
	}
}

// DeadLetters returns a copy of the dead-letter ring (visible, not silent).
func (e *Engine) DeadLetters() []DeadLetter {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]DeadLetter, len(e.dead))
	copy(out, e.dead)
	return out
}
