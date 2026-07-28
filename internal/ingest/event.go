package ingest

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// sentryEvent is the subset of the Sentry event JSON we project into columns.
// The full payload is stored verbatim; these fields exist to filter without
// parsing payloads at query time.
type sentryEvent struct {
	EventID     string          `json:"event_id"`
	Timestamp   json.RawMessage `json:"timestamp"` // RFC3339 string or unix float
	Level       string          `json:"level"`
	Release     string          `json:"release"`
	Environment string          `json:"environment"`
	Message     string          `json:"message"`
	Logentry    struct {
		Formatted string `json:"formatted"`
		Message   string `json:"message"`
	} `json:"logentry"`
	User struct {
		ID    string `json:"id"`
		Email string `json:"email"`
	} `json:"user"`
	Exception json.RawMessage `json:"exception"`
}

type sentryException struct {
	Values []struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"values"`
}

// ExtractedEvent is the normalized projection used by the columnar store.
type ExtractedEvent struct {
	EventID     string
	Timestamp   int64 // unix nanos
	Level       string
	Fingerprint string
	Release     string
	Environment string
	UserID      string
	Message     string
}

// ExtractEvent parses a Sentry event JSON payload into its columnar
// projection. It never fails hard on missing fields — an event with only an
// event_id is still storable; timestamps default to now.
func ExtractEvent(payload []byte, now time.Time) (ExtractedEvent, error) {
	var se sentryEvent
	if err := json.Unmarshal(payload, &se); err != nil {
		return ExtractedEvent{}, malformed("event payload not valid JSON")
	}
	ev := ExtractedEvent{
		EventID:     strings.ToLower(strings.ReplaceAll(se.EventID, "-", "")),
		Timestamp:   parseSentryTime(se.Timestamp, now),
		Level:       se.Level,
		Release:     se.Release,
		Environment: se.Environment,
		UserID:      firstNonEmpty(se.User.ID, se.User.Email),
	}
	if ev.Level == "" {
		ev.Level = "error"
	}

	var excType, excValue string
	if len(se.Exception) > 0 {
		var exc sentryException
		if json.Unmarshal(se.Exception, &exc) == nil && len(exc.Values) > 0 {
			last := exc.Values[len(exc.Values)-1]
			excType, excValue = last.Type, last.Value
		}
	}
	ev.Message = firstNonEmpty(
		joinNonEmpty(excType, excValue),
		se.Logentry.Formatted,
		se.Logentry.Message,
		se.Message,
	)

	// Interim fingerprint: exception type+value, else message. The real
	// grouping engine (order.md §4) replaces this — stack-trace normalization,
	// in-app weighting, rule chains. Deterministic and honest until then.
	basis := firstNonEmpty(joinNonEmpty(excType, excValue), ev.Message, "<no-message>")
	sum := sha1.Sum([]byte(basis))
	ev.Fingerprint = hex.EncodeToString(sum[:8])
	return ev, nil
}

// parseSentryTime accepts RFC3339 strings or unix-seconds numbers (possibly
// fractional), defaulting to now.
func parseSentryTime(raw json.RawMessage, now time.Time) int64 {
	if len(raw) == 0 {
		return now.UnixNano()
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano()
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f * float64(time.Second))
		}
		return now.UnixNano()
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil && f > 0 {
		return int64(f * float64(time.Second))
	}
	return now.UnixNano()
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

func joinNonEmpty(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + ": " + b
	}
}
