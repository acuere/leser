// Package ingest implements the Sentry envelope wire protocol (order.md §3)
// and the ingest pipeline: HTTP → WAL → consumer → event store. Every parse
// path is bounded; malformed input is rejected, never crashes (fuzzed in CI).
package ingest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Limits bound envelope parsing. Zero values take defaults (order.md §7:
// bound everything).
type Limits struct {
	MaxEnvelopeBytes int64 // whole envelope after decompression; default 20MB
	MaxItems         int   // items per envelope; default 50
	MaxItemBytes     int   // single item payload; default 10MB
}

func (l *Limits) defaults() {
	if l.MaxEnvelopeBytes <= 0 {
		l.MaxEnvelopeBytes = 20 << 20
	}
	if l.MaxItems <= 0 {
		l.MaxItems = 50
	}
	if l.MaxItemBytes <= 0 {
		l.MaxItemBytes = 10 << 20
	}
}

// Item types we accept (order.md §3).
const (
	ItemEvent        = "event"
	ItemTransaction  = "transaction"
	ItemAttachment   = "attachment"
	ItemSession      = "session"
	ItemClientReport = "client_report"
	ItemCheckIn      = "check_in"
)

var acceptedTypes = map[string]bool{
	ItemEvent: true, ItemTransaction: true, ItemAttachment: true,
	ItemSession: true, ItemClientReport: true, ItemCheckIn: true,
}

// ErrMalformed covers any structural envelope violation. The reason string is
// safe to return to SDKs.
type ErrMalformed struct{ Reason string }

func (e *ErrMalformed) Error() string { return "envelope: " + e.Reason }

func malformed(format string, args ...any) error {
	return &ErrMalformed{Reason: fmt.Sprintf(format, args...)}
}

// EnvelopeHeader is the first line of an envelope.
type EnvelopeHeader struct {
	EventID string `json:"event_id"`
	DSN     string `json:"dsn"`
	SentAt  string `json:"sent_at"`
}

// ItemHeader precedes each item payload.
type ItemHeader struct {
	Type   string `json:"type"`
	Length *int64 `json:"length"`
}

// Item is one parsed envelope item.
type Item struct {
	Header  ItemHeader
	Payload []byte
}

// Envelope is a fully parsed envelope.
type Envelope struct {
	Header EnvelopeHeader
	Items  []Item
}

// Parse reads a newline-delimited Sentry envelope from r, enforcing limits.
// r must already be decompressed; the caller caps the pre-decompression size.
func Parse(r io.Reader, lim Limits) (*Envelope, error) {
	lim.defaults()
	// Hard cap total bytes read even if the caller forgot to bound r
	// (zip-bomb guard: +1 lets us detect overflow).
	lr := &io.LimitedReader{R: r, N: lim.MaxEnvelopeBytes + 1}
	br := bufio.NewReaderSize(lr, 64<<10)

	headerLine, err := readLine(br, lim.MaxEnvelopeBytes)
	if err != nil {
		return nil, malformed("missing header line: %v", err)
	}
	var env Envelope
	if err := json.Unmarshal(headerLine, &env.Header); err != nil {
		return nil, malformed("header not valid JSON")
	}

	for {
		if lr.N <= 0 {
			return nil, malformed("envelope exceeds %d bytes", lim.MaxEnvelopeBytes)
		}
		line, err := readLine(br, lim.MaxEnvelopeBytes)
		if errors.Is(err, io.EOF) && len(bytes.TrimSpace(line)) == 0 {
			break // clean end after last item
		}
		if err != nil && len(bytes.TrimSpace(line)) == 0 {
			break
		}
		if len(bytes.TrimSpace(line)) == 0 {
			continue // tolerate a blank line between items
		}
		if len(env.Items) >= lim.MaxItems {
			return nil, malformed("more than %d items", lim.MaxItems)
		}
		var ih ItemHeader
		if err := json.Unmarshal(line, &ih); err != nil {
			return nil, malformed("item header not valid JSON")
		}
		if ih.Type == "" {
			return nil, malformed("item missing type")
		}

		var payload []byte
		if ih.Length != nil {
			n := *ih.Length
			if n < 0 || n > int64(lim.MaxItemBytes) {
				return nil, malformed("item length %d out of bounds", n)
			}
			payload = make([]byte, n)
			if _, err := io.ReadFull(br, payload); err != nil {
				return nil, malformed("item payload truncated")
			}
			// A newline after a length-prefixed payload is optional at EOF.
			if b, err := br.ReadByte(); err == nil && b != '\n' {
				return nil, malformed("missing newline after item payload")
			}
		} else {
			// No length: payload runs to the next newline.
			payload, err = readLine(br, int64(lim.MaxItemBytes))
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, malformed("unterminated item payload: %v", err)
			}
		}
		if !acceptedTypes[ih.Type] {
			continue // unknown item types are skipped, per protocol tolerance
		}
		env.Items = append(env.Items, Item{Header: ih, Payload: payload})
	}
	return &env, nil
}

// readLine reads until \n with a byte bound, returning the line without the
// terminator. Returns io.EOF (with any bytes read) at end of input.
func readLine(br *bufio.Reader, max int64) ([]byte, error) {
	var out []byte
	for {
		chunk, err := br.ReadSlice('\n')
		out = append(out, chunk...)
		if int64(len(out)) > max {
			return nil, malformed("line exceeds %d bytes", max)
		}
		switch {
		case err == nil:
			return out[:len(out)-1], nil // strip \n
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		default:
			return out, err
		}
	}
}
