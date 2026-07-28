// Package search parses the issue-stream query language (order.md §9):
//
//	is:unresolved level:error release:1.2.3 environment:prod user:u-77 plain text
//
// key:value tokens filter structured fields; bare words match issue titles.
// Values may be quoted ("two words"). Unknown keys are an error — a typo that
// silently matched everything would be worse than a message.
package search

import (
	"fmt"
	"strings"
)

// Filter is the parsed query.
type Filter struct {
	Status      string // is:
	Level       string // level:
	Release     string // release: (event-backed)
	Environment string // environment: / env: (event-backed)
	UserID      string // user: / user.id: (event-backed)
	Fingerprint string // fingerprint:
	Text        string // bare words, joined — title substring match
}

// NeedsEventLookup reports whether the filter requires consulting the event
// store (fields that live on events, not on the issues table).
func (f Filter) NeedsEventLookup() bool {
	return f.Release != "" || f.Environment != "" || f.UserID != ""
}

var validStatus = map[string]bool{
	"unresolved": true, "resolved": true, "ignored": true, "regressed": true,
}

// Parse tokenizes and validates a query string.
func Parse(q string) (Filter, error) {
	var f Filter
	var text []string
	for _, tok := range tokenize(q) {
		key, val, isKV := strings.Cut(tok, ":")
		if !isKV {
			text = append(text, tok)
			continue
		}
		val = strings.Trim(val, `"`)
		switch strings.ToLower(key) {
		case "is":
			if !validStatus[val] {
				return f, fmt.Errorf("search: is:%s invalid (unresolved|resolved|ignored|regressed)", val)
			}
			f.Status = val
		case "level":
			f.Level = val
		case "release":
			f.Release = val
		case "environment", "env":
			f.Environment = val
		case "user", "user.id":
			f.UserID = val
		case "fingerprint":
			f.Fingerprint = val
		default:
			return f, fmt.Errorf("search: unknown key %q (is, level, release, environment, user, fingerprint)", key)
		}
	}
	f.Text = strings.Join(text, " ")
	return f, nil
}

// tokenize splits on spaces while respecting double quotes:
// `level:error release:"1.2 beta" boom` → [level:error release:"1.2 beta" boom]
func tokenize(q string) []string {
	var out []string
	var cur strings.Builder
	inQuote := false
	for _, r := range q {
		switch {
		case r == '"':
			inQuote = !inQuote
			cur.WriteRune(r)
		case r == ' ' && !inQuote:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}
