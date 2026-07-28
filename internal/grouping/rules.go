package grouping

import (
	"path"
	"strings"
)

// Rule is a user-defined fingerprint rule (order.md §4): when every matcher
// matches the event, the rule's fingerprint parts replace the default chain.
// Rules are evaluated in order; the first match wins.
type Rule struct {
	Matchers    []Matcher `json:"matchers"`
	Fingerprint []string  `json:"fingerprint"`
}

// Matcher matches one event property with a glob pattern.
// Types: "type" (exception type), "value" (exception value), "message",
// "module" (any frame module), "path" (any frame path), "platform".
type Matcher struct {
	Type    string `json:"type"`
	Pattern string `json:"pattern"`
}

// EnhancementRule marks matching frame paths/modules as in-app or system,
// overriding heuristics (order.md §4 stack-trace enhancement rules).
type EnhancementRule struct {
	// Matcher glob applied to frame path (or module when MatchModule).
	Pattern     string `json:"pattern"`
	MatchModule bool   `json:"match_module"`
	InApp       bool   `json:"in_app"`
}

// ApplyEnhancements returns a copy of in with frame in_app flags forced where
// enhancement rules match. Runs before Fingerprint.
func ApplyEnhancements(in Input, rules []EnhancementRule) Input {
	if len(rules) == 0 {
		return in
	}
	out := in
	out.Exceptions = make([]Exception, len(in.Exceptions))
	for i, exc := range in.Exceptions {
		out.Exceptions[i] = exc
		out.Exceptions[i].Frames = make([]Frame, len(exc.Frames))
		copy(out.Exceptions[i].Frames, exc.Frames)
		for j := range out.Exceptions[i].Frames {
			f := &out.Exceptions[i].Frames[j]
			for _, r := range rules {
				subject := f.AbsPath
				if subject == "" {
					subject = f.Filename
				}
				if r.MatchModule {
					subject = f.Module
				}
				if globMatch(r.Pattern, subject) {
					v := r.InApp
					f.InApp = &v
					break
				}
			}
		}
	}
	return out
}

// applyRules evaluates fingerprint rules; first full match wins.
func applyRules(in Input, rules []Rule) ([]string, bool) {
	for _, r := range rules {
		if len(r.Fingerprint) == 0 || len(r.Matchers) == 0 {
			continue
		}
		if ruleMatches(in, r) {
			return r.Fingerprint, true
		}
	}
	return nil, false
}

func ruleMatches(in Input, r Rule) bool {
	for _, m := range r.Matchers {
		if !matcherMatches(in, m) {
			return false
		}
	}
	return true
}

func matcherMatches(in Input, m Matcher) bool {
	switch m.Type {
	case "platform":
		return globMatch(m.Pattern, in.Platform)
	case "message":
		return globMatch(m.Pattern, in.Message) || globMatch(m.Pattern, in.MessageTemplate)
	case "type":
		for _, e := range in.Exceptions {
			if globMatch(m.Pattern, e.Type) {
				return true
			}
		}
	case "value":
		for _, e := range in.Exceptions {
			if globMatch(m.Pattern, e.Value) {
				return true
			}
		}
	case "module":
		for _, e := range in.Exceptions {
			for _, f := range e.Frames {
				if globMatch(m.Pattern, f.Module) {
					return true
				}
			}
		}
	case "path":
		for _, e := range in.Exceptions {
			for _, f := range e.Frames {
				p := f.AbsPath
				if p == "" {
					p = f.Filename
				}
				if globMatch(m.Pattern, p) {
					return true
				}
			}
		}
	}
	return false
}

// globMatch: shell-style match with ** support via segment relaxation. A
// pattern containing "**" matches across path separators.
func globMatch(pattern, s string) bool {
	if pattern == "" {
		return false
	}
	if pattern == "*" || pattern == "**" {
		return s != ""
	}
	if strings.Contains(pattern, "**") {
		// Translate: match if the non-** pieces appear in order.
		parts := strings.Split(pattern, "**")
		rest := s
		for i, part := range parts {
			if part == "" {
				continue
			}
			idx := indexGlob(rest, part, i == 0)
			if idx < 0 {
				return false
			}
			rest = rest[idx:]
		}
		return true
	}
	ok, err := path.Match(pattern, s)
	return err == nil && ok
}

// indexGlob finds a literal-with-single-glob fragment inside s. Anchored means
// the fragment must match at the start.
func indexGlob(s, fragment string, anchored bool) int {
	if !strings.ContainsAny(fragment, "*?[") {
		if anchored {
			if strings.HasPrefix(s, fragment) {
				return len(fragment)
			}
			return -1
		}
		i := strings.Index(s, fragment)
		if i < 0 {
			return -1
		}
		return i + len(fragment)
	}
	// Fragment itself has simple globs: scan positions.
	for i := 0; i <= len(s); i++ {
		if anchored && i > 0 {
			break
		}
		for j := i; j <= len(s); j++ {
			if ok, _ := path.Match(fragment, s[i:j]); ok {
				return j
			}
		}
	}
	return -1
}
