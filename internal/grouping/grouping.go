// Package grouping computes the issue fingerprint for an event (order.md §4).
// It is a pure, deterministic function: (event, rules) -> fingerprint. Any
// change here requires re-review of the golden corpus in testdata/.
package grouping

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
)

// Basis records which strategy in the fallback chain produced a fingerprint —
// stored with the issue so operators can see why events grouped.
type Basis string

const (
	BasisCustom          Basis = "custom"           // SDK-supplied fingerprint
	BasisRule            Basis = "rule"             // project fingerprint rule
	BasisStacktrace      Basis = "stacktrace"       // normalized frames
	BasisException       Basis = "exception"        // type + normalized value
	BasisMessageTemplate Basis = "message-template" // unformatted log template
	BasisMessage         Basis = "message"          // normalized raw message
	BasisFallback        Basis = "fallback"         // nothing usable
)

// Frame is one stack frame in grouping input, already decoded from the wire.
type Frame struct {
	Function string
	Module   string
	Filename string
	AbsPath  string
	InApp    *bool // SDK hint; enhancement rules may override
}

// Exception is one exception in the chain (innermost last, Sentry order).
type Exception struct {
	Type   string
	Value  string
	Frames []Frame
}

// Input is everything grouping may consider. Line numbers are deliberately
// absent: they churn on every deploy and must never affect grouping.
type Input struct {
	Platform        string
	SDKFingerprint  []string // event.fingerprint; "{{ default }}" splices ours
	Exceptions      []Exception
	MessageTemplate string // logentry.message (unformatted, stable)
	Message         string
}

// Result carries the fingerprint and how it was derived.
type Result struct {
	Fingerprint string
	Basis       Basis
}

// maxFrames bounds how many frames contribute (order.md §7: bound everything).
const maxFrames = 30

// Fingerprint computes the grouping fingerprint. Pure and deterministic.
func Fingerprint(in Input, rules []Rule) Result {
	// 1. Project fingerprint rules (matcher -> fingerprint) run first: they
	// exist to override everything else.
	if fp, ok := applyRules(in, rules); ok {
		return Result{Fingerprint: hashParts("rule", fp...), Basis: BasisRule}
	}

	// 2. SDK custom fingerprint, with "{{ default }}" splicing.
	if len(in.SDKFingerprint) > 0 && !onlyDefault(in.SDKFingerprint) {
		parts := make([]string, 0, len(in.SDKFingerprint)+1)
		for _, p := range in.SDKFingerprint {
			if isDefaultToken(p) {
				d := defaultFingerprint(in)
				parts = append(parts, d.Fingerprint)
			} else {
				parts = append(parts, p)
			}
		}
		return Result{Fingerprint: hashParts("custom", parts...), Basis: BasisCustom}
	}

	return defaultFingerprint(in)
}

// defaultFingerprint is the built-in fallback chain:
// stacktrace -> exception type+value -> message template -> message -> fallback.
func defaultFingerprint(in Input) Result {
	if frames := selectFrames(in); len(frames) > 0 {
		parts := make([]string, 0, len(frames)*2+1)
		// The exception type participates so different exceptions from the
		// same call site do not merge.
		if t := lastExceptionType(in); t != "" {
			parts = append(parts, normalizeSymbol(t))
		}
		for _, f := range frames {
			parts = append(parts, normalizeModule(f), normalizeSymbol(f.Function))
		}
		return Result{Fingerprint: hashParts("stack", parts...), Basis: BasisStacktrace}
	}

	if len(in.Exceptions) > 0 {
		exc := in.Exceptions[len(in.Exceptions)-1]
		if exc.Type != "" || exc.Value != "" {
			return Result{
				Fingerprint: hashParts("exc", normalizeSymbol(exc.Type), NormalizeMessage(exc.Value)),
				Basis:       BasisException,
			}
		}
	}

	if in.MessageTemplate != "" {
		return Result{Fingerprint: hashParts("tmpl", in.MessageTemplate), Basis: BasisMessageTemplate}
	}
	if in.Message != "" {
		return Result{Fingerprint: hashParts("msg", NormalizeMessage(in.Message)), Basis: BasisMessage}
	}
	return Result{Fingerprint: hashParts("fallback"), Basis: BasisFallback}
}

// selectFrames picks the frames that participate in grouping: in-app frames
// dominate; when none exist, all frames are used. Bounded to maxFrames of the
// innermost (most recent) frames.
func selectFrames(in Input) []Frame {
	if len(in.Exceptions) == 0 {
		return nil
	}
	// Group on the last (innermost/most recent) exception's stacktrace; chained
	// causes churn independently.
	frames := in.Exceptions[len(in.Exceptions)-1].Frames
	if len(frames) == 0 {
		return nil
	}
	var inApp []Frame
	for _, f := range frames {
		if IsInApp(f, in.Platform) {
			inApp = append(inApp, f)
		}
	}
	pick := frames
	if len(inApp) > 0 {
		pick = inApp
	}
	if len(pick) > maxFrames {
		pick = pick[len(pick)-maxFrames:]
	}
	return pick
}

func lastExceptionType(in Input) string {
	if len(in.Exceptions) == 0 {
		return ""
	}
	return in.Exceptions[len(in.Exceptions)-1].Type
}

func onlyDefault(fp []string) bool {
	for _, p := range fp {
		if !isDefaultToken(p) {
			return false
		}
	}
	return true
}

func isDefaultToken(s string) bool {
	t := strings.TrimSpace(s)
	return t == "{{ default }}" || t == "{{default}}"
}

// hashParts produces the stable fingerprint hash. The namespace prefix keeps
// different bases from colliding on identical part strings.
func hashParts(ns string, parts ...string) string {
	h := sha1.New()
	h.Write([]byte(ns))
	for _, p := range parts {
		h.Write([]byte{0})
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
