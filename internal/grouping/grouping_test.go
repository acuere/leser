package grouping

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the golden corpus output")

// corpusCase mirrors testdata/corpus.json entries.
type corpusCase struct {
	Name     string `json:"name"`
	SameAs   string `json:"same_as"`
	DiffFrom string `json:"diff_from"`
	Input    struct {
		Platform        string   `json:"platform"`
		SDKFingerprint  []string `json:"sdk_fingerprint"`
		Message         string   `json:"message"`
		MessageTemplate string   `json:"message_template"`
		Exceptions      []struct {
			Type   string `json:"type"`
			Value  string `json:"value"`
			Frames []struct {
				Function string `json:"function"`
				Module   string `json:"module"`
				Filename string `json:"filename"`
				AbsPath  string `json:"abs_path"`
				InApp    *bool  `json:"in_app"`
			} `json:"frames"`
		} `json:"exceptions"`
	} `json:"input"`
}

func (c corpusCase) toInput() Input {
	in := Input{
		Platform:        c.Input.Platform,
		SDKFingerprint:  c.Input.SDKFingerprint,
		Message:         c.Input.Message,
		MessageTemplate: c.Input.MessageTemplate,
	}
	for _, e := range c.Input.Exceptions {
		exc := Exception{Type: e.Type, Value: e.Value}
		for _, f := range e.Frames {
			exc.Frames = append(exc.Frames, Frame{
				Function: f.Function, Module: f.Module,
				Filename: f.Filename, AbsPath: f.AbsPath, InApp: f.InApp,
			})
		}
		in.Exceptions = append(in.Exceptions, exc)
	}
	return in
}

func loadCorpus(t testing.TB) []corpusCase {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []corpusCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

// TestGoldenCorpus locks grouping behavior: any change to the computed
// fingerprints fails until testdata/corpus.golden is regenerated with -update
// and the diff is re-reviewed (order.md §4).
func TestGoldenCorpus(t *testing.T) {
	cases := loadCorpus(t)
	results := map[string]Result{}
	var lines []string
	for _, c := range cases {
		res := Fingerprint(c.toInput(), nil)
		results[c.Name] = res
		lines = append(lines, fmt.Sprintf("%s\t%s\t%s", c.Name, res.Basis, res.Fingerprint))
	}
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"

	goldenPath := filepath.Join("testdata", "corpus.golden")
	if *update {
		if err := os.WriteFile(goldenPath, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("golden file missing (run with -update once): %v", err)
	}
	if got != string(want) {
		t.Errorf("golden corpus drifted — re-review required.\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}

	// Invariance pairs: same_as must match, diff_from must not.
	for _, c := range cases {
		if c.SameAs != "" {
			if results[c.Name].Fingerprint != results[c.SameAs].Fingerprint {
				t.Errorf("%s must group with %s but did not", c.Name, c.SameAs)
			}
		}
		if c.DiffFrom != "" {
			if results[c.Name].Fingerprint == results[c.DiffFrom].Fingerprint {
				t.Errorf("%s must NOT group with %s but did", c.Name, c.DiffFrom)
			}
		}
	}
}

// TestGoldenReadable sanity-checks golden format (name, basis, 32-hex fp).
func TestGoldenReadable(t *testing.T) {
	f, err := os.Open(filepath.Join("testdata", "corpus.golden"))
	if err != nil {
		t.Skip("golden not generated yet")
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), "\t")
		if len(parts) != 3 || len(parts[2]) != 32 {
			t.Fatalf("malformed golden line: %q", sc.Text())
		}
	}
}

func TestDeterminism(t *testing.T) {
	cases := loadCorpus(t)
	for _, c := range cases {
		in := c.toInput()
		a := Fingerprint(in, nil)
		for i := 0; i < 50; i++ {
			if b := Fingerprint(in, nil); b != a {
				t.Fatalf("%s: nondeterministic (%v vs %v)", c.Name, a, b)
			}
		}
	}
}

func TestFingerprintRules(t *testing.T) {
	in := Input{
		Platform: "python",
		Exceptions: []Exception{{
			Type: "OperationalError", Value: "could not connect to server",
			Frames: []Frame{{Function: "connect", Module: "psycopg2"}},
		}},
	}
	rules := []Rule{{
		Matchers:    []Matcher{{Type: "type", Pattern: "OperationalError"}, {Type: "value", Pattern: "*connect*"}},
		Fingerprint: []string{"database-unavailable"},
	}}
	res := Fingerprint(in, rules)
	if res.Basis != BasisRule {
		t.Fatalf("basis %s, want rule", res.Basis)
	}
	// Different stack, same rule -> same group.
	in2 := in
	in2.Exceptions = []Exception{{Type: "OperationalError", Value: "server closed connection", Frames: []Frame{{Function: "reconnect", Module: "pool"}}}}
	if Fingerprint(in2, rules).Fingerprint != res.Fingerprint {
		t.Fatal("rule must unify groups")
	}
	// Non-matching event falls through to the default chain.
	in3 := Input{Message: "unrelated"}
	if Fingerprint(in3, rules).Basis == BasisRule {
		t.Fatal("rule must not match unrelated event")
	}
}

func TestEnhancementRules(t *testing.T) {
	f := false
	in := Input{
		Platform: "node",
		Exceptions: []Exception{{
			Type: "Error", Value: "x",
			Frames: []Frame{
				{Function: "handler", Filename: "src/generated/api.js"},
				{Function: "core", Filename: "src/app.js"},
			},
		}},
	}
	// Mark generated code as not-in-app; grouping should then ignore it.
	// (single * does not cross /; ** does — Sentry rule semantics)
	enh := []EnhancementRule{{Pattern: "**/generated/**", InApp: f}}
	withRules := ApplyEnhancements(in, enh)
	base := Fingerprint(in, nil)
	enhanced := Fingerprint(withRules, nil)
	if base.Fingerprint == enhanced.Fingerprint {
		t.Fatal("enhancement rule changed in-app set but fingerprint identical")
	}
	if got := withRules.Exceptions[0].Frames[0].InApp; got == nil || *got {
		t.Fatal("enhancement did not force in_app=false")
	}
	// Original input must be untouched (pure function).
	if in.Exceptions[0].Frames[0].InApp != nil {
		t.Fatal("ApplyEnhancements mutated its input")
	}
}

func TestNormalizeMessage(t *testing.T) {
	a := NormalizeMessage("retry 17 for 550e8400-e29b-41d4-a716-446655440000 at 0x7fff5c")
	b := NormalizeMessage("retry 99881 for 123e4567-e89b-12d3-a456-426614174000 at 0x1")
	if a != b {
		t.Fatalf("normalization unstable: %q vs %q", a, b)
	}
}

func BenchmarkFingerprint(b *testing.B) {
	cases := loadCorpus(b)
	inputs := make([]Input, len(cases))
	for i, c := range cases {
		inputs[i] = c.toInput()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Fingerprint(inputs[i%len(inputs)], nil)
	}
}
