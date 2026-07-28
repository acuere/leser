package grouping

import (
	"regexp"
	"strings"
)

// Normalization strips everything that churns between occurrences of the same
// logical error: addresses, ids, timestamps, deploy-specific paths, compiler-
// generated suffixes. Grouping input must be invariant across deploys.

var (
	reHexAddr = regexp.MustCompile(`0x[0-9a-fA-F]+`)
	reUUID    = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reLongHex = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
	reNumRun  = regexp.MustCompile(`\d+`)
	// Java lambdas: lambda$method$3 — the trailing counter churns per build.
	reJavaLambda = regexp.MustCompile(`lambda\$([\w.]+)\$\d+`)
	// generic trailing $N counters (inner classes, generated members)
	reDollarNum = regexp.MustCompile(`\$\d+$`)
	// compiler/runtime generated symbol suffixes: func1, lambda$3, $anon$2,
	// closure_7, <locals>, .constprop.0, .isra.4
	reGenSuffix = regexp.MustCompile(`(?:[.$]?(?:func|lambda|anon|closure|gen|block)[\d$]*|\.(?:constprop|isra|part)\.\d+)$`)
	// go type parameter instantiations: foo[...] — churn per instantiation set
	reGoInst = regexp.MustCompile(`\[\.\.\.\]|\[[^\]]{0,80}\]`)
)

// NormalizeMessage collapses variable content in free-text messages so that
// "timeout for user 12345 (0x7fca)" == "timeout for user 99 (0x1)".
func NormalizeMessage(s string) string {
	if len(s) > 512 {
		s = s[:512]
	}
	s = reUUID.ReplaceAllString(s, "<uuid>")
	s = reHexAddr.ReplaceAllString(s, "<addr>")
	s = reLongHex.ReplaceAllString(s, "<hex>")
	s = reNumRun.ReplaceAllString(s, "<num>")
	return strings.TrimSpace(s)
}

// normalizeSymbol cleans a function or type symbol.
func normalizeSymbol(s string) string {
	if s == "" {
		return "<unknown>"
	}
	if len(s) > 256 {
		s = s[:256]
	}
	s = reGoInst.ReplaceAllString(s, "[T]")
	s = reHexAddr.ReplaceAllString(s, "<addr>")
	s = reJavaLambda.ReplaceAllString(s, "lambda$$$1")
	s = reDollarNum.ReplaceAllString(s, "")
	s = reGenSuffix.ReplaceAllString(s, ".<gen>")
	return s
}

// normalizeModule derives a stable module identity for a frame: the reported
// module if present, else the filename stripped of deploy-specific prefixes,
// content hashes, and minification artifacts.
func normalizeModule(f Frame) string {
	if f.Module != "" {
		return normalizeSymbol(f.Module)
	}
	name := f.Filename
	if name == "" {
		name = f.AbsPath
	}
	if name == "" {
		return "<unknown>"
	}
	name = strings.ReplaceAll(name, "\\", "/")
	// Keep at most the last three path components: enough identity, no
	// machine-specific prefix (/home/deploy/build-1234/...).
	parts := strings.Split(name, "/")
	if len(parts) > 3 {
		parts = parts[len(parts)-3:]
	}
	name = strings.Join(parts, "/")
	// Webpack/vite content hashes: app.a1b2c3d4.js -> app.<hash>.js
	name = reBundleHash.ReplaceAllString(name, ".<hash>.")
	name = reUUID.ReplaceAllString(name, "<uuid>")
	name = reNumRun.ReplaceAllString(name, "<num>")
	return name
}

var reBundleHash = regexp.MustCompile(`\.[0-9a-fA-F]{6,20}\.`)

// library path fragments per ecosystem: a frame whose path contains any of
// these is not application code.
var libraryMarkers = []string{
	"node_modules/", "site-packages/", "dist-packages/", "/vendor/",
	"vendor/bundle", "/gems/", "/go/pkg/mod/", "/usr/lib/", "/usr/local/lib/",
	"<frozen ", "internal/modules/cjs", "webpack/bootstrap",
}

// library module prefixes (JVM/.NET/Go style dotted or slashed namespaces).
var libraryModulePrefixes = []string{
	"java.", "javax.", "jakarta.", "sun.", "com.sun.", "kotlin.", "scala.",
	"org.springframework.", "org.apache.", "org.hibernate.", "io.netty.",
	"System.", "Microsoft.", "runtime.", "reflect.", "net/http.", "database/sql.",
}

// IsInApp classifies a frame as application code. The SDK's explicit in_app
// hint wins when present; otherwise path and module heuristics decide.
// Enhancement rules (per-project) are applied by the caller before this via
// Rule application setting Frame.InApp.
func IsInApp(f Frame, platform string) bool {
	if f.InApp != nil {
		return *f.InApp
	}
	path := f.AbsPath
	if path == "" {
		path = f.Filename
	}
	lower := strings.ToLower(strings.ReplaceAll(path, "\\", "/"))
	for _, m := range libraryMarkers {
		if strings.Contains(lower, m) {
			return false
		}
	}
	for _, p := range libraryModulePrefixes {
		if strings.HasPrefix(f.Module, p) || strings.HasPrefix(f.Function, p) {
			return false
		}
	}
	// Go stdlib: module like "fmt", "strings" with GOROOT-ish path.
	if strings.Contains(lower, "/libexec/") || strings.Contains(lower, "/goroot/") {
		return false
	}
	return true
}
