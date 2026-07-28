// Package buildinfo exposes version metadata stamped at build time via -ldflags.
package buildinfo

import "runtime/debug"

// These are overridden at build time with:
//
//	-ldflags "-X leser/internal/buildinfo.Version=... -X leser/internal/buildinfo.Commit=..."
var (
	// Version is the semantic version of the build (e.g. v0.1.0).
	Version = "dev"
	// Commit is the git commit the binary was built from.
	Commit = "unknown"
	// Date is the build date (RFC3339). Empty for reproducible builds unless stamped.
	Date = ""
)

// Info is a snapshot of build metadata.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date,omitempty"`
	GoVersion string `json:"go_version"`
}

// Get returns the current build metadata, falling back to VCS info embedded by
// the Go toolchain when explicit ldflags were not provided.
func Get() Info {
	i := Info{Version: Version, Commit: Commit, Date: Date}
	if bi, ok := debug.ReadBuildInfo(); ok {
		i.GoVersion = bi.GoVersion
		if Commit == "unknown" {
			for _, s := range bi.Settings {
				if s.Key == "vcs.revision" {
					i.Commit = s.Value
				}
			}
		}
	}
	return i
}
