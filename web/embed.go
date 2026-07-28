// Package web embeds the compiled UI assets into the binary.
package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var distFS embed.FS

// Assets returns the embedded UI filesystem rooted at the dist directory, so
// index.html is served at "/". Returns nil if assets are missing.
func Assets() fs.FS {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil
	}
	return sub
}
