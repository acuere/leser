//go:build !darwin

package main

import "os"

// fullFsync on non-Darwin: plain fsync already issues a real barrier on Linux
// (subject to the filesystem/drive cache), so F_FULLFSYNC has no separate meaning.
func fullFsync(f *os.File) error { return f.Sync() }
