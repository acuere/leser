//go:build darwin

package main

import (
	"os"

	"golang.org/x/sys/unix"
)

// fullFsync issues F_FULLFSYNC, the only call that flushes to physical media on
// macOS. Plain fsync() returns before the drive cache is flushed.
func fullFsync(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
