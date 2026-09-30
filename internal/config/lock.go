package config

import (
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

// A kernel lock is released on process exit, including SIGKILL. It never writes
// or deletes a lock object on Drive. One host/config directory owns a repository.
func Lock(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "process.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another mct-backup command is using %s", dir)
	}
	return func() { _ = unix.Flock(int(f.Fd()), unix.LOCK_UN); _ = f.Close() }, nil
}
