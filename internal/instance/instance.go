// Package instance identifies one cell-agent data directory and prevents two
// processes from serving the same local state concurrently.
package instance

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Handle is an exclusive data-directory lock plus its persistent instance id.
type Handle struct {
	file *os.File
	ID   string
}

// Acquire takes <dir>/.lock with LOCK_EX|LOCK_NB and loads/creates
// <dir>/instance-id. The instance id is written and fsynced before it is
// returned, so a restarted process can prove which lease belonged to this
// directory.
func Acquire(dir string) (*Handle, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("instance: data directory is already in use: %w", err)
	}
	id, err := loadOrCreate(filepath.Join(dir, "instance-id"))
	if err != nil {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return nil, err
	}
	return &Handle{file: f, ID: id}, nil
}

// Close releases the data-directory lock.
func (h *Handle) Close() error {
	if h == nil || h.file == nil {
		return nil
	}
	_ = syscall.Flock(int(h.file.Fd()), syscall.LOCK_UN)
	return h.file.Close()
}

func loadOrCreate(path string) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if id := string(b); len(id) >= 16 {
			return id, nil
		}
		return "", fmt.Errorf("instance: corrupt instance-id")
	} else if !os.IsNotExist(err) {
		return "", err
	}
	var raw [16]byte
	if _, err := io.ReadFull(rand.Reader, raw[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(raw[:])
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return loadOrCreate(path)
		}
		return "", err
	}
	if _, err := f.WriteString(id); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	return id, nil
}
