// Package storage provides immutable repository objects. Only Delete (used by purge)
// is allowed to remove an object; Put must never replace different bytes.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrNotFound = errors.New("object not found")

type Object struct {
	Key     string    `json:"key"`
	Size    int64     `json:"size"`
	Created time.Time `json:"created"`
}

type Store interface {
	List(context.Context) ([]Object, error)
	Put(context.Context, string, string) error
	Read(context.Context, string, int64, int64) ([]byte, error)
	Delete(context.Context, string) error
}

func ValidKey(k string) bool {
	return k != "" && !strings.Contains(k, "..") && !strings.ContainsAny(k, "\\\x00") && !strings.HasPrefix(k, "/")
}

func FileHash(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), n, err
}

type Local struct{ Root string }

func (s *Local) path(k string) (string, error) {
	if !ValidKey(k) {
		return "", fmt.Errorf("invalid object key %q", k)
	}
	return filepath.Join(s.Root, filepath.FromSlash(k)), nil
}

func (s *Local) List(ctx context.Context) ([]Object, error) {
	var objects []Object
	err := filepath.WalkDir(s.Root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unexpected non-file in repository: %s", p)
		}
		r, err := filepath.Rel(s.Root, p)
		if err != nil {
			return err
		}
		if strings.HasPrefix(filepath.Base(p), ".tmp-") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		objects = append(objects, Object{filepath.ToSlash(r), info.Size(), info.ModTime()})
		return nil
	})
	return objects, err
}

func (s *Local) Put(ctx context.Context, key, source string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dest, err := s.path(key)
	if err != nil {
		return err
	}
	if _, err = os.Stat(dest); err == nil {
		a, _, e := FileHash(source)
		if e != nil {
			return e
		}
		b, _, e := FileHash(dest)
		if e != nil {
			return e
		}
		if a != b {
			return fmt.Errorf("immutable object conflict: %s", key)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(dest), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(dest), ".tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err = io.Copy(f, in); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	// Link publishes without replacing an existing object.
	if err = os.Link(f.Name(), dest); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(dest))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *Local) Read(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.path(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if offset < 0 || length < -1 {
		return nil, errors.New("invalid read range")
	}
	if _, err = f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	if length == -1 {
		return io.ReadAll(f)
	}
	b := make([]byte, length)
	_, err = io.ReadFull(f, b)
	return b, err
}

func (s *Local) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := s.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
