package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Full restores/verification cache up to eight encrypted packs on disk. A
// single-file restore uses byte ranges directly and needs no pack cache.
type chunkReader struct {
	r    *Repo
	ctx  context.Context
	dir  string
	age  map[string]int
	tick int
}

func newChunkReader(ctx context.Context, r *Repo, full bool) (*chunkReader, error) {
	c := &chunkReader{r: r, ctx: ctx, age: map[string]int{}}
	if full {
		var err error
		c.dir, err = os.MkdirTemp(r.Dir, "read-cache-")
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}
func (c *chunkReader) close() {
	if c.dir != "" {
		_ = os.RemoveAll(c.dir)
	}
}
func (c *chunkReader) read(hash string) ([]byte, error) {
	if c.dir == "" {
		return c.r.Chunk(c.ctx, hash)
	}
	ref, ok, err := c.r.lookup(hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("missing chunk %s", hash)
	}
	name := filepath.Join(c.dir, Hash([]byte(ref.Pack)))
	c.tick++
	if _, ok = c.age[ref.Pack]; !ok {
		if len(c.age) >= 8 {
			old := ""
			for p, age := range c.age {
				if old == "" || age < c.age[old] {
					old = p
				}
			}
			if err = os.Remove(filepath.Join(c.dir, Hash([]byte(old)))); err != nil {
				return nil, err
			}
			delete(c.age, old)
		}
		b, err := c.r.Store.Read(c.ctx, ref.Pack, 0, -1)
		if err != nil {
			return nil, err
		}
		if err = os.WriteFile(name, b, 0600); err != nil {
			return nil, err
		}
	}
	c.age[ref.Pack] = c.tick
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	b := make([]byte, ref.Length)
	_, err = f.ReadAt(b, ref.Offset)
	f.Close()
	if err != nil {
		return nil, err
	}
	p, err := c.r.unseal(hash, b, MaxChunkSize)
	if err != nil {
		return nil, err
	}
	if len(p) != ref.Raw || Hash(p) != hash {
		return nil, errors.New("chunk checksum mismatch")
	}
	return p, nil
}

func writeFile(c *chunkReader, f File, out io.Writer) error {
	h := sha256.New()
	writer := io.MultiWriter(out, h)
	var n int64
	for _, hash := range f.Chunks {
		if err := c.ctx.Err(); err != nil {
			return err
		}
		b, err := c.read(hash)
		if err != nil {
			return fmt.Errorf("%s: %w", f.Path, err)
		}
		k, err := writer.Write(b)
		if err != nil {
			return err
		}
		n += int64(k)
	}
	if n != f.Size || hex.EncodeToString(h.Sum(nil)) != f.Hash {
		return fmt.Errorf("file checksum/size mismatch: %s", f.Path)
	}
	return nil
}

func (r *Repo) Verify(ctx context.Context, s *Snapshot) error {
	c, err := newChunkReader(ctx, r, true)
	if err != nil {
		return err
	}
	defer c.close()
	for i, f := range s.Files {
		if f.Kind == "file" {
			if err = writeFile(c, f, io.Discard); err != nil {
				return err
			}
		}
		if i%100 == 0 {
			fmt.Fprintf(r.Log, "Verified %d/%d entries\n", i+1, len(s.Files))
		}
	}
	return nil
}

func (r *Repo) Restore(ctx context.Context, s *Snapshot, selected, dest string) error {
	selected = strings.TrimSuffix(filepath.ToSlash(selected), "/")
	if selected == "." {
		selected = ""
	}
	var files []File
	for _, f := range s.Files {
		if selected == "" || f.Path == selected || strings.HasPrefix(f.Path, selected+"/") {
			files = append(files, f)
		}
	}
	if len(files) == 0 && selected != "" {
		return fmt.Errorf("path %q was absent from snapshot %s", selected, s.ID)
	}
	// Validate all link targets before creating anything. Never allow a restored
	// link to redirect subsequent writes outside the destination.
	for _, f := range files {
		if f.Kind == "symlink" {
			resolved := path.Join(path.Dir(f.Path), f.Target)
			if path.IsAbs(f.Target) || !filepath.IsLocal(filepath.FromSlash(resolved)) {
				return fmt.Errorf("unsafe symlink %s -> %s; select individual regular files to restore", f.Path, f.Target)
			}
		}
	}
	if entries, err := os.ReadDir(dest); err == nil && len(entries) > 0 {
		return errors.New("restore destination must be empty; existing files are never overwritten")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(dest, 0700); err != nil {
		return err
	}
	root, err := os.OpenRoot(dest)
	if err != nil {
		return err
	}
	defer root.Close()
	c, err := newChunkReader(ctx, r, selected == "" || len(files) > 1)
	if err != nil {
		return err
	}
	defer c.close()
	for _, f := range files {
		if err = ctx.Err(); err != nil {
			return err
		}
		if f.Kind == "symlink" {
			continue
		}
		if f.Kind == "dir" {
			if err = root.MkdirAll(f.Path, 0700); err != nil {
				return err
			}
			continue
		}
		if err = root.MkdirAll(path.Dir(f.Path), 0700); err != nil {
			return err
		}
		tmp := path.Join(path.Dir(f.Path), ".mct-restore-"+ID())
		out, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		err = writeFile(c, f, out)
		if err == nil {
			err = out.Chmod(os.FileMode(f.Mode) & 0777)
		}
		if err == nil {
			err = out.Sync()
		}
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = root.Link(tmp, f.Path)
		}
		_ = root.Remove(tmp)
		if err != nil {
			return err
		}
		if err = root.Chtimes(f.Path, time.Now(), f.ModTime); err != nil {
			return err
		}
	}
	for _, f := range files {
		if f.Kind == "symlink" {
			if err = root.MkdirAll(path.Dir(f.Path), 0700); err != nil {
				return err
			}
			if err = root.Symlink(f.Target, f.Path); err != nil {
				return err
			}
		}
	}
	for i := len(files) - 1; i >= 0; i-- {
		f := files[i]
		if f.Kind == "dir" {
			if err = root.Chmod(f.Path, os.FileMode(f.Mode)&0777); err != nil {
				return err
			}
			if err = root.Chtimes(f.Path, time.Now(), f.ModTime); err != nil {
				return err
			}
		}
	}
	return nil
}
