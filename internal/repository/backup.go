package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/restic/chunker"
	"golang.org/x/sys/unix"
)

type File struct {
	Path      string    `json:"path"`
	Kind      string    `json:"kind"`
	Size      int64     `json:"size"`
	Mode      uint32    `json:"mode"`
	ModTime   time.Time `json:"mtime"`
	Signature string    `json:"signature,omitempty"`
	Hash      string    `json:"sha256,omitempty"`
	Chunks    []string  `json:"chunks,omitempty"`
	Target    string    `json:"target,omitempty"`
	Unstable  bool      `json:"unstable,omitempty"`
}

type Snapshot struct {
	Version          int       `json:"version"`
	ID               string    `json:"id"`
	Source           string    `json:"source"`
	Started          time.Time `json:"started"`
	Completed        time.Time `json:"completed"`
	CoverageComplete bool      `json:"coverage_complete"`
	Files            []File    `json:"files"`
	Warnings         []string  `json:"warnings,omitempty"`
	ReadBytes        int64     `json:"read_bytes"`
	UploadedBytes    int64     `json:"uploaded_bytes"`
	ReusedFiles      int       `json:"reused_files"`
	Rehashed         bool      `json:"rehashed"`
}

type BackupOptions struct {
	UploadConcurrency int
	Source            string
	Excludes          []string
	Rehash            bool
	Now               func() time.Time
}

func signature(info os.FileInfo) string {
	extra := ""
	v := reflect.ValueOf(info.Sys())
	if v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
		if v.Kind() == reflect.Struct {
			for _, name := range []string{"Dev", "Ino", "Ctim", "Ctimespec"} {
				f := v.FieldByName(name)
				if f.IsValid() {
					extra += fmt.Sprint(f.Interface()) + ":"
				}
			}
		}
	}
	return fmt.Sprintf("%d:%d:%d:%s", info.Size(), info.ModTime().UnixNano(), info.Mode(), extra)
}

func excluded(name string, patterns []string) bool {
	for _, p := range patterns {
		p = strings.TrimSuffix(filepath.ToSlash(p), "/")
		if name == p || strings.HasPrefix(name, p+"/") {
			return true
		}
		if ok, _ := path.Match(p, name); ok {
			return true
		}
		if !strings.Contains(p, "/") {
			if ok, _ := path.Match(p, path.Base(name)); ok {
				return true
			}
		}
	}
	return false
}

func (r *Repo) Snapshots() []string {
	var ids []string
	for key := range r.Objects {
		if strings.HasPrefix(key, "snapshots/") {
			ids = append(ids, strings.TrimPrefix(key, "snapshots/"))
		}
	}
	sort.Strings(ids)
	return ids
}

func (r *Repo) LoadSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	if id == "latest" || id == "" {
		ids := r.Snapshots()
		if len(ids) == 0 {
			return nil, errors.New("no committed snapshots")
		}
		id = ids[len(ids)-1]
	}
	if strings.ContainsAny(id, "/\\") || strings.Contains(id, "..") {
		return nil, errors.New("invalid snapshot ID")
	}
	b, err := r.readEncrypted(ctx, "snapshots/"+id)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err = json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if s.Version != 1 || s.ID != id || s.Completed.Before(s.Started) {
		return nil, errors.New("invalid snapshot metadata")
	}
	seen := make(map[string]bool, len(s.Files))
	for _, f := range s.Files {
		if f.Path == "." || !fs.ValidPath(f.Path) || strings.Contains(f.Path, "\\") || seen[f.Path] || f.Size < 0 {
			return nil, fmt.Errorf("invalid or duplicate snapshot path %q", f.Path)
		}
		seen[f.Path] = true
		if f.Kind != "file" && f.Kind != "dir" && f.Kind != "symlink" {
			return nil, fmt.Errorf("unknown file kind %q", f.Kind)
		}
	}
	return &s, nil
}

func (r *Repo) SnapshotAt(ctx context.Context, at time.Time) (*Snapshot, error) {
	ids := r.Snapshots()
	for i := len(ids) - 1; i >= 0; i-- {
		s, err := r.LoadSnapshot(ctx, ids[i])
		if err != nil {
			return nil, err
		}
		if !s.Completed.After(at) {
			return s, nil
		}
	}
	return nil, fmt.Errorf("no retained snapshot completed at or before %s", at.Format(time.RFC3339))
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c contextReader) Read(b []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(b)
}

// fileReadError is the only class of error downgraded to missing coverage.
// Repository, upload and encryption failures always abort the backup.
type fileReadError struct{ err error }

func (e *fileReadError) Error() string { return e.err.Error() }
func (e *fileReadError) Unwrap() error { return e.err }

func (r *Repo) capture(ctx context.Context, root *os.Root, name string) (File, int64, error) {
	var result File
	var totalRead int64
	for attempt := 0; attempt < 2; attempt++ {
		f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			return File{}, totalRead, &fileReadError{err}
		}
		before, err := f.Stat()
		if err != nil {
			f.Close()
			return File{}, totalRead, &fileReadError{err}
		}
		if !before.Mode().IsRegular() {
			f.Close()
			return File{}, totalRead, &fileReadError{errors.New("file changed type during capture")}
		}
		result = File{Path: name, Kind: "file", Mode: uint32(before.Mode().Perm()), ModTime: before.ModTime().UTC(), Signature: signature(before)}
		h := sha256.New()
		reader := io.TeeReader(contextReader{ctx, io.LimitReader(f, before.Size())}, h)
		var c *chunker.Chunker
		if strings.HasSuffix(name, ".mca") {
			c = chunker.New(reader, chunker.Pol(0x3DA3358B4DC173), chunker.WithBoundaries(32<<10, 256<<10), chunker.WithAverageBits(16))
		} else {
			c = chunker.New(reader, chunker.Pol(0x3DA3358B4DC173), chunker.WithBoundaries(64<<10, MaxChunkSize), chunker.WithAverageBits(18))
		}
		buf := make([]byte, 0, MaxChunkSize)
		for {
			chunk, e := c.Next(buf)
			if e == io.EOF {
				break
			}
			if e != nil {
				f.Close()
				if ctx.Err() != nil {
					return File{}, totalRead, ctx.Err()
				}
				return File{}, totalRead, &fileReadError{e}
			}
			hash, e := r.AddChunk(ctx, chunk.Data)
			if e != nil {
				f.Close()
				return File{}, totalRead, e
			}
			result.Chunks = append(result.Chunks, hash)
			result.Size += int64(len(chunk.Data))
			totalRead += int64(len(chunk.Data))
			buf = chunk.Data[:0]
		}
		after, statErr := f.Stat()
		_ = f.Close()
		current, pathErr := root.Lstat(name)
		result.Hash = hex.EncodeToString(h.Sum(nil))
		stable := statErr == nil && pathErr == nil && current.Mode().IsRegular() && signature(before) == signature(after) && signature(before) == signature(current) && result.Size == before.Size()
		if stable {
			return result, totalRead, nil
		}
		result.Unstable = true
		result.Signature = ""
	}
	return result, totalRead, nil
}

func (r *Repo) Backup(ctx context.Context, opt BackupOptions) (*Snapshot, error) {
	if opt.UploadConcurrency == 0 {
		opt.UploadConcurrency = 4
	}
	if opt.UploadConcurrency < 1 || opt.UploadConcurrency > 16 {
		return nil, errors.New("upload concurrency must be between 1 and 16")
	}
	if err := r.resume(ctx); err != nil {
		return nil, err
	}
	if err := r.syncIndex(ctx); err != nil {
		return nil, err
	}
	uploader := newUploadPool(ctx, opt.UploadConcurrency)
	r.uploader = uploader
	defer func() { uploader.close(); r.uploader = nil }()
	if opt.Now == nil {
		opt.Now = time.Now
	}
	source, err := filepath.Abs(opt.Source)
	if err != nil {
		return nil, err
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return nil, err
	}
	// Never let local cache/spool or a local repository back itself up.
	for _, p := range []string{r.Dir} {
		abs, e := filepath.Abs(p)
		if e != nil {
			return nil, e
		}
		rel, e := filepath.Rel(source, abs)
		if e == nil && (rel == "." || filepath.IsLocal(rel)) {
			return nil, errors.New("configuration/spool directory must be outside the backup source")
		}
	}
	for _, p := range opt.Excludes {
		if _, err = path.Match(p, ""); err != nil {
			return nil, fmt.Errorf("invalid exclude pattern %q: %w", p, err)
		}
	}
	root, err := os.OpenRoot(source)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	s := &Snapshot{Version: 1, Source: source, Started: opt.Now().UTC(), CoverageComplete: true, Rehashed: opt.Rehash}
	previous := map[string]File{}
	ids := r.Snapshots()
	if len(ids) > 0 {
		prev, err := r.LoadSnapshot(ctx, ids[len(ids)-1])
		if err != nil {
			return nil, err
		}
		if prev.Source != source {
			return nil, fmt.Errorf("repository belongs to %s, not %s; use a separate configuration/repository", prev.Source, source)
		}
		for _, f := range prev.Files {
			previous[f.Path] = f
		}
		// Metadata is the fast path, not an integrity proof. Force a full read at least weekly.
		lastFull := time.Time{}
		for i := len(ids) - 1; i >= 0; i-- {
			p := prev
			if i != len(ids)-1 {
				p, err = r.LoadSnapshot(ctx, ids[i])
				if err != nil {
					return nil, err
				}
			}
			if p.Rehashed && p.CoverageComplete {
				lastFull = p.Completed
				break
			}
		}
		if s.Started.Sub(lastFull) >= 7*24*time.Hour {
			s.Rehashed = true
		}
	} else {
		s.Rehashed = true
	}
	uploadStart := r.Uploaded
	lastProgress := time.Now()
	err = fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." && walkErr != nil {
			return walkErr
		}
		if walkErr != nil {
			s.CoverageComplete = false
			s.Warnings = append(s.Warnings, fmt.Sprintf("%s: unreadable: %v", name, walkErr))
			return nil
		}
		if name == "." {
			return nil
		}
		if excluded(name, opt.Excludes) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, e := d.Info()
		if e != nil {
			s.CoverageComplete = false
			s.Warnings = append(s.Warnings, fmt.Sprintf("%s: metadata unavailable: %v", name, e))
			return nil
		}
		if time.Since(lastProgress) > 10*time.Second {
			fmt.Fprintf(r.Log, "Captured %d entries; read %d bytes; reused %d files\n", len(s.Files), s.ReadBytes, s.ReusedFiles)
			lastProgress = time.Now()
		}
		entry := File{Path: name, Mode: uint32(info.Mode().Perm()), ModTime: info.ModTime().UTC()}
		switch {
		case info.IsDir():
			entry.Kind = "dir"
		case info.Mode()&os.ModeSymlink != 0:
			entry.Kind = "symlink"
			entry.Target, e = root.Readlink(name)
			if e != nil {
				s.CoverageComplete = false
				s.Warnings = append(s.Warnings, name+": symlink unreadable")
				return nil
			}
		case info.Mode().IsRegular():
			if prev, ok := previous[name]; ok && !s.Rehashed && !prev.Unstable && prev.Kind == "file" && prev.Signature == signature(info) {
				entry = prev
				s.ReusedFiles++
			} else {
				var n int64
				entry, n, e = r.capture(ctx, root, name)
				s.ReadBytes += n
				if e != nil {
					var readErr *fileReadError
					if !errors.As(e, &readErr) {
						return e
					}
					s.CoverageComplete = false
					s.Warnings = append(s.Warnings, fmt.Sprintf("%s: not captured: %v", name, e))
					return nil
				}
				if entry.Unstable {
					s.Warnings = append(s.Warnings, name+": changed during both reads; captured bytes may be inconsistent")
				}
			}
		default:
			s.CoverageComplete = false
			s.Warnings = append(s.Warnings, name+": skipped special file")
			return nil
		}
		s.Files = append(s.Files, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = r.Flush(ctx); err != nil {
		return nil, err
	}
	fmt.Fprintln(r.Log, "Waiting for all pack uploads before committing snapshot...")
	if err = uploader.wait(); err != nil {
		return nil, err
	}
	// Refresh from remote metadata, rather than trusting provisional references.
	if err = r.syncIndex(ctx); err != nil {
		return nil, err
	}
	s.UploadedBytes = r.Uploaded - uploadStart
	s.Completed = opt.Now().UTC()
	s.ID = s.Completed.Format("20060102T150405.000000000Z") + "-" + ID()[:8]
	// Check every reference before publishing; no previous manifest is required to restore.
	for _, f := range s.Files {
		for _, hash := range f.Chunks {
			_, ok, e := r.lookup(hash)
			if e != nil {
				return nil, e
			}
			if !ok {
				return nil, fmt.Errorf("cannot commit: missing chunk %s", hash)
			}
		}
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if err = r.putEncrypted(ctx, "snapshots/"+s.ID, data); err != nil {
		return nil, err
	}
	return s, nil
}
