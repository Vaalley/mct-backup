// Package repository implements format v1: encrypted immutable packs, pack
// indexes, and complete snapshot manifests published last as commit records.
package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
	"mct-backup/internal/config"
	"mct-backup/internal/storage"
)

const PackSize = 128 << 20
const MaxChunkSize = 1 << 20

type compressor struct {
	buf    bytes.Buffer
	writer *gzip.Writer
}

var compressors = sync.Pool{New: func() any { c := &compressor{}; c.writer, _ = gzip.NewWriterLevel(&c.buf, gzip.BestSpeed); return c }}

type Ref struct {
	Pack   string `json:"pack"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Raw    int    `json:"raw"`
}
type packIndex struct {
	Version int            `json:"version"`
	Pack    string         `json:"pack"`
	Chunks  map[string]Ref `json:"chunks"`
}
type pendingUpload struct{ PackKey, IndexKey, PackFile, IndexFile string }

type Repo struct {
	Store          storage.Store
	Dir            string
	AEAD           cipher.AEAD
	DB             *bolt.DB
	Objects        map[string]storage.Object
	pack           *os.File
	packKey        string
	packOffset     int64
	pending        map[string]Ref
	TargetPackSize int64
	Uploaded       int64
	Log            io.Writer
	live           map[[32]byte]struct{}
	purgePlan      *PurgePlan
	uploader       *uploadPool
}

func NewKey() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	return base64.StdEncoding.EncodeToString(b), err
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func Open(ctx context.Context, store storage.Store, dir, key string, initialize bool, log io.Writer) (*Repo, error) {
	b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(key))
	if err != nil || len(b) != 32 {
		return nil, errors.New("recovery key must be a base64-encoded 32-byte key")
	}
	block, err := aes.NewCipher(b)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(dir, "spool"), 0700); err != nil {
		return nil, err
	}
	if log == nil {
		log = io.Discard
	}
	r := &Repo{Store: store, Dir: dir, AEAD: aead, Objects: map[string]storage.Object{}, TargetPackSize: PackSize, Log: &synchronizedWriter{out: log}}
	objects, err := store.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, o := range objects {
		r.Objects[o.Key] = o
	}
	if _, ok := r.Objects["repository"]; !ok {
		if !initialize {
			return nil, errors.New("repository metadata is missing; use login/init to initialize an empty repository")
		}
		if len(objects) != 0 {
			return nil, errors.New("refusing to initialize a nonempty repository without metadata")
		}
		meta, _ := json.Marshal(map[string]any{"version": 1, "id": Hash(b)[:32], "format": "mct-backup"})
		if err = r.putEncrypted(ctx, "repository", meta); err != nil {
			return nil, err
		}
	} else {
		meta, err := r.readEncrypted(ctx, "repository")
		if err != nil {
			return nil, fmt.Errorf("unlock repository: %w; check the recovery key", err)
		}
		var m struct {
			Version int    `json:"version"`
			Format  string `json:"format"`
		}
		if err = json.Unmarshal(meta, &m); err != nil {
			return nil, err
		}
		if m.Version != 1 || m.Format != "mct-backup" {
			return nil, errors.New("unsupported repository format")
		}
	}
	r.DB, err = bolt.Open(filepath.Join(dir, "index.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open rebuildable chunk cache: %w", err)
	}
	if err = r.syncIndex(ctx); err != nil {
		r.DB.Close()
		return nil, err
	}
	return r, nil
}

func (r *Repo) Close() error {
	if r.pack != nil {
		p := r.pack.Name()
		_ = r.pack.Close()
		_ = os.Remove(p)
	}
	if r.DB != nil {
		return r.DB.Close()
	}
	return nil
}

func (r *Repo) seal(aad string, plain []byte) ([]byte, error) {
	c := compressors.Get().(*compressor)
	defer compressors.Put(c)
	c.buf.Reset()
	c.writer.Reset(&c.buf)
	compressed := &c.buf
	w := c.writer
	if _, err := w.Write(plain); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	var payload []byte
	if compressed.Len()+1 < len(plain) {
		payload = append([]byte{1}, compressed.Bytes()...)
	} else {
		payload = append([]byte{0}, plain...)
	}
	nonce := make([]byte, r.AEAD.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return r.AEAD.Seal(nonce, nonce, payload, []byte(aad)), nil
}

func (r *Repo) unseal(aad string, b []byte, limit int64) ([]byte, error) {
	n := r.AEAD.NonceSize()
	if len(b) < n {
		return nil, errors.New("truncated encrypted object")
	}
	p, err := r.AEAD.Open(nil, b[:n], b[n:], []byte(aad))
	if err != nil {
		return nil, errors.New("encrypted object authentication failed")
	}
	if len(p) == 0 {
		return nil, errors.New("empty encrypted payload")
	}
	if p[0] == 0 {
		if int64(len(p)-1) > limit {
			return nil, errors.New("object exceeds size limit")
		}
		return p[1:], nil
	}
	if p[0] != 1 {
		return nil, errors.New("unknown compression format")
	}
	gz, err := gzip.NewReader(bytes.NewReader(p[1:]))
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	plain, err := io.ReadAll(io.LimitReader(gz, limit+1))
	if int64(len(plain)) > limit {
		return nil, errors.New("decompressed object exceeds size limit")
	}
	return plain, err
}

func (r *Repo) putEncrypted(ctx context.Context, key string, plain []byte) error {
	p := filepath.Join(r.Dir, "spool", Hash([]byte(key))+".metadata")
	b, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		b, err = r.seal(key, plain)
		if err != nil {
			return err
		}
		if err = config.WritePrivate(p, b); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		existing, e := r.unseal(key, b, 2<<30)
		if e != nil {
			return e
		}
		if !bytes.Equal(existing, plain) {
			return errors.New("pending metadata differs; resume the existing backup first")
		}
	}
	job := metadataUpload{Key: key, File: p}
	j, _ := json.Marshal(job)
	jobPath := p + ".meta-job"
	if err = config.WritePrivate(jobPath, j); err != nil {
		return err
	}
	return r.uploadMetadata(ctx, jobPath, job)
}

type metadataUpload struct{ Key, File string }

func (r *Repo) uploadMetadata(ctx context.Context, jobPath string, job metadataUpload) error {
	rel, err := filepath.Rel(filepath.Join(r.Dir, "spool"), job.File)
	if err != nil || !filepath.IsLocal(rel) {
		return errors.New("invalid metadata upload path")
	}
	info, err := os.Stat(job.File)
	if err != nil {
		return err
	}
	if err = r.Store.Put(ctx, job.Key, job.File); err != nil {
		return err
	}
	r.Objects[job.Key] = storage.Object{Key: job.Key, Size: info.Size(), Created: time.Now().UTC()}
	if err = os.Remove(jobPath); err != nil {
		return err
	}
	_ = os.Remove(job.File)
	return nil
}

// Cache authenticated, immutable metadata, never source files. Remote listing
// remains authoritative: deleted objects are never resurrected from this cache.
func (r *Repo) readEncrypted(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := r.Objects[key]; !ok {
		return nil, storage.ErrNotFound
	}
	cacheable := strings.HasPrefix(key, "indexes/") || strings.HasPrefix(key, "snapshots/")
	cache := filepath.Join(r.Dir, "metadata-cache", Hash([]byte(key))+".enc")
	if cacheable {
		if b, err := os.ReadFile(cache); err == nil {
			if plain, err := r.unseal(key, b, 2<<30); err == nil {
				return plain, nil
			}
		}
	}
	b, err := r.Store.Read(ctx, key, 0, -1)
	if err != nil {
		return nil, err
	}
	plain, err := r.unseal(key, b, 2<<30)
	if err != nil {
		return nil, err
	}
	if cacheable {
		if err = config.WritePrivate(cache, b); err != nil {
			return nil, fmt.Errorf("cache repository metadata: %w", err)
		}
	}
	return plain, nil
}

func (r *Repo) syncIndex(ctx context.Context) error {
	objects, err := r.Store.List(ctx)
	if err != nil {
		return err
	}
	r.Objects = map[string]storage.Object{}
	var indexes []string
	for _, o := range objects {
		r.Objects[o.Key] = o
		if strings.HasPrefix(o.Key, "indexes/") {
			indexes = append(indexes, o.Key)
		}
	}
	sort.Strings(indexes)
	if err = r.DB.Update(func(tx *bolt.Tx) error {
		loaded, e := tx.CreateBucketIfNotExists([]byte("loaded"))
		if e != nil {
			return e
		}
		reset := false
		if e = loaded.ForEach(func(k, v []byte) error {
			if _, ok := r.Objects[string(k)]; !ok {
				reset = true
			}
			return nil
		}); e != nil {
			return e
		}
		if reset {
			if e = tx.DeleteBucket([]byte("loaded")); e != nil {
				return e
			}
			if e = tx.DeleteBucket([]byte("chunks")); e != nil && e != bolt.ErrBucketNotFound {
				return e
			}
			if _, e = tx.CreateBucket([]byte("loaded")); e != nil {
				return e
			}
		}
		_, e = tx.CreateBucketIfNotExists([]byte("chunks"))
		return e
	}); err != nil {
		return err
	}
	for _, key := range indexes {
		if err = ctx.Err(); err != nil {
			return err
		}
		loaded := false
		if err = r.DB.View(func(tx *bolt.Tx) error { loaded = tx.Bucket([]byte("loaded")).Get([]byte(key)) != nil; return nil }); err != nil {
			return err
		}
		if loaded {
			continue
		}
		data, err := r.readEncrypted(ctx, key)
		if err != nil {
			return fmt.Errorf("read pack index %s: %w", key, err)
		}
		var idx packIndex
		if err = json.Unmarshal(data, &idx); err != nil {
			return err
		}
		if err = r.importIndex(key, idx); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) importIndex(key string, idx packIndex) error {
	if idx.Version != 1 {
		return errors.New("unsupported pack index version")
	}
	pack, ok := r.Objects[idx.Pack]
	if !ok {
		return fmt.Errorf("pack index %s refers to missing pack %s", key, idx.Pack)
	}
	return r.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("chunks"))
		for hash, ref := range idx.Chunks {
			if len(hash) != 64 || ref.Pack != idx.Pack || ref.Offset < 0 || ref.Length <= 0 || ref.Offset > pack.Size-ref.Length || ref.Raw < 0 || ref.Raw > MaxChunkSize {
				return fmt.Errorf("invalid chunk reference in %s", key)
			}
			b, err := json.Marshal(ref)
			if err != nil {
				return err
			}
			if err = bucket.Put([]byte(hash), b); err != nil {
				return err
			}
		}
		return tx.Bucket([]byte("loaded")).Put([]byte(key), []byte{1})
	})
}

func (r *Repo) lookup(hash string) (Ref, bool, error) {
	if ref, ok := r.pending[hash]; ok {
		return ref, true, nil
	}
	var ref Ref
	found := false
	err := r.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("chunks")).Get([]byte(hash))
		if b == nil {
			return nil
		}
		found = true
		return json.Unmarshal(b, &ref)
	})
	if found {
		if _, ok := r.Objects[ref.Pack]; !ok {
			return Ref{}, false, fmt.Errorf("chunk cache references missing pack %s; run rebuild-index", ref.Pack)
		}
	}
	return ref, found, err
}

func (r *Repo) AddChunk(ctx context.Context, b []byte) (string, error) {
	return r.addChunk(ctx, b, false)
}
func (r *Repo) addChunk(ctx context.Context, b []byte, force bool) (string, error) {
	if r.uploader != nil {
		if err := r.uploader.Err(); err != nil {
			return "", err
		}
	}
	if len(b) > MaxChunkSize {
		return "", errors.New("chunk exceeds format limit")
	}
	hash := Hash(b)
	_, ok, err := r.lookup(hash)
	if err != nil {
		return "", err
	}
	if ok && !force {
		return hash, nil
	}
	if _, ok := r.pending[hash]; ok {
		return hash, nil
	}
	if r.pack == nil {
		r.packKey = "packs/" + ID()
		r.pack, err = os.CreateTemp(filepath.Join(r.Dir, "spool"), "pack-*")
		if err != nil {
			return "", err
		}
		r.pending = map[string]Ref{}
		r.packOffset = 0
	}
	sealed, err := r.seal(hash, b)
	if err != nil {
		return "", err
	}
	if _, err = r.pack.Write(sealed); err != nil {
		return "", err
	}
	r.pending[hash] = Ref{r.packKey, r.packOffset, int64(len(sealed)), len(b)}
	r.packOffset += int64(len(sealed))
	if r.packOffset >= r.TargetPackSize {
		if err = r.Flush(ctx); err != nil {
			return "", err
		}
	}
	return hash, nil
}

func (r *Repo) Flush(ctx context.Context) error {
	if r.pack == nil {
		return nil
	}
	if err := r.pack.Sync(); err != nil {
		return err
	}
	if err := r.pack.Close(); err != nil {
		return err
	}
	idx := packIndex{1, r.packKey, r.pending}
	plain, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	indexKey := "indexes/" + strings.TrimPrefix(r.packKey, "packs/")
	b, err := r.seal(indexKey, plain)
	if err != nil {
		return err
	}
	indexFile := filepath.Join(r.Dir, "spool", ID()+".index")
	if err = config.WritePrivate(indexFile, b); err != nil {
		return err
	}
	job := pendingUpload{r.packKey, indexKey, r.pack.Name(), indexFile}
	jobFile := filepath.Join(r.Dir, "spool", ID()+".job")
	j, _ := json.Marshal(job)
	if err = config.WritePrivate(jobFile, j); err != nil {
		return err
	}
	// Once journaled, preserve the spool on failure so a later invocation resumes it.
	r.pack = nil
	r.pending = nil
	if r.uploader != nil {
		// Provisional references let the scanner deduplicate against queued packs.
		// Their loaded-index markers force a cache rebuild after a crash if those
		// indexes never reached Drive. No manifest publishes before the barrier.
		if err = r.recordJob(job); err != nil {
			return err
		}
		if err = r.uploader.submit(func(uploadCtx context.Context) error { return r.sendJob(uploadCtx, jobFile, job) }); err != nil {
			return err
		}
	} else if err = r.uploadJob(ctx, jobFile, job); err != nil {
		return err
	}
	return r.importIndex(indexKey, idx)
}

func (r *Repo) uploadJob(ctx context.Context, path string, job pendingUpload) error {
	if err := r.recordJob(job); err != nil {
		return err
	}
	return r.sendJob(ctx, path, job)
}

// Only the scanner goroutine mutates Objects, Uploaded and the chunk database.
func (r *Repo) recordJob(job pendingUpload) error {
	for _, item := range []struct{ key, file string }{{job.PackKey, job.PackFile}, {job.IndexKey, job.IndexFile}} {
		info, err := os.Stat(item.file)
		if err != nil {
			return err
		}
		r.Objects[item.key] = storage.Object{Key: item.key, Size: info.Size(), Created: time.Now().UTC()}
		r.Uploaded += info.Size()
	}
	return nil
}

func (r *Repo) sendJob(ctx context.Context, path string, job pendingUpload) error {
	for _, item := range []struct{ key, file string }{{job.PackKey, job.PackFile}, {job.IndexKey, job.IndexFile}} {
		// Journal paths must remain inside this configuration's spool directory.
		rel, err := filepath.Rel(filepath.Join(r.Dir, "spool"), item.file)
		if err != nil || !filepath.IsLocal(rel) {
			return errors.New("invalid upload journal path")
		}
		info, err := os.Stat(item.file)
		if err != nil {
			return fmt.Errorf("pending upload file missing: %w", err)
		}
		fmt.Fprintf(r.Log, "Uploading %s (%d bytes)\n", item.key, info.Size())
		if err = r.Store.Put(ctx, item.key, item.file); err != nil {
			return err
		}
	}
	// Removing the journal first leaves only harmless local orphans on a crash.
	if err := os.Remove(path); err != nil {
		return err
	}
	_ = os.Remove(job.PackFile)
	_ = os.Remove(job.IndexFile)
	return nil
}

func (r *Repo) resume(ctx context.Context) error {
	jobs, err := filepath.Glob(filepath.Join(r.Dir, "spool", "*.job"))
	if err != nil {
		return err
	}
	for _, p := range jobs {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var job pendingUpload
		if err = json.Unmarshal(b, &job); err != nil {
			return err
		}
		if err = r.uploadJob(ctx, p, job); err != nil {
			return err
		}
	}
	jobs, err = filepath.Glob(filepath.Join(r.Dir, "spool", "*.meta-job"))
	if err != nil {
		return err
	}
	for _, p := range jobs {
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var job metadataUpload
		if err = json.Unmarshal(b, &job); err != nil {
			return err
		}
		if err = r.uploadMetadata(ctx, p, job); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) Chunk(ctx context.Context, hash string) ([]byte, error) {
	ref, ok, err := r.lookup(hash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("missing chunk %s", hash)
	}
	b, err := r.Store.Read(ctx, ref.Pack, ref.Offset, ref.Length)
	if err != nil {
		return nil, err
	}
	plain, err := r.unseal(hash, b, MaxChunkSize)
	if err != nil {
		return nil, err
	}
	if len(plain) != ref.Raw || Hash(plain) != hash {
		return nil, fmt.Errorf("chunk checksum mismatch: %s", hash)
	}
	return plain, nil
}
