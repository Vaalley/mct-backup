package repository

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mct-backup/internal/storage"
)

func fixture(t *testing.T) (*Repo, string, string, string) {
	t.Helper()
	base := t.TempDir()
	source := filepath.Join(base, "server")
	remote := filepath.Join(base, "remote")
	dir := filepath.Join(base, "config")
	for _, p := range []string{source, remote} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	key, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Open(context.Background(), &storage.Local{Root: remote}, dir, key, true, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, source, remote, key
}
func put(t *testing.T, p string, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
}
func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.New(rand.NewSource(int64(n))).Read(b)
	return b
}
func backup(t *testing.T, r *Repo, source string) *Snapshot {
	t.Helper()
	s, e := r.Backup(context.Background(), BackupOptions{Source: source})
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestIncrementalPointInTimeAndRebuild(t *testing.T) {
	r, source, remote, key := fixture(t)
	ctx := context.Background()
	region := randomBytes(8 << 20)
	put(t, filepath.Join(source, "world/region/r.0.0.mca"), region)
	put(t, filepath.Join(source, "world/playerdata/player.dat"), []byte("old player"))
	put(t, filepath.Join(source, "empty"), nil)
	one := backup(t, r, source)
	objectsBefore, _ := r.Store.List(ctx)
	hashes := map[string]string{}
	for _, o := range objectsBefore {
		b, e := r.Store.Read(ctx, o.Key, 0, -1)
		if e != nil {
			t.Fatal(e)
		}
		hashes[o.Key] = Hash(b)
	}
	two := backup(t, r, source)
	if two.ReadBytes != 0 || two.UploadedBytes != 0 || two.ReusedFiles != 3 {
		t.Fatalf("unchanged backup: %+v", two)
	}
	region[2<<20] ^= 0xff
	put(t, filepath.Join(source, "world/region/r.0.0.mca"), region)
	put(t, filepath.Join(source, "world/playerdata/player.dat"), []byte("new player"))
	three := backup(t, r, source)
	if three.UploadedBytes >= int64(len(region))/2 {
		t.Fatalf("region edit uploaded too much: %d", three.UploadedBytes)
	}
	for key, want := range hashes {
		b, e := r.Store.Read(ctx, key, 0, -1)
		if e != nil || Hash(b) != want {
			t.Fatalf("normal backup mutated %s: %v", key, e)
		}
	}
	at, e := r.SnapshotAt(ctx, two.Completed)
	if e != nil || at.ID != two.ID {
		t.Fatalf("point in time: %v %v", at, e)
	}
	if e = r.Restore(ctx, one, "world/playerdata/player.dat", filepath.Join(t.TempDir(), "restore")); e != nil {
		t.Fatal(e)
	}
	dest := filepath.Join(t.TempDir(), "restore")
	if e = r.Restore(ctx, three, "", dest); e != nil {
		t.Fatal(e)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "world/region/r.0.0.mca"))
	if !bytes.Equal(got, region) {
		t.Fatal("restored region differs")
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(r.Dir, "index.db")); e != nil {
		t.Fatal(e)
	}
	rebuilt, e := Open(ctx, &storage.Local{Root: remote}, r.Dir, key, false, io.Discard)
	if e != nil {
		t.Fatal(e)
	}
	defer rebuilt.Close()
	if e = rebuilt.Verify(ctx, one); e != nil {
		t.Fatal(e)
	}
	if e = rebuilt.Verify(ctx, three); e != nil {
		t.Fatal(e)
	}
}

type failingStore struct {
	mu sync.Mutex
	storage.Store
	failPrefix string
	failed     bool
	mutate     func(string)
}

func (s *failingStore) Put(ctx context.Context, key, file string) error {
	s.mu.Lock()
	if s.mutate != nil {
		s.mutate(key)
	}
	if !s.failed && strings.HasPrefix(key, s.failPrefix) {
		s.failed = true
		s.mu.Unlock()
		return errors.New("simulated network interruption")
	}
	s.mu.Unlock()
	return s.Store.Put(ctx, key, file)
}

func TestInterruptedPackAndManifestResume(t *testing.T) {
	for _, prefix := range []string{"indexes/", "snapshots/"} {
		t.Run(prefix, func(t *testing.T) {
			r, source, remote, key := fixture(t)
			ctx := context.Background()
			put(t, filepath.Join(source, "world/region/r.mca"), randomBytes(2<<20))
			failing := &failingStore{Store: r.Store, failPrefix: prefix}
			r.Store = failing
			if _, e := r.Backup(ctx, BackupOptions{Source: source}); e == nil {
				t.Fatal("expected interruption")
			}
			if len(r.Snapshots()) != 0 {
				t.Fatal("incomplete backup became visible")
			}
			if e := r.Close(); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(ctx, &storage.Local{Root: remote}, r.Dir, key, false, io.Discard)
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			s := backup(t, reopened, source)
			if e = reopened.Verify(ctx, s); e != nil {
				t.Fatal(e)
			}
			jobs, _ := filepath.Glob(filepath.Join(r.Dir, "spool", "*.job"))
			metas, _ := filepath.Glob(filepath.Join(r.Dir, "spool", "*.meta-job"))
			if len(jobs)+len(metas) != 0 {
				t.Fatal("upload jobs not consumed")
			}
		})
	}
}

func TestLiveFileMutationIsRecorded(t *testing.T) {
	r, source, _, _ := fixture(t)
	p := filepath.Join(source, "world/region/r.mca")
	b := randomBytes(2 << 20)
	put(t, p, b)
	r.TargetPackSize = 1
	f := &failingStore{Store: r.Store, failed: true}
	n := 0
	f.mutate = func(key string) {
		if strings.HasPrefix(key, "packs/") {
			n++
			file, e := os.OpenFile(p, os.O_WRONLY, 0)
			if e != nil {
				t.Fatal(e)
			}
			_, e = file.WriteAt([]byte{byte(n)}, int64(len(b)-1))
			file.Close()
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	r.Store = f
	s := backup(t, r, source)
	unstable := false
	for _, entry := range s.Files {
		if entry.Kind == "file" {
			unstable = entry.Unstable
		}
	}
	if !unstable || len(s.Warnings) == 0 {
		t.Fatal("concurrent changes not flagged")
	}
	if e := r.Verify(context.Background(), s); e != nil {
		t.Fatal(e)
	}
}

func TestCorruptionWrongKeyAndRestoreSafety(t *testing.T) {
	r, source, remote, _ := fixture(t)
	put(t, filepath.Join(source, "player.dat"), []byte("important bytes"))
	s := backup(t, r, source)
	dest := t.TempDir()
	put(t, filepath.Join(dest, "existing"), []byte("keep"))
	if e := r.Restore(context.Background(), s, "", dest); e == nil {
		t.Fatal("overwrote nonempty destination")
	}
	unsafe := &Snapshot{Files: []File{{Path: "link", Kind: "symlink", Target: "../../escape"}}}
	if e := r.Restore(context.Background(), unsafe, "", filepath.Join(t.TempDir(), "out")); e == nil {
		t.Fatal("unsafe link accepted")
	}
	key, _ := NewKey()
	if wrong, e := Open(context.Background(), &storage.Local{Root: remote}, t.TempDir(), key, false, io.Discard); e == nil {
		wrong.Close()
		t.Fatal("wrong key accepted")
	}
	for name := range r.Objects {
		if strings.HasPrefix(name, "packs/") {
			p := filepath.Join(remote, filepath.FromSlash(name))
			b, _ := os.ReadFile(p)
			b[len(b)-1] ^= 0xff
			put(t, p, b)
			break
		}
	}
	if e := r.Verify(context.Background(), s); e == nil {
		t.Fatal("corruption undetected")
	}
}

func TestEmptySnapshotRestoresAndMissingPathFails(t *testing.T) {
	r, source, _, _ := fixture(t)
	s := backup(t, r, source)
	dest := filepath.Join(t.TempDir(), "restore")
	if err := r.Restore(context.Background(), s, "", dest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatal(err)
	}
	if err := r.Restore(context.Background(), s, "missing.dat", filepath.Join(t.TempDir(), "out")); err == nil {
		t.Fatal("missing file silently restored")
	}
}

type concurrencyStore struct {
	storage.Store
	active      atomic.Int32
	maximum     atomic.Int32
	earlyCommit atomic.Bool
}

func (s *concurrencyStore) Put(ctx context.Context, key, file string) error {
	if strings.HasPrefix(key, "snapshots/") && s.active.Load() != 0 {
		s.earlyCommit.Store(true)
	}
	if strings.HasPrefix(key, "packs/") {
		n := s.active.Add(1)
		defer s.active.Add(-1)
		for old := s.maximum.Load(); n > old; old = s.maximum.Load() {
			if s.maximum.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	return s.Store.Put(ctx, key, file)
}

func TestConcurrentUploadsAreBoundedAndCommitWaits(t *testing.T) {
	r, source, _, _ := fixture(t)
	r.TargetPackSize = 64 << 10
	store := &concurrencyStore{Store: r.Store}
	r.Store = store
	put(t, filepath.Join(source, "world/region/r.mca"), randomBytes(4<<20))
	s, err := r.Backup(context.Background(), BackupOptions{Source: source, UploadConcurrency: 3})
	if err != nil {
		t.Fatal(err)
	}
	if max := store.maximum.Load(); max < 2 || max > 3 {
		t.Fatalf("wanted concurrent uploads bounded by 3; observed %d", max)
	}
	if store.earlyCommit.Load() || store.active.Load() != 0 {
		t.Fatal("snapshot committed before uploads finished")
	}
	if err = r.Verify(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

func TestRetentionCalendarAndIndefiniteYears(t *testing.T) {
	loc, e := time.LoadLocation("Europe/London")
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 9, 10, 20, 0, 0, 0, loc)
	var all []Summary
	for d := 0; d < 365*4; d++ {
		at := now.AddDate(0, 0, -d)
		all = append(all, Summary{at.Format("2006-01-02"), at, true})
	}
	keep := Retain(all, now, loc)
	for _, id := range []string{"2026-09-10", "2026-09-04", "2026-08-31", "2026-08-23", "2025-12-31", "2024-12-31", "2023-12-31"} {
		if !keep[id] {
			t.Errorf("missing retained bucket %s", id)
		}
	}
	if keep["2026-07-15"] {
		t.Error("unselected old daily snapshot kept")
	}
	// ISO week crosses New Year, and calendar days remain correct at DST changes.
	boundary := time.Date(2027, 1, 3, 12, 0, 0, 0, loc)
	items := []Summary{{"a", boundary.AddDate(0, 0, -3), true}, {"b", boundary, true}}
	if len(Retain(items, boundary, loc)) != 2 {
		t.Fatal("year boundary lost yearly snapshot")
	}
	incomplete := Summary{"partial", now.Add(time.Minute), false}
	all = append(all, incomplete)
	retained := Retain(all, now.Add(time.Minute), loc)
	if !retained["2026-09-10"] || !retained["partial"] {
		t.Fatal("partial snapshot displaced complete backup")
	}
}

func TestPurgeCompactsSharedPacksAndKeepsRestores(t *testing.T) {
	r, source, remote, _ := fixture(t)
	ctx := context.Background()
	a := randomBytes(128 << 10)
	b := randomBytes(96 << 10)
	put(t, filepath.Join(source, "a.dat"), a)
	put(t, filepath.Join(source, "b.dat"), b)
	t1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	s1, e := r.Backup(ctx, BackupOptions{Source: source, Now: func() time.Time { return t1 }})
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(source, "b.dat")); e != nil {
		t.Fatal(e)
	}
	t2 := time.Date(2024, 12, 31, 12, 0, 0, 0, time.UTC)
	s2, e := r.Backup(ctx, BackupOptions{Source: source, Now: func() time.Time { return t2 }})
	if e != nil {
		t.Fatal(e)
	}
	// Make physical packs older than the garbage-collection grace period.
	purgeTime := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	old := purgeTime.Add(-72 * time.Hour)
	for key, o := range r.Objects {
		if strings.HasPrefix(key, "packs/") || strings.HasPrefix(key, "indexes/") {
			o.Created = old
			r.Objects[key] = o
			p := filepath.Join(remote, filepath.FromSlash(key))
			if e = os.Chtimes(p, old, old); e != nil {
				t.Fatal(e)
			}
		}
	}
	p, e := r.PlanPurge(ctx, purgeTime, time.UTC)
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Expire) != 1 || p.Expire[0] != s1.ID || len(p.CompactPacks) == 0 {
		t.Fatalf("unexpected purge plan %+v", p)
	}
	if e = r.ApplyPurge(ctx, p); e != nil {
		t.Fatal(e)
	}
	if e = r.Verify(ctx, s2); e != nil {
		t.Fatal(e)
	}
	if _, e = r.LoadSnapshot(ctx, s1.ID); e == nil {
		t.Fatal("expired snapshot still present")
	}
	dest := filepath.Join(t.TempDir(), "out")
	if e = r.Restore(ctx, s2, "", dest); e != nil {
		t.Fatal(e)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "a.dat"))
	if !bytes.Equal(got, a) {
		t.Fatal("purge damaged retained file")
	}
}
