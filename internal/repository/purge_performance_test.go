package repository

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	"mct-backup/internal/storage"
)

func largeMarkFixture(tb testing.TB, n int) (*Repo, *Snapshot) {
	tb.Helper()
	dir := tb.TempDir()
	db, err := bolt.Open(filepath.Join(dir, "index.db"), 0600, nil)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	hashes := make([]string, n)
	for i := range hashes {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(i))
		h := sha256.Sum256(b[:])
		hashes[i] = hex.EncodeToString(h[:])
	}
	sort.Strings(hashes)
	ref, _ := json.Marshal(Ref{Pack: "packs/test", Length: 1, Raw: 1})
	// Sorted batches keep fixture construction out of the old pathological path.
	for start := 0; start < n; start += 1000 {
		end := min(n, start+1000)
		if err = db.Update(func(tx *bolt.Tx) error {
			b, e := tx.CreateBucketIfNotExists([]byte("chunks"))
			if e != nil {
				return e
			}
			for _, hash := range hashes[start:end] {
				if e = b.Put([]byte(hash), ref); e != nil {
					return e
				}
			}
			return nil
		}); err != nil {
			tb.Fatal(err)
		}
	}
	rand.New(rand.NewSource(17)).Shuffle(n, func(i, j int) { hashes[i], hashes[j] = hashes[j], hashes[i] })
	r := &Repo{DB: db, Objects: map[string]storage.Object{"packs/test": {Key: "packs/test", Size: 1}}, Log: io.Discard, live: make(map[[32]byte]struct{})}
	return r, &Snapshot{ID: "large", Files: []File{{Path: "large.mca", Kind: "file", Chunks: hashes}}}
}

func TestMarkLiveLargeRandomSet(t *testing.T) {
	r, s := largeMarkFixture(t, 100000)
	before := r.DB.Stats()
	start := time.Now()
	if err := r.markLive(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if len(r.live) != 100000 {
		t.Fatal("lost live chunks", len(r.live))
	}
	if err := r.markLive(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if len(r.live) != 100000 {
		t.Fatal("duplicate snapshot inflated live set")
	}
	after := r.DB.Stats()
	if after.TxStats.Write != before.TxStats.Write {
		t.Fatal("marking wrote to the database")
	}
	t.Logf("100,000 random references plus duplicate pass: %s; database writes: 0", time.Since(start))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.markLive(ctx, s); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
}

func BenchmarkMarkLiveMillion(b *testing.B) {
	r, s := largeMarkFixture(b, 1000000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r.live = make(map[[32]byte]struct{}, 1000000)
		if err := r.markLive(context.Background(), s); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPurgeRequiresCompletePlan(t *testing.T) {
	r, source, _, _ := fixture(t)
	put(t, filepath.Join(source, "a"), []byte("data"))
	backup(t, r, source)
	if err := r.ApplyPurge(context.Background(), &PurgePlan{}); err == nil {
		t.Fatal("accepted unbuilt plan")
	}
	p, err := r.PlanPurge(context.Background(), time.Now(), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = r.PlanPurge(ctx, time.Now(), time.UTC); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = r.ApplyPurge(context.Background(), p); err == nil {
		t.Fatal("accepted stale plan after failed planning")
	}
}

type countReadStore struct {
	storage.Store
	mu    sync.Mutex
	reads int
}

func (s *countReadStore) Read(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.Store.Read(ctx, key, offset, length)
}

func TestAuthenticatedMetadataCache(t *testing.T) {
	r, source, _, _ := fixture(t)
	put(t, filepath.Join(source, "a"), []byte("data"))
	s := backup(t, r, source)
	store := &countReadStore{Store: r.Store}
	r.Store = store
	key := "snapshots/" + s.ID
	if _, err := r.LoadSnapshot(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
	first := store.reads
	if _, err := r.LoadSnapshot(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
	if store.reads != first {
		t.Fatal("cache did not avoid remote read")
	}
	cached := filepath.Join(r.Dir, "metadata-cache", Hash([]byte(key))+".enc")
	if err := os.WriteFile(cached, []byte("corrupt cache"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LoadSnapshot(context.Background(), s.ID); err != nil {
		t.Fatal(err)
	}
	if store.reads != first+1 {
		t.Fatal("corrupt cache was not refetched")
	}
	delete(r.Objects, key)
	if _, err := r.LoadSnapshot(context.Background(), s.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("cache resurrected absent remote object", err)
	}
}
