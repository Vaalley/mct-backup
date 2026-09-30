package repository

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Summary struct {
	ID        string
	Completed time.Time
	Complete  bool
}

// Retain selects calendar buckets (including the current partial bucket), not
// elapsed durations. A complete snapshot wins over an incomplete one in a bucket.
func Retain(all []Summary, now time.Time, loc *time.Location) map[string]bool {
	keep := map[string]bool{}
	if len(all) == 0 {
		return keep
	}
	buckets := map[string]Summary{}
	local := now.In(loc)
	today := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
	week := today.AddDate(0, 0, -(int(today.Weekday())+6)%7)
	month := time.Date(local.Year(), local.Month(), 1, 0, 0, 0, 0, loc)
	var newest, newestComplete Summary
	for _, s := range all {
		if newest.ID == "" || s.Completed.After(newest.Completed) {
			newest = s
		}
		if s.Complete && (newestComplete.ID == "" || s.Completed.After(newestComplete.Completed)) {
			newestComplete = s
		}
		// A grace period protects recent interrupted work and makes purge conservative.
		if s.Completed.After(now.Add(-24 * time.Hour)) {
			keep[s.ID] = true
		}
		t := s.Completed.In(loc)
		keys := []string{fmt.Sprintf("y:%04d", t.Year())}
		if !t.Before(today.AddDate(0, 0, -6)) && !t.After(now) {
			keys = append(keys, "d:"+t.Format("2006-01-02"))
		}
		if !t.Before(week.AddDate(0, 0, -21)) && !t.After(now) {
			y, w := t.ISOWeek()
			keys = append(keys, fmt.Sprintf("w:%d-%02d", y, w))
		}
		if !t.Before(month.AddDate(0, -11, 0)) && !t.After(now) {
			keys = append(keys, "m:"+t.Format("2006-01"))
		}
		for _, key := range keys {
			prev, ok := buckets[key]
			if !ok || (!prev.Complete && s.Complete) || (prev.Complete == s.Complete && (s.Completed.After(prev.Completed) || (s.Completed.Equal(prev.Completed) && s.ID > prev.ID))) {
				buckets[key] = s
			}
		}
	}
	for _, s := range buckets {
		keep[s.ID] = true
	}
	keep[newest.ID] = true
	if newestComplete.ID != "" {
		keep[newestComplete.ID] = true
	}
	return keep
}

type PurgePlan struct {
	Keep             []string `json:"keep"`
	Expire           []string `json:"expire"`
	DeleteObjects    []string `json:"delete_objects"`
	CompactPacks     []string `json:"compact_packs"`
	ReclaimableBytes int64    `json:"reclaimable_bytes_estimate"`
}

// Keep exact SHA-256 keys in memory. Random inserts into a single bbolt write
// transaction previously caused quadratic slice insertion and multi-hour purges.
func chunkKey(hash string) ([32]byte, error) {
	var key [32]byte
	if len(hash) != 64 {
		return key, errors.New("invalid chunk hash length")
	}
	_, err := hex.Decode(key[:], []byte(hash))
	return key, err
}

func (r *Repo) isLive(hash string) (bool, error) {
	if r.live == nil {
		return false, errors.New("purge reachability set is missing")
	}
	key, err := chunkKey(hash)
	if err != nil {
		return false, err
	}
	_, ok := r.live[key]
	return ok, nil
}

func (r *Repo) markLive(ctx context.Context, s *Snapshot) error {
	return r.DB.View(func(tx *bolt.Tx) error {
		chunks := tx.Bucket([]byte("chunks"))
		count := 0
		for _, f := range s.Files {
			for _, hash := range f.Chunks {
				if count%1024 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				count++
				key, err := chunkKey(hash)
				if err != nil {
					return err
				}
				if _, ok := r.live[key]; ok {
					continue
				}
				b := chunks.Get([]byte(hash))
				if b == nil {
					return fmt.Errorf("retained snapshot %s has a missing chunk; purge aborted", s.ID)
				}
				var ref Ref
				if err = json.Unmarshal(b, &ref); err != nil {
					return err
				}
				if _, ok := r.Objects[ref.Pack]; !ok {
					return fmt.Errorf("retained snapshot references missing pack %s", ref.Pack)
				}
				r.live[key] = struct{}{}
				if len(r.live)%250000 == 0 {
					fmt.Fprintf(r.Log, "Marked %d unique retained chunks\n", len(r.live))
				}
			}
		}
		return ctx.Err()
	})
}

// Download only a small number of indexes concurrently. Each worker returns at
// most one decoded index at a time, so memory does not scale with pack count.
func (r *Repo) visitPackIndexes(ctx context.Context, visit func(string, packIndex) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var keys []string
	for key := range r.Objects {
		if strings.HasPrefix(key, "indexes/") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	type result struct {
		key   string
		index packIndex
		err   error
	}
	jobs := make(chan string)
	results := make(chan result, 4)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for key := range jobs {
				data, err := r.readEncrypted(ctx, key)
				var idx packIndex
				if err == nil {
					err = json.Unmarshal(data, &idx)
				}
				select {
				case results <- result{key, idx, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, key := range keys {
			select {
			case jobs <- key:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { workers.Wait(); close(results) }()
	var first error
	count := 0
	for result := range results {
		if first != nil {
			continue
		}
		if result.err != nil {
			first = result.err
			cancel()
			continue
		}
		if result.index.Version != 1 || result.key != "indexes/"+strings.TrimPrefix(result.index.Pack, "packs/") {
			first = errors.New("invalid pack index identity")
			cancel()
			continue
		}
		if first = visit(result.key, result.index); first != nil {
			cancel()
			continue
		}
		count++
		if count%100 == 0 {
			fmt.Fprintf(r.Log, "Inspected %d/%d pack indexes\n", count, len(keys))
		}
	}
	if first != nil {
		return first
	}
	return ctx.Err()
}

func (r *Repo) PlanPurge(ctx context.Context, now time.Time, loc *time.Location) (*PurgePlan, error) {
	r.live = nil
	r.purgePlan = nil
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fmt.Fprintln(r.Log, "Planning retention; loading snapshot metadata")
	for _, pattern := range []string{"*.job", "*.meta-job"} {
		jobs, err := filepath.Glob(filepath.Join(r.Dir, "spool", pattern))
		if err != nil {
			return nil, err
		}
		if len(jobs) > 0 {
			return nil, errors.New("pending backup uploads exist; run backup to resume them before purging")
		}
	}
	var summaries []Summary
	for _, id := range r.Snapshots() {
		s, err := r.LoadSnapshot(ctx, id)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, Summary{s.ID, s.Completed, s.CoverageComplete})
	}
	if len(summaries) == 0 {
		return nil, errors.New("no committed snapshots; refusing to purge")
	}
	keep := Retain(summaries, now, loc)
	p := &PurgePlan{}
	r.live = make(map[[32]byte]struct{})
	for _, summary := range summaries {
		if !keep[summary.ID] {
			p.Expire = append(p.Expire, summary.ID)
			continue
		}
		p.Keep = append(p.Keep, summary.ID)
		s, err := r.LoadSnapshot(ctx, summary.ID)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(r.Log, "Marking retained snapshot %s\n", summary.ID)
		if err = r.markLive(ctx, s); err != nil {
			return nil, err
		}
	}

	indexed := map[string]bool{}
	if err := r.visitPackIndexes(ctx, func(key string, idx packIndex) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		indexed[idx.Pack] = true
		o, ok := r.Objects[idx.Pack]
		if !ok {
			return fmt.Errorf("index references missing pack %s", idx.Pack)
		}
		if now.Sub(o.Created) < 24*time.Hour {
			return nil
		}
		liveCount := 0
		var deadBytes int64
		for hash, ref := range idx.Chunks {
			live, err := r.isLive(hash)
			if err != nil {
				return err
			}
			if live {
				liveCount++
			} else {
				deadBytes += ref.Length
			}
		}
		if liveCount == 0 {
			p.DeleteObjects = append(p.DeleteObjects, key, idx.Pack)
			p.ReclaimableBytes += o.Size + r.Objects[key].Size
		} else if liveCount < len(idx.Chunks) {
			p.CompactPacks = append(p.CompactPacks, idx.Pack)
			p.ReclaimableBytes += deadBytes
		}
		return nil
	}); err != nil {
		return nil, err
	}

	for key, o := range r.Objects {
		if strings.HasPrefix(key, "packs/") && !indexed[key] && now.Sub(o.Created) >= 24*time.Hour {
			p.DeleteObjects = append(p.DeleteObjects, key)
			p.ReclaimableBytes += o.Size
		}
	}
	sort.Strings(p.CompactPacks)
	sort.Strings(p.DeleteObjects)
	r.purgePlan = p
	fmt.Fprintf(r.Log, "Purge plan ready: %d retained snapshots, %d unique chunks, %d expired snapshots, %d objects to delete, %d packs to compact\n", len(p.Keep), len(r.live), len(p.Expire), len(p.DeleteObjects), len(p.CompactPacks))
	return p, nil
}

// ApplyPurge runs under the same host lock as backup/restore. New packs and their
// indexes are committed before deleting old indexes/packs. Every crash boundary
// therefore leaves either the old data or verified replacements reachable.
func (r *Repo) ApplyPurge(ctx context.Context, p *PurgePlan) error {
	if p == nil || r.purgePlan != p || r.live == nil {
		return errors.New("a completed purge plan from this repository session is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() { r.purgePlan = nil; r.live = nil }()
	for _, id := range p.Expire {
		if err := r.Store.Delete(ctx, "snapshots/"+id); err != nil {
			return err
		}
		delete(r.Objects, "snapshots/"+id)
	}
	old := map[string]bool{}
	for _, pack := range p.CompactPacks {
		old[pack] = true
	}
	c, err := newChunkReader(ctx, r, true)
	if err != nil {
		return err
	}
	defer c.close()
	for _, pack := range p.CompactPacks {
		key := "indexes/" + strings.TrimPrefix(pack, "packs/")
		data, err := r.readEncrypted(ctx, key)
		if err != nil {
			return err
		}
		var idx packIndex
		if err = json.Unmarshal(data, &idx); err != nil {
			return err
		}
		for hash := range idx.Chunks {
			live, err := r.isLive(hash)
			if err != nil {
				return err
			}
			if !live {
				continue
			}
			ref, ok, err := r.lookup(hash)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("missing live chunk during compaction")
			}
			if !old[ref.Pack] {
				continue
			}
			b, err := c.read(hash)
			if err != nil {
				return err
			}
			if _, err = r.addChunk(ctx, b, true); err != nil {
				return err
			}
		}
	}
	if err = r.Flush(ctx); err != nil {
		return err
	}
	if err = r.DB.View(func(tx *bolt.Tx) error {
		chunks := tx.Bucket([]byte("chunks"))
		var encoded [64]byte
		count := 0
		for hash := range r.live {
			if count%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			count++
			hex.Encode(encoded[:], hash[:])
			b := chunks.Get(encoded[:])
			if b == nil {
				return errors.New("live chunk disappeared during purge")
			}
			var ref Ref
			if err := json.Unmarshal(b, &ref); err != nil {
				return err
			}
			if old[ref.Pack] {
				return errors.New("replacement chunks not committed; old packs preserved")
			}
			if _, ok := r.Objects[ref.Pack]; !ok {
				return errors.New("live chunk references a missing pack")
			}
		}
		return ctx.Err()
	}); err != nil {
		return err
	}

	deletions := append([]string{}, p.DeleteObjects...)
	for _, pack := range p.CompactPacks {
		deletions = append(deletions, "indexes/"+strings.TrimPrefix(pack, "packs/"), pack)
	}
	// Delete every obsolete index before its corresponding pack. Orphan packs are
	// recoverable garbage; an index referencing a deleted pack would block recovery.
	sort.Slice(deletions, func(i, j int) bool { return deletions[i] < deletions[j] })
	for _, key := range deletions {
		if err = r.Store.Delete(ctx, key); err != nil {
			return err
		}
		delete(r.Objects, key)
	}
	return r.syncIndex(ctx)
}
