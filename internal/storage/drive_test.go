package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"mct-backup/internal/config"
)

// This fake implements the real Drive protocol shape, including resumable
// status probes, MD5 metadata, range reads and pre-generated file IDs.
type fakeDrive struct {
	data       []byte
	properties map[string]string
	complete   bool
	server     *httptest.Server
	puts       int
	nextOffset int64
}

func newFake(t *testing.T) *fakeDrive {
	t.Helper()
	f := &fakeDrive{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/drive/v3/files/generateIds":
			fmt.Fprint(w, `{"ids":["object-id"]}`)
		case r.URL.Path == "/drive/v3/files" && r.Method == "GET":
			if f.complete {
				_ = json.NewEncoder(w).Encode(map[string]any{"files": []any{f.metadata()}})
			} else {
				fmt.Fprint(w, `{"files":[]}`)
			}
		case r.URL.Path == "/upload/drive/v3/files":
			var m struct {
				Properties map[string]string `json:"appProperties"`
			}
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				t.Error(err)
			}
			f.properties = m.Properties
			if f.complete {
				w.WriteHeader(409)
				return
			}
			w.Header().Set("Location", f.server.URL+"/session")
			w.WriteHeader(200)
		case r.URL.Path == "/session":
			rangeHeader := r.Header.Get("Content-Range")
			if strings.HasPrefix(rangeHeader, "bytes */") {
				if f.complete {
					w.WriteHeader(200)
					return
				}
				if len(f.data) > 0 {
					w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(f.data)-1))
				}
				w.WriteHeader(308)
				return
			}
			var start, end, total int64
			if _, err := fmt.Sscanf(rangeHeader, "bytes %d-%d/%d", &start, &end, &total); err != nil {
				t.Error(err)
			}
			if start != int64(len(f.data)) {
				t.Errorf("resume started at %d, want %d", start, len(f.data))
				w.WriteHeader(400)
				return
			}
			f.nextOffset = start
			f.puts++
			b, _ := io.ReadAll(r.Body)
			f.data = append(f.data, b...)
			if int64(len(f.data)) == total {
				f.complete = true
				w.WriteHeader(200)
			} else {
				w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(f.data)-1))
				w.WriteHeader(308)
			}
		case r.URL.Path == "/drive/v3/files/object-id" && r.Method == "DELETE":
			f.complete = false
			w.WriteHeader(204)
		case r.URL.Path == "/drive/v3/files/object-id" && r.URL.Query().Get("alt") == "media":
			if v := r.Header.Get("Range"); v != "" {
				var start, end int
				if _, err := fmt.Sscanf(v, "bytes=%d-%d", &start, &end); err != nil {
					t.Error(err)
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(f.data)))
				w.WriteHeader(206)
				_, _ = w.Write(f.data[start : end+1])
			} else {
				_, _ = w.Write(f.data)
			}
		case r.URL.Path == "/drive/v3/files/object-id":
			_ = json.NewEncoder(w).Encode(f.metadata())
		default:
			t.Errorf("unexpected Drive request %s %s", r.Method, r.URL)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}
func (f *fakeDrive) metadata() map[string]any {
	h := md5.Sum(f.data)
	return map[string]any{"id": "object-id", "name": "pack", "size": strconv.Itoa(len(f.data)), "createdTime": time.Now().UTC().Format(time.RFC3339), "md5Checksum": hex.EncodeToString(h[:]), "appProperties": f.properties}
}
func (f *fakeDrive) client(dir string) *Drive {
	d := NewDrive(f.server.Client(), "folder", dir)
	d.API = f.server.URL + "/drive/v3"
	d.UploadAPI = f.server.URL + "/upload/drive/v3"
	return d
}

func TestDriveUploadRangeReadAndImmutability(t *testing.T) {
	fake := newFake(t)
	dir := t.TempDir()
	source := filepath.Join(dir, "pack")
	data := bytes.Repeat([]byte("Minecraft"), 1024)
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	d := fake.client(dir)
	ctx := context.Background()
	if err := d.Put(ctx, "packs/test", source); err != nil {
		t.Fatal(err)
	}
	if err := d.Put(ctx, "packs/test", source); err != nil {
		t.Fatal(err)
	}
	if fake.puts != 1 {
		t.Fatal("duplicate upload", fake.puts)
	}
	objects, err := d.List(ctx)
	if err != nil || len(objects) != 1 {
		t.Fatal(objects, err)
	}
	got, err := d.Read(ctx, "packs/test", 5, 20)
	if err != nil || !bytes.Equal(got, data[5:25]) {
		t.Fatal("range restore", err)
	}
	if err = os.WriteFile(source, []byte("different"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = d.Put(ctx, "packs/test", source); err == nil {
		t.Fatal("immutable overwrite allowed")
	}
	if err = d.Delete(ctx, "packs/test"); err != nil {
		t.Fatal(err)
	}
}

func TestDriveResumesPersistedSession(t *testing.T) {
	fake := newFake(t)
	dir := t.TempDir()
	source := filepath.Join(dir, "pack")
	data := []byte("a pack whose beginning was already uploaded")
	if err := os.WriteFile(source, data, 0600); err != nil {
		t.Fatal(err)
	}
	fake.data = append([]byte{}, data[:8]...)
	fake.properties = map[string]string{"mctKey": "packs/test"}
	sha, size, err := FileHash(source)
	if err != nil {
		t.Fatal(err)
	}
	state := uploadState{ID: "object-id", URL: fake.server.URL + "/session", SHA256: sha, Size: size}
	b, _ := json.Marshal(state)
	h := sha256.Sum256([]byte("folder/packs/test"))
	p := filepath.Join(dir, "uploads", hex.EncodeToString(h[:])+".json")
	if err = config.WritePrivate(p, b); err != nil {
		t.Fatal(err)
	}
	d := fake.client(dir)
	if err = d.Put(context.Background(), "packs/test", source); err != nil {
		t.Fatal(err)
	}
	if fake.nextOffset != 8 || !bytes.Equal(fake.data, data) {
		t.Fatal("upload did not resume correctly")
	}
	if _, err = os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("completed upload state remains")
	}
}
