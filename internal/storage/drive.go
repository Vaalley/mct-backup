package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"mct-backup/internal/config"
)

type Drive struct {
	mu        sync.RWMutex
	Client    *http.Client
	Folder    string
	StateDir  string
	API       string
	UploadAPI string
	objects   map[string]driveFile
}

type driveFile struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Size       int64             `json:"size,string"`
	Created    time.Time         `json:"createdTime"`
	MD5        string            `json:"md5Checksum"`
	Properties map[string]string `json:"appProperties"`
}

type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("Google Drive HTTP %d: %s", e.Status, e.Message)
}
func statusIs(err error, code int) bool {
	var e *apiError
	return errors.As(err, &e) && e.Status == code
}

func NewDrive(client *http.Client, folder, state string) *Drive {
	return &Drive{Client: client, Folder: folder, StateDir: state, API: "https://www.googleapis.com/drive/v3", UploadAPI: "https://www.googleapis.com/upload/drive/v3", objects: make(map[string]driveFile)}
}

func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (d *Drive) request(ctx context.Context, method, u string, body []byte, headers map[string]string) (*http.Response, error) {
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := d.Client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if attempt == 5 {
				return nil, fmt.Errorf("Google Drive request failed: %w", err)
			}
			if err := pause(ctx, time.Second<<attempt); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			return resp, nil
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		resp.Body.Close()
		retry := resp.StatusCode == 429 || resp.StatusCode >= 500 || (resp.StatusCode == 403 && (bytes.Contains(b, []byte("rateLimitExceeded")) || bytes.Contains(b, []byte("userRateLimitExceeded"))))
		if !retry || attempt == 5 {
			return nil, &apiError{resp.StatusCode, string(b)}
		}
		delay := time.Second << attempt
		if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 && n <= 120 {
			delay = time.Duration(n) * time.Second
		}
		if err := pause(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("Google Drive retries exhausted")
}

func (d *Drive) json(ctx context.Context, method, endpoint string, body any, out any) error {
	var b []byte
	var err error
	if body != nil {
		b, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	r, err := d.request(ctx, method, endpoint, b, map[string]string{"Content-Type": "application/json"})
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if out != nil {
		return json.NewDecoder(r.Body).Decode(out)
	}
	return nil
}

func quote(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'"
}

func (d *Drive) list(ctx context.Context, query string) ([]driveFile, error) {
	var all []driveFile
	token := ""
	for {
		q := url.Values{"q": {query}, "pageSize": {"1000"}, "fields": {"nextPageToken,files(id,name,size,createdTime,md5Checksum,appProperties)"}, "spaces": {"drive"}}
		if token != "" {
			q.Set("pageToken", token)
		}
		var page struct {
			Files []driveFile `json:"files"`
			Next  string      `json:"nextPageToken"`
		}
		if err := d.json(ctx, "GET", d.API+"/files?"+q.Encode(), nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Files...)
		token = page.Next
		if token == "" {
			break
		}
	}
	return all, nil
}

// FindFolder restricts discovery to repositories created by this OAuth app.
func (d *Drive) FindFolder(ctx context.Context, name string) (string, error) {
	files, err := d.list(ctx, "trashed = false and mimeType = 'application/vnd.google-apps.folder' and name = "+quote(name)+" and appProperties has { key='mctRepository' and value='1' }")
	if err != nil {
		return "", err
	}
	if len(files) > 1 {
		return "", fmt.Errorf("multiple repositories named %q; use login --folder-id", name)
	}
	if len(files) == 1 {
		return files[0].ID, nil
	}
	return "", nil
}

func (d *Drive) newID(ctx context.Context) (string, error) {
	var r struct {
		IDs []string `json:"ids"`
	}
	if err := d.json(ctx, "GET", d.API+"/files/generateIds?count=1&space=drive&type=files", nil, &r); err != nil {
		return "", err
	}
	if len(r.IDs) != 1 {
		return "", errors.New("Google Drive returned no object ID")
	}
	return r.IDs[0], nil
}

func (d *Drive) CreateFolder(ctx context.Context, name string) (string, error) {
	id, err := d.newID(ctx)
	if err != nil {
		return "", err
	}
	err = d.json(ctx, "POST", d.API+"/files", map[string]any{"id": id, "name": name, "mimeType": "application/vnd.google-apps.folder", "appProperties": map[string]string{"mctRepository": "1"}}, nil)
	if statusIs(err, 409) {
		err = nil
	}
	return id, err
}

func (d *Drive) List(ctx context.Context) ([]Object, error) {
	files, err := d.list(ctx, "trashed = false and "+quote(d.Folder)+" in parents")
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]driveFile)
	var objects []Object
	for _, f := range files {
		key := f.Properties["mctKey"]
		if key == "" {
			continue
		}
		if !ValidKey(key) {
			return nil, fmt.Errorf("invalid repository object key %q", key)
		}
		if _, exists := byKey[key]; exists {
			return nil, fmt.Errorf("duplicate repository object %q; refusing ambiguous data", key)
		}
		byKey[key] = f
		objects = append(objects, Object{key, f.Size, f.Created})
	}
	d.mu.Lock()
	d.objects = byKey
	d.mu.Unlock()
	return objects, nil
}

func (d *Drive) resolve(ctx context.Context, key string) (driveFile, error) {
	d.mu.RLock()
	cached, ok := d.objects[key]
	d.mu.RUnlock()
	if ok {
		return cached, nil
	}
	files, err := d.list(ctx, "trashed = false and "+quote(d.Folder)+" in parents and appProperties has { key='mctKey' and value="+quote(key)+" }")
	if err != nil {
		return driveFile{}, err
	}
	if len(files) == 0 {
		return driveFile{}, ErrNotFound
	}
	if len(files) != 1 {
		return driveFile{}, fmt.Errorf("duplicate repository object %q", key)
	}
	d.mu.Lock()
	d.objects[key] = files[0]
	d.mu.Unlock()
	return files[0], nil
}

type uploadState struct {
	ID, URL, SHA256 string
	Size            int64
}

func (d *Drive) Put(ctx context.Context, key, source string) error {
	if !ValidKey(key) {
		return fmt.Errorf("invalid object key %q", key)
	}
	sha, size, err := FileHash(source)
	if err != nil {
		return err
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	m := md5.New()
	if _, err = io.Copy(m, in); err != nil {
		return err
	}
	md := hex.EncodeToString(m.Sum(nil))
	if f, err := d.resolve(ctx, key); err == nil {
		if f.Size != size || f.MD5 != md {
			return fmt.Errorf("immutable object conflict: %s", key)
		}
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	h := sha256.Sum256([]byte(d.Folder + "/" + key))
	statePath := filepath.Join(d.StateDir, "uploads", hex.EncodeToString(h[:])+".json")
	st := uploadState{SHA256: sha, Size: size}
	if b, e := os.ReadFile(statePath); e == nil {
		if e = json.Unmarshal(b, &st); e != nil {
			return e
		}
		if st.SHA256 != sha || st.Size != size {
			return fmt.Errorf("pending upload content changed for %s", key)
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if st.ID == "" {
		st.ID, err = d.newID(ctx)
		if err != nil {
			return err
		}
	}
	save := func() error {
		b, e := json.Marshal(st)
		if e != nil {
			return e
		}
		return config.WritePrivate(statePath, b)
	}
	if err = save(); err != nil {
		return err
	}
	finish := func() error {
		var f driveFile
		if err := d.json(ctx, "GET", d.API+"/files/"+url.PathEscape(st.ID)+"?fields=id,name,size,createdTime,md5Checksum,appProperties", nil, &f); err != nil {
			return err
		}
		if f.Size != size || f.MD5 != md || f.Properties["mctKey"] != key {
			return fmt.Errorf("uploaded object failed checksum verification: %s", key)
		}
		d.mu.Lock()
		d.objects[key] = f
		d.mu.Unlock()
		return os.Remove(statePath)
	}
	for sessions := 0; sessions < 3; sessions++ {
		if st.URL == "" {
			meta, _ := json.Marshal(map[string]any{"id": st.ID, "name": strings.ReplaceAll(key, "/", "-"), "parents": []string{d.Folder}, "appProperties": map[string]string{"mctKey": key}})
			r, e := d.request(ctx, "POST", d.UploadAPI+"/files?uploadType=resumable", meta, map[string]string{"Content-Type": "application/json", "X-Upload-Content-Type": "application/octet-stream", "X-Upload-Content-Length": strconv.FormatInt(size, 10)})
			if statusIs(e, 409) {
				return finish()
			}
			if e != nil {
				return e
			}
			st.URL = r.Header.Get("Location")
			r.Body.Close()
			u, e := url.Parse(st.URL)
			base, _ := url.Parse(d.UploadAPI)
			if e != nil || u.Host == "" || (u.Host != base.Host && !(u.Scheme == "https" && strings.HasSuffix(u.Hostname(), ".googleapis.com"))) {
				return errors.New("invalid Drive resumable upload location")
			}
			if err = save(); err != nil {
				return err
			}
		}
		// Query the server's offset, including after a process restart.
		r, e := d.request(ctx, "PUT", st.URL, nil, map[string]string{"Content-Range": fmt.Sprintf("bytes */%d", size)})
		if statusIs(e, 404) || statusIs(e, 410) {
			st.URL = ""
			if err = save(); err != nil {
				return err
			}
			continue
		}
		if e != nil {
			return e
		}
		if r.StatusCode == 200 || r.StatusCode == 201 {
			r.Body.Close()
			return finish()
		}
		offset, e := uploadOffset(r)
		r.Body.Close()
		if e != nil {
			return e
		}
		for offset < size {
			n := min(int64(8<<20), size-offset)
			b := make([]byte, n)
			if _, err = in.ReadAt(b, offset); err != nil {
				return err
			}
			r, e = d.request(ctx, "PUT", st.URL, b, map[string]string{"Content-Type": "application/octet-stream", "Content-Range": fmt.Sprintf("bytes %d-%d/%d", offset, offset+n-1, size)})
			if e != nil {
				return e
			} // session is persisted; next run probes the acknowledged offset
			if r.StatusCode == 200 || r.StatusCode == 201 {
				r.Body.Close()
				return finish()
			}
			next, e := uploadOffset(r)
			r.Body.Close()
			if e != nil {
				return e
			}
			if next <= offset || next > size {
				return errors.New("Drive upload made no valid progress")
			}
			offset = next
		}
		return errors.New("Drive upload did not return a completion response")
	}
	return errors.New("Drive upload session repeatedly expired; retry backup")
}

func uploadOffset(r *http.Response) (int64, error) {
	if r.StatusCode != 308 {
		return 0, fmt.Errorf("unexpected upload status %d", r.StatusCode)
	}
	v := r.Header.Get("Range")
	if v == "" {
		return 0, nil
	}
	if !strings.HasPrefix(v, "bytes=0-") {
		return 0, fmt.Errorf("invalid upload range %q", v)
	}
	n, e := strconv.ParseInt(strings.TrimPrefix(v, "bytes=0-"), 10, 64)
	return n + 1, e
}

func (d *Drive) Read(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	if offset < 0 || length < -1 {
		return nil, errors.New("invalid read range")
	}
	if length == 0 {
		return []byte{}, nil
	}
	f, err := d.resolve(ctx, key)
	if err != nil {
		return nil, err
	}
	headers := map[string]string{}
	if length >= 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	} else if offset != 0 {
		headers["Range"] = fmt.Sprintf("bytes=%d-", offset)
	}
	r, err := d.request(ctx, "GET", d.API+"/files/"+url.PathEscape(f.ID)+"?alt=media", nil, headers)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if headers["Range"] != "" && r.StatusCode != http.StatusPartialContent {
		return nil, errors.New("Drive ignored requested byte range")
	}
	var reader io.Reader = r.Body
	if length >= 0 {
		reader = io.LimitReader(reader, length+1)
	}
	b, err := io.ReadAll(reader)
	if err == nil && length >= 0 && int64(len(b)) != length {
		err = io.ErrUnexpectedEOF
	}
	return b, err
}

func (d *Drive) Delete(ctx context.Context, key string) error {
	f, err := d.resolve(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	err = d.json(ctx, "DELETE", d.API+"/files/"+url.PathEscape(f.ID), nil, nil)
	if statusIs(err, 404) {
		err = nil
	}
	if err == nil {
		d.mu.Lock()
		delete(d.objects, key)
		d.mu.Unlock()
	}
	return err
}
