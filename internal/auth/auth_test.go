package auth

import (
	"bufio"
	"bytes"
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"mct-backup/internal/config"
)

func TestCredentialsWizardAndPrivateRefresh(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "client.json")
	if err := os.WriteFile(p, []byte(`{"installed":{"client_id":"client","client_secret":"secret"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := &config.Config{Version: 1}
	var out bytes.Buffer
	if err := Credentials(c, p, bufio.NewReader(strings.NewReader("")), &out); err != nil {
		t.Fatal(err)
	}
	if c.ClientID != "client" {
		t.Fatal("missing credentials")
	}
	token := &oauth2.Token{AccessToken: "access", RefreshToken: "refresh", Expiry: time.Now().Add(time.Hour)}
	s := &savingSource{source: oauth2.StaticTokenSource(token), c: c, dir: dir}
	if _, err := s.Token(); err != nil {
		t.Fatal(err)
	}
	saved, err := config.Load(dir)
	if err != nil || saved.Token.RefreshToken != "refresh" {
		t.Fatal("token not persisted", err)
	}
	info, _ := os.Stat(filepath.Join(dir, "config.json"))
	if info.Mode().Perm() != 0600 {
		t.Fatal("unsafe permissions")
	}
	if scope := OAuth(c).Scopes; len(scope) != 1 || scope[0] != "https://www.googleapis.com/auth/drive.file" {
		t.Fatal("excessive Drive scope")
	}
}

type loginWriter struct{ url chan string }

func (w loginWriter) Write(b []byte) (int, error) {
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "https://accounts.google.com/") {
			select {
			case w.url <- line:
			default:
			}
		}
	}
	return len(b), nil
}

func TestLoginUsesPKCEAndRejectsWrongState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := loginWriter{make(chan string, 1)}
	done := make(chan error, 1)
	go func() { done <- Login(ctx, &config.Config{ClientID: "test"}, w, true, 0, "example.invalid") }()
	var raw string
	select {
	case raw = <-w.url:
	case <-time.After(5 * time.Second):
		t.Fatal("login did not produce URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) < 43 || q.Get("access_type") != "offline" {
		t.Fatal("missing PKCE/offline access")
	}
	callback := q.Get("redirect_uri") + "?state=wrong&code=forged"
	resp, err := http.Get(callback)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal("invalid state accepted")
	}
	select {
	case e := <-done:
		t.Fatal("invalid callback completed login", e)
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("login did not cancel")
	}
}
