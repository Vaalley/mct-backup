package auth

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"mct-backup/internal/config"
)

func TestClientRefreshPersistsV2AuthIDAndReusesToken(t *testing.T) {
	dir := t.TempDir()
	c := &config.Config{Version: 1, AuthID: "keyid:password"}
	var refreshes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/refresh" {
			refreshes.Add(1)
			if got := r.Header.Get("X-AuthID"); got != "keyid:password" {
				t.Errorf("X-AuthID = %q", got)
			}
			if got := r.Header.Get("User-Agent"); got != "mct-backup" {
				t.Errorf("User-Agent = %q", got)
			}
			fmt.Fprint(w, `{"access_token":"access-token","expires":3600,"type":"googledrive","v2_authid":"v2:googledrive:refresh-token"}`)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer access-token" {
			t.Errorf("Authorization = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	c.OAuthURL = server.URL + "/refresh"
	client, err := Client(context.Background(), c, dir)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		resp, err := client.Get(server.URL + "/resource")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if got := refreshes.Load(); got != 1 {
		t.Fatalf("refresh request count = %d, want 1", got)
	}
	if c.AuthID != "v2:googledrive:refresh-token" {
		t.Fatalf("AuthID = %q", c.AuthID)
	}
	saved, err := config.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if saved.AuthID != c.AuthID {
		t.Fatalf("saved AuthID = %q", saved.AuthID)
	}
	info, err := os.Stat(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("config permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestRefreshRetriesBadAuthIDOnceWithoutLeakingAuthID(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("X-Reason", "Invalid authid in query")
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()
	const authID = "private-key:private-password"
	src := &authIDSource{
		ctx:  context.Background(),
		c:    &config.Config{AuthID: authID},
		dir:  t.TempDir(),
		http: server.Client(),
		url:  server.URL,
		pause: func(context.Context, time.Duration) error {
			return nil
		},
	}
	_, err := src.Token()
	if err == nil || !strings.Contains(err.Error(), "Invalid authid in query") {
		t.Fatalf("Token() error = %v", err)
	}
	if strings.Contains(err.Error(), authID) {
		t.Fatalf("error contains AuthID: %v", err)
	}
	if !strings.HasSuffix(err.Error(), "; run mct-backup login") {
		t.Fatalf("error does not end with login instruction: %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}

func TestRefreshRetriesServerError(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `{"access_token":"access-token","expires":3600}`)
	}))
	defer server.Close()
	var pauses []time.Duration
	src := &authIDSource{
		ctx:  context.Background(),
		c:    &config.Config{AuthID: "keyid:password"},
		dir:  t.TempDir(),
		http: server.Client(),
		url:  server.URL,
		pause: func(_ context.Context, delay time.Duration) error {
			pauses = append(pauses, delay)
			return nil
		},
	}
	token, err := src.Token()
	if err != nil {
		t.Fatal(err)
	}
	if token.AccessToken != "access-token" {
		t.Fatalf("AccessToken = %q", token.AccessToken)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
	if len(pauses) != 1 || pauses[0] != time.Second {
		t.Fatalf("pauses = %v, want [1s]", pauses)
	}
}

func TestLoginURL(t *testing.T) {
	tests := []struct {
		refreshURL string
		want       string
	}{
		{
			refreshURL: DefaultOAuthURL,
			want:       "https://duplicati-oauth-handler.appspot.com?type=googledrive",
		},
		{
			refreshURL: "https://u:p@h:8080/refresh?x=1",
			want:       "https://u:p@h:8080?x=1&type=googledrive",
		},
	}
	for _, tt := range tests {
		got, err := LoginURL(tt.refreshURL)
		if err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Errorf("LoginURL(%q) = %q, want %q", tt.refreshURL, got, tt.want)
		}
	}
}

func TestLoginPromptsForAndValidatesAuthID(t *testing.T) {
	t.Setenv("MCT_BACKUP_OAUTH_URL", "")
	c := &config.Config{}
	var out strings.Builder
	if err := Login(c, bufio.NewReader(strings.NewReader("abc:def\n")), &out, true, ""); err != nil {
		t.Fatal(err)
	}
	if c.AuthID != "abc:def" {
		t.Fatalf("AuthID = %q", c.AuthID)
	}
	if !strings.Contains(out.String(), "https://duplicati-oauth-handler.appspot.com?type=googledrive") ||
		!strings.Contains(out.String(), "Sign in with Google on Duplicati's login page") {
		t.Fatalf("login instructions missing: %s", out.String())
	}
	if err := Login(&config.Config{}, bufio.NewReader(strings.NewReader("nocolon\n")), &out, true, ""); err == nil {
		t.Fatal("Login accepted an AuthID without a colon")
	}
}

func TestRefreshURLPrecedence(t *testing.T) {
	t.Setenv("MCT_BACKUP_OAUTH_URL", "https://env.example/refresh")
	if got := RefreshURL(&config.Config{OAuthURL: "https://config.example/refresh"}); got != "https://config.example/refresh" {
		t.Fatalf("config URL = %q", got)
	}
	if got := RefreshURL(&config.Config{}); got != "https://env.example/refresh" {
		t.Fatalf("environment URL = %q", got)
	}
	t.Setenv("MCT_BACKUP_OAUTH_URL", "")
	if got := RefreshURL(&config.Config{}); got != DefaultOAuthURL {
		t.Fatalf("default URL = %q", got)
	}
}
