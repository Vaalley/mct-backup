package auth

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"mct-backup/internal/config"
)

// Desktop client credentials can be supplied by the app publisher at build time.
// No third-party application's OAuth identity is borrowed.
var DefaultClientID, DefaultClientSecret string

func OAuth(c *config.Config) *oauth2.Config {
	return &oauth2.Config{ClientID: c.ClientID, ClientSecret: c.ClientSecret, Scopes: []string{"https://www.googleapis.com/auth/drive.file"}, Endpoint: oauth2.Endpoint{AuthURL: "https://accounts.google.com/o/oauth2/v2/auth", TokenURL: "https://oauth2.googleapis.com/token", AuthStyle: oauth2.AuthStyleInParams}}
}

type savingSource struct {
	mu     sync.Mutex
	source oauth2.TokenSource
	c      *config.Config
	dir    string
}

func (s *savingSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.source.Token()
	if err != nil {
		return nil, fmt.Errorf("refresh Google login: %w; run mct-backup login", err)
	}
	if s.c.Token == nil || t.AccessToken != s.c.Token.AccessToken || t.RefreshToken != s.c.Token.RefreshToken {
		s.c.Token = t
		if err = config.Save(s.dir, s.c); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func Client(ctx context.Context, c *config.Config, dir string) (*http.Client, error) {
	if c.Token == nil || c.ClientID == "" {
		return nil, errors.New("Google login is missing; run mct-backup login")
	}
	base := &http.Client{Timeout: 2 * time.Minute}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, base)
	client := oauth2.NewClient(ctx, &savingSource{source: OAuth(c).TokenSource(ctx, c.Token), c: c, dir: dir})
	client.Timeout = 2 * time.Minute
	return client, nil
}

func Prompt(in *bufio.Reader, out io.Writer, label string) (string, error) {
	fmt.Fprint(out, label)
	line, err := in.ReadString('\n')
	if err != nil && len(line) == 0 {
		return "", fmt.Errorf("interactive input required (%s): %w", strings.TrimSpace(label), err)
	}
	return strings.TrimSpace(line), nil
}

func Credentials(c *config.Config, path string, in *bufio.Reader, out io.Writer) error {
	if path == "" && c.ClientID != "" {
		return nil
	}
	if path == "" {
		c.ClientID = os.Getenv("MCT_BACKUP_CLIENT_ID")
		c.ClientSecret = os.Getenv("MCT_BACKUP_CLIENT_SECRET")
		if c.ClientID == "" {
			c.ClientID = DefaultClientID
			c.ClientSecret = DefaultClientSecret
		}
		if c.ClientID != "" {
			return nil
		}
		fmt.Fprintln(out, "One-time Google app setup (this build has no publisher OAuth client):\n1. Create/select a project and enable Google Drive API:\n   https://console.cloud.google.com/apis/library/drive.googleapis.com\n2. Configure Google Auth Platform → Branding/Audience. For a personal external app, add your Google account as a test user.\n3. Create a Desktop app client and download its JSON:\n   https://console.cloud.google.com/auth/clients\nTesting-mode refresh tokens can expire after 7 days. Use Production publishing status for unattended backups; follow any verification requirements Google shows.")
		var err error
		path, err = Prompt(in, out, "Path to downloaded desktop client JSON: ")
		if err != nil {
			return err
		}
	}
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		path = home + path[1:]
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read desktop client JSON: %w", err)
	}
	var doc struct {
		Installed struct {
			ID     string `json:"client_id"`
			Secret string `json:"client_secret"`
		} `json:"installed"`
	}
	if err = json.Unmarshal(b, &doc); err != nil {
		return err
	}
	if doc.Installed.ID == "" {
		return errors.New("credentials must be a Google Desktop app JSON file (installed.client_id)")
	}
	c.ClientID = doc.Installed.ID
	c.ClientSecret = doc.Installed.Secret
	return nil
}

func OpenBrowser(u string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "linux":
		cmd = exec.Command("xdg-open", u)
	default:
		return errors.New("open the URL in your browser")
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

func Login(ctx context.Context, c *config.Config, out io.Writer, noBrowser bool, port int, sshHost string) error {
	listener, err := net.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return fmt.Errorf("open OAuth callback listener: %w", err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cfg := OAuth(c)
	cfg.RedirectURL = "http://" + listener.Addr().String() + "/callback"
	var random [32]byte
	if _, err = rand.Read(random[:]); err != nil {
		return err
	}
	state := base64.RawURLEncoding.EncodeToString(random[:])
	verifier := oauth2.GenerateVerifier()
	type result struct {
		code string
		err  error
	}
	done := make(chan result, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.URL.Query().Get("state")), []byte(state)) != 1 {
			http.Error(w, "Invalid login state. Return to mct-backup and use its login link.", 400)
			return
		}
		res := result{code: r.URL.Query().Get("code")}
		if r.URL.Query().Get("error") != "" {
			res.err = errors.New("Google consent was declined; run mct-backup login to retry")
		}
		if res.code == "" && res.err == nil {
			res.err = errors.New("Google callback did not contain an authorization code")
		}
		select {
		case done <- res:
			fmt.Fprintln(w, "Google response received. Return to mct-backup to finish setup; you can close this tab.")
		default:
			http.Error(w, "Login response already received", 409)
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			select {
			case done <- result{err: err}:
			default:
			}
		}
	}()
	u := cfg.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.SetAuthURLParam("prompt", "consent"), oauth2.S256ChallengeOption(verifier))
	if noBrowser {
		_, p, _ := net.SplitHostPort(listener.Addr().String())
		if sshHost == "" {
			sshHost = "root@nb24cdc.mevnode.com"
		}
		fmt.Fprintf(out, "For login over SSH, run this in another terminal on your computer and leave it open:\n  ssh -N -L %s:127.0.0.1:%s %s\n", p, p, sshHost)
	}
	fmt.Fprintf(out, "Open this URL in your browser and approve Google Drive access:\n%s\nWaiting up to 5 minutes...\n", u)
	if !noBrowser {
		if err := OpenBrowser(u); err != nil {
			fmt.Fprintln(out, "Browser could not be opened automatically; use the URL above.")
		}
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("login did not finish: %w; run mct-backup login again", ctx.Err())
	case r := <-done:
		if r.err != nil {
			return r.err
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: time.Minute})
		token, err := cfg.Exchange(ctx, r.code, oauth2.VerifierOption(verifier))
		if err != nil {
			return fmt.Errorf("exchange Google authorization: %w", err)
		}
		if token.RefreshToken == "" {
			return errors.New("Google did not provide offline access; revoke this app's access and run login again")
		}
		c.Token = token
		return nil
	}
}
