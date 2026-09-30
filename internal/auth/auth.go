package auth

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"mct-backup/internal/config"
)

const DefaultOAuthURL = "https://duplicati-oauth-handler.appspot.com/refresh"

func RefreshURL(c *config.Config) string {
	if c.OAuthURL != "" {
		return c.OAuthURL
	}
	if u := os.Getenv("MCT_BACKUP_OAUTH_URL"); u != "" {
		return u
	}
	return DefaultOAuthURL
}

func LoginURL(refreshURL string) (string, error) {
	u, err := url.Parse(refreshURL)
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" || u.Opaque != "" {
		return "", errors.New("OAuth refresh URL must be an absolute URL")
	}
	u.Path = ""
	u.RawPath = ""
	u.Fragment = ""
	u.RawFragment = ""
	if u.RawQuery == "" {
		u.RawQuery = "type=googledrive"
	} else {
		u.RawQuery += "&type=googledrive"
	}
	return u.String(), nil
}

type authIDSource struct {
	mu    sync.Mutex
	ctx   context.Context
	c     *config.Config
	dir   string
	http  *http.Client
	url   string
	pause func(context.Context, time.Duration) error
}

func (s *authIDSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for retries := 0; ; {
		req, err := http.NewRequestWithContext(s.ctx, http.MethodGet, s.url, nil)
		if err != nil {
			return nil, refreshError("OAuth service request failed")
		}
		req.Header.Set("X-AuthID", s.c.AuthID)
		req.Header.Set("User-Agent", "mct-backup")

		resp, err := s.http.Do(req)
		if err != nil {
			if retries < 5 {
				if err := s.wait(time.Duration(1<<retries) * time.Second); err != nil {
					return nil, refreshError("OAuth service request failed")
				}
				retries++
				continue
			}
			return nil, refreshError("OAuth service request failed")
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			reason := resp.Header.Get("X-Reason")
			_ = resp.Body.Close()
			if reason == "" {
				reason = resp.Status
			}
			maxRetries := 0
			switch {
			case resp.StatusCode >= 400 && resp.StatusCode < 500:
				maxRetries = 1
			case resp.StatusCode >= 500:
				maxRetries = 5
			}
			if retries < maxRetries {
				if err := s.wait(time.Duration(1<<retries) * time.Second); err != nil {
					return nil, refreshError(reason)
				}
				retries++
				continue
			}
			return nil, refreshError(reason)
		}

		var result struct {
			AccessToken string `json:"access_token"`
			Expires     int64  `json:"expires"`
			V2AuthID    string `json:"v2_authid"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
		_ = resp.Body.Close()
		if err != nil {
			return nil, refreshError("invalid response from OAuth service")
		}
		if result.AccessToken == "" {
			return nil, refreshError("OAuth service returned no access token")
		}
		if result.V2AuthID != "" && result.V2AuthID != s.c.AuthID {
			s.c.AuthID = result.V2AuthID
			if err := config.Save(s.dir, s.c); err != nil {
				return nil, err
			}
		}
		return &oauth2.Token{
			AccessToken: result.AccessToken,
			TokenType:   "Bearer",
			Expiry:      time.Now().Add(time.Duration(result.Expires)*time.Second - 30*time.Second),
		}, nil
	}
}

func (s *authIDSource) wait(delay time.Duration) error {
	if s.pause != nil {
		return s.pause(s.ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case <-timer.C:
		return nil
	}
}

func refreshError(reason string) error {
	return fmt.Errorf("%s; run mct-backup login", reason)
}

func Client(ctx context.Context, c *config.Config, dir string) (*http.Client, error) {
	if c.AuthID == "" {
		return nil, errors.New("Google login is missing; run mct-backup login")
	}
	base := &http.Client{Timeout: 2 * time.Minute}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, base)
	src := &authIDSource{
		ctx:  ctx,
		c:    c,
		dir:  dir,
		http: &http.Client{Timeout: 30 * time.Second},
		url:  RefreshURL(c),
	}
	client := oauth2.NewClient(ctx, oauth2.ReuseTokenSource(nil, src))
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

func Login(c *config.Config, in *bufio.Reader, out io.Writer, noBrowser bool, authID string) error {
	if authID == "" {
		loginURL, err := LoginURL(RefreshURL(c))
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Sign in with Google on Duplicati's login page, then copy the AuthID it shows:\n%s\n", loginURL)
		if !noBrowser {
			if err := OpenBrowser(loginURL); err != nil {
				fmt.Fprintln(out, "Browser could not be opened automatically; use the URL above.")
			}
		}
		authID, err = Prompt(in, out, "Paste the AuthID: ")
		if err != nil {
			return err
		}
	}
	authID = strings.TrimSpace(authID)
	if colon := strings.IndexByte(authID, ':'); colon <= 0 {
		return errors.New("AuthID must contain ':' after a non-empty key ID")
	}
	c.AuthID = authID
	return nil
}
