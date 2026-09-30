package config

import (
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"os"
	"path/filepath"
)

type Config struct {
	Version      int           `json:"version"`
	ClientID     string        `json:"client_id,omitempty"`
	ClientSecret string        `json:"client_secret,omitempty"`
	Token        *oauth2.Token `json:"token,omitempty"`
	FolderID     string        `json:"folder_id,omitempty"`
	LocalRepo    string        `json:"local_repository,omitempty"`
	Key          string        `json:"recovery_key,omitempty"`
	Source       string        `json:"source"`
	Timezone     string        `json:"timezone"`
}

func DefaultDir() (string, error) {
	if d := os.Getenv("MCT_BACKUP_CONFIG_DIR"); d != "" {
		return filepath.Abs(d)
	}
	d, err := os.UserConfigDir()
	return filepath.Join(d, "mct-backup"), err
}

func Load(dir string) (*Config, error) {
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("read configuration: %w; run mct-backup login", err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.Version != 1 {
		return nil, fmt.Errorf("unsupported configuration version %d", c.Version)
	}
	return &c, nil
}

func Save(dir string, c *Config) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return WritePrivate(filepath.Join(dir, "config.json"), append(b, '\n'))
}

func WritePrivate(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
