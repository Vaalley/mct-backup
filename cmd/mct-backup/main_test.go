package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mct-backup/internal/config"
)

func TestResetLegacyDrive(t *testing.T) {
	dir := t.TempDir()
	for name, contents := range map[string]string{
		"index.db":         "index",
		"spool/x":          "spooled",
		"uploads/y":        "upload state",
		"metadata-cache/z": "cached metadata",
	} {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	c := &config.Config{
		FolderID: "old-folder",
		Key:      "recovery-key",
		Source:   "/srv/server",
		Timezone: "Europe/London",
	}
	var out bytes.Buffer
	if err := resetLegacyDrive(dir, c, &out); err != nil {
		t.Fatal(err)
	}
	if c.FolderID != "" || c.Key != "recovery-key" || c.Source != "/srv/server" || c.Timezone != "Europe/London" {
		t.Fatalf("unexpected migrated config: %+v", c)
	}
	if out.Len() == 0 {
		t.Fatal("missing migration notice")
	}
	for _, name := range []string{"index.db", "spool", "uploads", "metadata-cache"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s remains: %v", name, err)
		}
	}

	if err := os.MkdirAll(filepath.Join(dir, "spool"), 0700); err != nil {
		t.Fatal(err)
	}
	authenticated := &config.Config{FolderID: "folder", AuthID: "keyid:password"}
	out.Reset()
	if err := resetLegacyDrive(dir, authenticated, &out); err != nil {
		t.Fatal(err)
	}
	if authenticated.FolderID != "folder" || out.Len() != 0 {
		t.Fatalf("authenticated config was changed: %+v", authenticated)
	}
	if _, err := os.Stat(filepath.Join(dir, "spool")); err != nil {
		t.Fatalf("authenticated local state was removed: %v", err)
	}
}

func TestOfflineCLIWorkflow(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "config")
	source := filepath.Join(base, "server")
	remote := filepath.Join(base, "repo")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "player.dat"), []byte("player inventory"), 0600); err != nil {
		t.Fatal(err)
	}
	call := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(context.Background(), append([]string{"--config-dir", dir}, args...), strings.NewReader(""), &out, io.Discard); err != nil {
			t.Fatal(args, err)
		}
		return out.String()
	}
	call("init", "--local", remote, "--source", source)
	call("backup")
	got := call("backup", "--json")
	if !strings.Contains(got, `"read_bytes":0`) {
		t.Fatal(got)
	}
	call("snapshots")
	call("verify")
	call("rebuild-index")
	call("restore", "--path", "player.dat", "--to", filepath.Join(base, "restore"))
	call("purge", "--dry-run")
	call("export-key", "--out", filepath.Join(base, "key"))
	info, err := os.Stat(filepath.Join(base, "key"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("recovery key permissions", err)
	}
}

func TestHelpNeedsNoCredentials(t *testing.T) {
	for _, command := range []string{"login", "backup", "restore", "purge", "verify", "init"} {
		var out bytes.Buffer
		err := run(context.Background(), []string{"--config-dir", filepath.Join(t.TempDir(), "missing"), command, "--help"}, nil, &out, io.Discard)
		if err != nil && !strings.Contains(err.Error(), "help requested") {
			t.Fatal(command, err)
		}
		if out.Len() == 0 {
			t.Fatal("missing help", command)
		}
	}
}
