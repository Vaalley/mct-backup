package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
