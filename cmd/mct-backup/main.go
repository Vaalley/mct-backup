package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata"

	"mct-backup/internal/auth"
	"mct-backup/internal/config"
	"mct-backup/internal/repository"
	"mct-backup/internal/storage"
)

var version = "dev"

const help = `mct-backup — incremental Minecraft backups to Google Drive

Usage: mct-backup [--config-dir DIR] COMMAND [options]

  login          Google Drive sign-in via Duplicati's OAuth service
  backup         Capture live files; upload only new encrypted chunks
  snapshots      List committed restore points
  ls             List files in a snapshot
  restore        Restore a file, directory, or full snapshot to an empty directory
  verify         Download and verify a snapshot's file bytes
  purge          Preview retention and garbage collection; --yes applies it
  status         Show repository configuration without secrets
  export-key     Save the recovery key to a private file
  rebuild-index  Rebuild the local cache from immutable remote indexes
  init           Initialize a local repository for offline use/testing
  version        Print version

Start: mct-backup login
       mct-backup backup
       mct-backup restore --path world/playerdata/UUID.dat --to ./restore

Run a command with --help for its options.
`

type warningError struct{ message string }

func (e *warningError) Error() string { return e.message }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "mct-backup:", err)
		var w *warningError
		if errors.As(err, &w) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func flags(name string, out io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(out)
	return f
}
func parse(f *flag.FlagSet, args []string) error {
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected argument %q", f.Arg(0))
	}
	return nil
}

type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }

func run(ctx context.Context, args []string, in io.Reader, out, log io.Writer) error {
	dir, err := config.DefaultDir()
	if err != nil {
		return err
	}
	global := flags("mct-backup", out)
	global.StringVar(&dir, "config-dir", dir, "configuration, upload spool, and cache directory")
	global.Usage = func() { fmt.Fprint(out, help) }
	if err = global.Parse(args); err != nil {
		return err
	}
	args = global.Args()
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprint(out, help)
		return nil
	}
	command := args[0]
	args = args[1:]
	if command == "version" {
		fmt.Fprintln(out, "mct-backup", version)
		return nil
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return err
	}
	// Help must work before login and without taking a repository lock.
	if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
		return commandHelp(command, out)
	}
	unlock, err := config.Lock(dir)
	if err != nil {
		return err
	}
	defer unlock()
	if command == "login" {
		return login(ctx, dir, args, bufio.NewReader(in), out, log)
	}
	if command == "init" {
		return initLocal(ctx, dir, args, out, log)
	}
	c, err := config.Load(dir)
	if err != nil {
		return err
	}
	if command == "status" {
		if len(args) != 0 {
			return errors.New("status takes no arguments")
		}
		fmt.Fprintf(out, "Configuration: %s\nSource: %s\nTimezone: %s\n", dir, c.Source, c.Timezone)
		if c.LocalRepo != "" {
			fmt.Fprintln(out, "Local repository:", c.LocalRepo)
		} else {
			fmt.Fprintf(out, "Google Drive: https://drive.google.com/drive/folders/%s\n", c.FolderID)
			fmt.Fprintf(out, "Login: Duplicati OAuth service (%s)\n", auth.RefreshURL(c))
		}
		return nil
	}
	if command == "export-key" {
		f := flags(command, out)
		dest := f.String("out", "", "required recovery key file (keep outside the server)")
		if err = parse(f, args); err != nil {
			return err
		}
		if *dest == "" {
			return errors.New("use export-key --out /path/to/recovery-key.txt")
		}
		file, err := os.OpenFile(*dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(file, c.Key)
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			fmt.Fprintln(out, "Recovery key saved to", *dest)
		}
		return err
	}
	if command == "rebuild-index" {
		if len(args) != 0 {
			return errors.New("rebuild-index takes no arguments")
		}
		if err = os.Remove(filepath.Join(dir, "index.db")); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	store, err := storeFor(ctx, c, dir)
	if err != nil {
		return err
	}
	r, err := repository.Open(ctx, store, dir, c.Key, false, log)
	if err != nil {
		return err
	}
	defer r.Close()
	switch command {
	case "rebuild-index":
		fmt.Fprintln(out, "Local chunk index rebuilt from repository data.")
		return nil
	case "backup":
		f := flags(command, out)
		source := f.String("source", c.Source, "live server directory")
		rehash := f.Bool("rehash", false, "read all file contents (also automatic at least weekly)")
		uploads := f.Int("upload-concurrency", 4, "parallel pack uploads (1–16); scanner prepares one additional pack")
		asJSON := f.Bool("json", false, "print snapshot summary as JSON")
		var excludes stringsFlag
		f.Var(&excludes, "exclude", "exclude relative path/pattern; repeatable; directory names exclude their subtree")
		if err = parse(f, args); err != nil {
			return err
		}
		if c.LocalRepo != "" {
			if within(*source, c.LocalRepo) {
				return errors.New("local repository must be outside the source directory")
			}
		}
		if *uploads < 1 || *uploads > 16 {
			return errors.New("--upload-concurrency must be between 1 and 16")
		}
		s, err := r.Backup(ctx, repository.BackupOptions{Source: *source, Excludes: append([]string{"logs", "crash-reports", "session.lock"}, excludes...), Rehash: *rehash, UploadConcurrency: *uploads})
		if err != nil {
			return err
		}
		if *asJSON {
			if err = json.NewEncoder(out).Encode(summary(s)); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(out, "Committed %s\n%d entries; %d reused files; %d bytes read; %d new pack/index bytes uploaded\n", s.ID, len(s.Files), s.ReusedFiles, s.ReadBytes, s.UploadedBytes)
			showWarnings(out, s)
		}
		if len(s.Warnings) > 0 {
			return &warningError{"snapshot committed with capture warnings (exit 2); inspect snapshots/ls output"}
		}
		return nil
	case "snapshots":
		f := flags(command, out)
		asJSON := f.Bool("json", false, "JSON summaries")
		if err = parse(f, args); err != nil {
			return err
		}
		rows := []map[string]any{}
		for _, id := range r.Snapshots() {
			s, err := r.LoadSnapshot(ctx, id)
			if err != nil {
				return err
			}
			if *asJSON {
				rows = append(rows, summary(s))
			} else {
				fmt.Fprintf(out, "%s  files=%d  warnings=%d  coverage=%t\n", s.ID, len(s.Files), len(s.Warnings), s.CoverageComplete)
			}
		}
		if *asJSON {
			return json.NewEncoder(out).Encode(rows)
		}
		return nil
	case "restore", "verify", "ls":
		f := flags(command, out)
		id := f.String("snapshot", "latest", "snapshot ID or latest")
		at := f.String("at", "", "select newest snapshot completed by this RFC3339 time")
		selected := f.String("path", "", "relative file/directory; empty selects all")
		dest := ""
		asJSON := false
		if command == "restore" {
			f.StringVar(&dest, "to", "", "required empty destination directory")
		}
		if command == "ls" {
			f.BoolVar(&asJSON, "json", false, "JSON file entries")
		}
		if err = parse(f, args); err != nil {
			return err
		}
		var s *repository.Snapshot
		if *at != "" {
			if *id != "latest" {
				return errors.New("choose --at or --snapshot, not both")
			}
			t, e := time.Parse(time.RFC3339, *at)
			if e != nil {
				return fmt.Errorf("--at requires RFC3339, e.g. 2026-09-10T03:00:00Z: %w", e)
			}
			s, err = r.SnapshotAt(ctx, t)
		} else {
			s, err = r.LoadSnapshot(ctx, *id)
		}
		if err != nil {
			return err
		}
		if command == "ls" {
			var files []repository.File
			for _, entry := range s.Files {
				if *selected == "" || entry.Path == *selected || strings.HasPrefix(entry.Path, *selected+"/") {
					if asJSON {
						files = append(files, entry)
					} else {
						fmt.Fprintf(out, "%s\t%d\t%s\n", entry.Kind, entry.Size, entry.Path)
					}
				}
			}
			if asJSON {
				return json.NewEncoder(out).Encode(files)
			}
			return nil
		}
		fmt.Fprintf(out, "Snapshot %s\nCapture window: %s → %s\n", s.ID, s.Started.Format(time.RFC3339), s.Completed.Format(time.RFC3339))
		showWarnings(out, s)
		if command == "verify" {
			if *selected != "" {
				return errors.New("verify checks a whole snapshot; use restore --path for a file")
			}
			if err = r.Verify(ctx, s); err != nil {
				return err
			}
			fmt.Fprintln(out, "Verified: every file matches its captured bytes.")
			return nil
		}
		if dest == "" {
			return errors.New("restore requires --to with an empty destination directory")
		}
		if within(s.Source, dest) || within(dest, s.Source) {
			return errors.New("restore destination must be separate from the live server directory")
		}
		if err = r.Restore(ctx, s, *selected, dest); err != nil {
			return err
		}
		fmt.Fprintln(out, "Restored and verified into", dest)
		return nil
	case "purge":
		f := flags(command, out)
		yes := f.Bool("yes", false, "confirm permanent deletion of expired snapshots/unreferenced data")
		dry := f.Bool("dry-run", false, "preview only (the default)")
		zone := f.String("timezone", c.Timezone, "calendar timezone for retention")
		if err = parse(f, args); err != nil {
			return err
		}
		if *yes && *dry {
			return errors.New("--yes and --dry-run cannot be combined")
		}
		loc, err := time.LoadLocation(*zone)
		if err != nil {
			return err
		}
		p, err := r.PlanPurge(ctx, time.Now(), loc)
		if err != nil {
			return err
		}
		if err = json.NewEncoder(out).Encode(p); err != nil {
			return err
		}
		if !*yes {
			fmt.Fprintln(out, "Preview only. Run purge --yes to apply this retention policy and permanently delete expired data.")
			return nil
		}
		if err = r.ApplyPurge(ctx, p); err != nil {
			return err
		}
		fmt.Fprintln(out, "Purge complete; retained snapshots remain restorable.")
		return nil
	default:
		return fmt.Errorf("unknown command %q; run mct-backup --help", command)
	}
}

func within(parent, child string) bool {
	a, e := filepath.Abs(parent)
	if e != nil {
		return false
	}
	b, e := filepath.Abs(child)
	if e != nil {
		return false
	}
	if real, e := filepath.EvalSymlinks(a); e == nil {
		a = real
	}
	if real, e := filepath.EvalSymlinks(b); e == nil {
		b = real
	}
	rel, e := filepath.Rel(a, b)
	return e == nil && (rel == "." || filepath.IsLocal(rel))
}
func summary(s *repository.Snapshot) map[string]any {
	return map[string]any{"id": s.ID, "started": s.Started, "completed": s.Completed, "entries": len(s.Files), "coverage_complete": s.CoverageComplete, "warnings": s.Warnings, "read_bytes": s.ReadBytes, "uploaded_bytes": s.UploadedBytes, "reused_files": s.ReusedFiles, "rehashed": s.Rehashed}
}
func showWarnings(out io.Writer, s *repository.Snapshot) {
	for i, w := range s.Warnings {
		if i == 5 {
			fmt.Fprintf(out, "... %d more warnings; use snapshots --json for all warnings\n", len(s.Warnings)-5)
			break
		}
		fmt.Fprintln(out, "Warning:", w)
	}
}

func storeFor(ctx context.Context, c *config.Config, dir string) (storage.Store, error) {
	if c.LocalRepo != "" {
		return &storage.Local{Root: c.LocalRepo}, nil
	}
	if c.FolderID == "" {
		return nil, errors.New("Drive repository is not configured; run login")
	}
	client, err := auth.Client(ctx, c, dir)
	if err != nil {
		return nil, err
	}
	return storage.NewDrive(client, c.FolderID, dir), nil
}

func login(ctx context.Context, dir string, args []string, in *bufio.Reader, out, log io.Writer) error {
	f := flags("login", out)
	authID := f.String("auth-id", "", "AuthID to use without prompting")
	oauthURL := f.String("oauth-url", "", "Duplicati-compatible OAuth refresh URL")
	noBrowser := f.Bool("no-browser", os.Getenv("SSH_CONNECTION") != "", "print the login URL instead of opening a browser")
	folder := f.String("folder", "mct-backup", "repository folder name")
	folderID := f.String("folder-id", "", "existing folder ID accessible to this Google account")
	keyFile := f.String("key-file", "", "recovery key file for an existing repository")
	source := f.String("source", "", "default live server directory")
	zone := f.String("timezone", "", "calendar timezone (default Europe/London)")
	if err := parse(f, args); err != nil {
		return err
	}
	c, err := config.Load(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		c = &config.Config{Version: 1, Source: "/srv/mctraveler/server", Timezone: "Europe/London"}
	}
	if c.LocalRepo != "" {
		return errors.New("this configuration uses a local repository; choose another --config-dir for Drive")
	}
	if *source != "" {
		c.Source = *source
	}
	if *zone != "" {
		c.Timezone = *zone
	}
	if *oauthURL != "" {
		c.OAuthURL = *oauthURL
	}
	if _, err = time.LoadLocation(c.Timezone); err != nil {
		return err
	}
	if err = resetLegacyDrive(dir, c, out); err != nil {
		return err
	}
	if err = auth.Login(c, in, out, *noBrowser, *authID); err != nil {
		return err
	}
	// Save offline credentials before remote setup, so interrupted setup is recoverable.
	if err = config.Save(dir, c); err != nil {
		return err
	}
	client, err := auth.Client(ctx, c, dir)
	if err != nil {
		return err
	}
	drive := storage.NewDrive(client, "", dir)
	if *folderID != "" && c.FolderID != "" && *folderID != c.FolderID {
		return errors.New("use a separate --config-dir when switching repositories")
	}
	if *folderID != "" {
		c.FolderID = *folderID
	}
	if c.FolderID == "" {
		c.FolderID, err = drive.FindFolder(ctx, *folder)
		if err != nil {
			return err
		}
	}
	existing := c.FolderID != ""
	if *keyFile != "" {
		b, err := os.ReadFile(*keyFile)
		if err != nil {
			return err
		}
		imported := strings.TrimSpace(string(b))
		if c.Key != "" && c.Key != imported {
			return errors.New("supplied recovery key differs from this configuration; use a separate --config-dir for another repository")
		}
		c.Key = imported
	}
	if c.Key == "" {
		if existing {
			p, err := auth.Prompt(in, out, "Existing repository found. Path to its recovery key file: ")
			if err != nil {
				return err
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			c.Key = strings.TrimSpace(string(b))
		} else {
			c.Key, err = repository.NewKey()
			if err != nil {
				return err
			}
		}
	}
	if err = config.Save(dir, c); err != nil {
		return err
	}
	if !existing {
		c.FolderID, err = drive.CreateFolder(ctx, *folder)
		if err != nil {
			return err
		}
		if err = config.Save(dir, c); err != nil {
			return err
		}
	}
	drive.Folder = c.FolderID
	r, err := repository.Open(ctx, drive, dir, c.Key, true, log)
	if err != nil {
		return err
	}
	if err = r.Close(); err != nil {
		return err
	}
	fmt.Fprintf(out, "Login complete.\nDrive folder: https://drive.google.com/drive/folders/%s\nSource: %s\nNext: mct-backup export-key --out ./mct-backup-recovery-key.txt\nThen: mct-backup backup\n", c.FolderID, c.Source)
	return nil
}

func resetLegacyDrive(dir string, c *config.Config, out io.Writer) error {
	if c.LocalRepo != "" || c.FolderID == "" || c.AuthID != "" {
		return nil
	}
	for _, name := range []string{"index.db", "spool", "uploads", "metadata-cache"} {
		if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	c.FolderID = ""
	fmt.Fprintln(out, `The old Drive folder is left untouched but isn't visible to Duplicati's sign-in; a new "mct-backup" folder will be created.`)
	return nil
}

func initLocal(ctx context.Context, dir string, args []string, out, log io.Writer) error {
	f := flags("init", out)
	dest := f.String("local", "", "required local repository directory")
	source := f.String("source", "/srv/mctraveler/server", "default server directory")
	keyFile := f.String("key-file", "", "existing repository's recovery key file")
	if err := parse(f, args); err != nil {
		return err
	}
	if *dest == "" {
		return errors.New("init is for offline use: init --local /path/to/repository; for Google Drive run login")
	}
	if _, err := os.Stat(filepath.Join(dir, "config.json")); err == nil {
		return errors.New("configuration already exists; use another --config-dir")
	} else if !os.IsNotExist(err) {
		return err
	}
	root, err := filepath.Abs(*dest)
	if err != nil {
		return err
	}
	if within(*source, root) {
		return errors.New("local repository must be outside source")
	}
	if within(root, dir) || within(dir, root) {
		return errors.New("local repository and config directory must be separate")
	}
	key := ""
	if *keyFile != "" {
		b, e := os.ReadFile(*keyFile)
		if e != nil {
			return e
		}
		key = strings.TrimSpace(string(b))
	} else {
		key, err = repository.NewKey()
		if err != nil {
			return err
		}
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return err
	}
	c := &config.Config{Version: 1, LocalRepo: root, Key: key, Source: *source, Timezone: "Europe/London"}
	if err = config.Save(dir, c); err != nil {
		return err
	}
	r, err := repository.Open(ctx, &storage.Local{Root: root}, dir, key, true, log)
	if err != nil {
		return err
	}
	if err = r.Close(); err != nil {
		return err
	}
	fmt.Fprintln(out, "Local repository ready:", root)
	return nil
}

func commandHelp(command string, out io.Writer) error {
	// Use the real parsers for commands that can run before repository loading.
	switch command {
	case "login":
		return login(context.Background(), "", []string{"--help"}, nil, out, out)
	case "init":
		return initLocal(context.Background(), "", []string{"--help"}, out, out)
	}
	help := map[string]string{
		"backup":        "backup [--source DIR] [--rehash] [--exclude PATTERN ...] [--upload-concurrency 4] [--json]\nDefaults: 4 parallel uploads; source from login; excludes logs, crash-reports, session.lock.\n",
		"snapshots":     "snapshots [--json]\nLists committed snapshots and capture warnings.\n",
		"ls":            "ls [--snapshot ID|latest | --at RFC3339] [--path RELATIVE_PATH] [--json]\n",
		"restore":       "restore [--snapshot ID|latest | --at RFC3339] [--path RELATIVE_PATH] --to EMPTY_DIR\nPaths are recreated relative to the original server root. Existing files are never overwritten.\n",
		"verify":        "verify [--snapshot ID|latest | --at RFC3339]\nVerifies captured bytes; does not validate Minecraft application consistency.\n",
		"purge":         "purge [--dry-run | --yes] [--timezone Europe/London]\nDefault is preview. --yes permanently deletes expired data. Retains 7 daily, 4 weekly, 12 monthly calendar buckets and one snapshot per year indefinitely, plus newest and 24-hour grace.\n",
		"status":        "status\nShow configuration without secrets.\n",
		"export-key":    "export-key --out NEW_FILE\nWrites a private recovery key file; refuses to overwrite. Store it outside the server.\n",
		"rebuild-index": "rebuild-index\nRebuild the local disk index from the remote repository.\n",
	}
	if h, ok := help[command]; ok {
		fmt.Fprint(out, "Usage: mct-backup ", h)
		return nil
	}
	return fmt.Errorf("unknown command %q", command)
}
