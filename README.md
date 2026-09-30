# mct-backup

An incremental Go backup CLI for a live Minecraft Java server, with Google Drive
storage, individual-file historical restores, and calendar-based retention.

```sh
make build
./mct-backup login
./mct-backup export-key --out ./mct-backup-recovery-key.txt
./mct-backup backup
```

The default source is `/srv/mctraveler/server`. Files are read directly while the
server runs. No staging copy, Minecraft commands, or shutdown is performed.
The scanner prepares data while up to four packs upload concurrently. The
encrypted upload spool is bounded to approximately five 128 MiB packs (640 MiB)
plus metadata during normal operation. Interrupted jobs are retained for resumption.

## Installation

Requires Go 1.25 or later. macOS and Linux are supported.

```sh
make build                          # ./mct-backup
make install                        # ~/.local/bin/mct-backup
make linux                          # dist/mct-backup-linux-amd64 and -arm64
```

Ensure `~/.local/bin` is on your PATH after installing. With a binary on PATH,
every example can use `mct-backup` instead of `./mct-backup`.

## Google Drive login

`mct-backup login` guides setup, opens the system browser, requests access only to
files created/opened with this application (`drive.file`), and creates or reconnects
to the `mct-backup` Drive folder. Access tokens refresh automatically for scheduled
backups. Authentication uses a loopback callback with PKCE and state validation.

Google requires a registered OAuth **Desktop app** client. This source build does
not contain an OAuth identity. On first login the wizard explains how to enable
the Drive API, configure the consent screen, and download the client JSON, then
asks for its path. You can also provide it directly:

```sh
mct-backup login --credentials ~/Downloads/client_secret.json
```

The publisher can configure `MCT_BACKUP_CLIENT_ID` and
`MCT_BACKUP_CLIENT_SECRET`, or embed its own desktop client at build time with Go
linker variables `mct-backup/internal/auth.DefaultClientID` and
`mct-backup/internal/auth.DefaultClientSecret`. Then the user's first command goes
straight to browser consent. Desktop client secrets are not a substitute for PKCE
or user consent. No other application's OAuth client is borrowed.

For an external app in Google's **Testing** state, refresh tokens can expire
after seven days. For unattended operation, configure Production publishing
status and complete any requirements Google presents. The same OAuth client
must be used when recovering the repository with `drive.file` access.

### Login on the SSH server

```sh
mct-backup login --no-browser --ssh-host root@nb24cdc.mevnode.com
```

The command prints an SSH tunnel command and a Google URL. Run the tunnel in a
second terminal on your computer, keep it open, then open the URL in your local
browser. The callback is forwarded to the loopback listener on the server. Login
times out after five minutes. This does not use Google's deprecated manual-code
flow. SSH sessions are detected automatically.

`login --source DIR --timezone Europe/London` configures defaults. An existing
app-accessible repository can be selected with `--folder-id ID`. Recovery on a new
machine requires `--key-file FILE` and the original OAuth client.

### Recovery key and local state

```sh
mct-backup export-key --out /safe/location/mct-backup-recovery-key.txt
mct-backup status
```

Keep a copy of the recovery key **outside the Minecraft server**. Without it,
Drive data cannot be decrypted. Export refuses to overwrite an existing file.

Configuration contains the OAuth refresh token and recovery key; it is stored
with mode `0600`, in a `0700` directory. Do not commit or share it. The default is
the platform's user configuration directory (`~/.config/mct-backup` on Linux and
`~/Library/Application Support/mct-backup` on macOS). Override with
`MCT_BACKUP_CONFIG_DIR` or `--config-dir DIR` **before** the subcommand.

The chunk index is a bbolt database, not a required part of recovery. Use
`mct-backup rebuild-index` to reconstruct it from remote pack indexes. The
configuration directory must remain outside the source directory.

## Backup

```sh
mct-backup backup
mct-backup backup --source /srv/mctraveler/server
mct-backup backup --exclude backup-udfix-20260904-1401
mct-backup backup --rehash
mct-backup backup --upload-concurrency 4
```

All regular files, directory modes/timestamps, and symlink targets are captured.
Symlinks are not followed. Ownership, ACLs and extended attributes are not stored.
The default exclusions are directories named `logs` or `crash-reports`, and files
named `session.lock`. Other server/mod data is included. Repeated `--exclude`
values match a relative path or a Go `path.Match` pattern; a matched directory
excludes its whole subtree. `**` has no special recursive meaning.

Unchanged files reuse previous references, based on size, mode, nanosecond mtime,
inode/device, and ctime where available. A full content read is automatic at
least every seven days and can be forced with `--rehash`. An unchanged daily run
uploads a new manifest but zero new data chunks.

`--upload-concurrency` accepts 1–16 (default 4). It controls independent pack
uploads; each pack's resumable chunks remain sequential. Reading/chunking overlaps
uploads, while backpressure bounds spool usage to `(concurrency + 1) × 128 MiB`
plus metadata. The snapshot manifest waits until all packs and indexes are uploaded
and verified. Failures cancel other uploads and preserve their journals for the
next run. Only the scanner updates the deduplication cache. Increasing concurrency
can help with network latency but does not bypass Drive quotas or guarantee a
proportional speedup. A running process keeps its original concurrency settings.

Region files (`.mca`) use Rabin content-defined chunking with 32–256 KiB bounds
and 16 average bits (approximately 64 KiB after the minimum boundary). Other
files use 64 KiB–1 MiB bounds and 18 average bits. Small player files usually fit
in one chunk. Hashing raw bytes preserves exact region contents and allows chunk
boundaries to recover after internal data shifts. These defaults need measurement
against the actual server's workload before claiming an optimal ratio.

Chunks are deduplicated by SHA-256, compressed only when smaller, then encrypted
individually using AES-256-GCM with random nonces. Approximately 128 MiB of new
encrypted records form a pack. File names, manifests, and indexes are encrypted;
object sizes, counts, and creation times remain visible to Drive.

### Live-file semantics

This is a best-effort live backup. Files in one snapshot can represent different
moments. A file's metadata is checked before and after capture and at its path;
a detected change triggers one retry. If the second capture also changes, its
captured bytes are kept and marked unstable. A read is limited to its initial
size to avoid following a growing log indefinitely. Unreadable/disappearing files
are recorded as missing coverage. Walk races can still miss newly added files.

Snapshots with warnings are committed and return exit code **2**. Operational
failures return **1** without publishing an incomplete manifest; success returns
**0**. Stable-looking metadata cannot prove application consistency. `verify`
proves the restored bytes match the capture; it cannot prove a world will load.

## Restore and verify

```sh
mct-backup snapshots
mct-backup snapshots --json
mct-backup ls --snapshot latest --path world/playerdata
mct-backup restore --at 2026-09-10T03:00:00Z \
  --path world/playerdata/UUID.dat --to ./restore
mct-backup restore --snapshot SNAPSHOT_ID --to ./restored-server
mct-backup verify --snapshot latest
```

`--at` selects the latest retained snapshot **completed at or before** the supplied
RFC3339 timestamp. The CLI prints its capture window. There is no arbitrary-time
history between snapshots. A file absent in that snapshot is reported as absent;
an older version is not silently substituted.

The destination must be empty. Files are recreated with their original relative
paths, so the first example writes `./restore/world/playerdata/UUID.dat`. File
bytes are checked against their full SHA-256 before publication. Existing files
are never overwritten. Restores use `os.Root` to contain filesystem operations
and reject absolute/escaping symlink targets. Restoration is separate from the
live source; deploying a restored world is a separate operation.

Single-file restores use Drive byte ranges. Full restores and verification cache
up to eight encrypted packs (approximately 1 GiB) on local disk to reduce API
requests. Completed commands remove that read cache; SIGKILL can leave it behind.
Ensure sufficient space for the restored files as well as this cache.

## Retention and purging

```sh
mct-backup purge --dry-run
mct-backup purge --yes
```

The default timezone is `Europe/London`. Keep the union of the latest successful
snapshot in each of these calendar buckets, including the current partial bucket:

| Tier | Retention |
|---|---|
| Daily | Latest 7 calendar days |
| Weekly | Latest 4 ISO weeks, starting Monday |
| Monthly | Latest 12 calendar months |
| Yearly | Every calendar year, indefinitely |

Snapshots can satisfy multiple tiers. Complete coverage is preferred over missing
coverage within a bucket. The newest snapshot and newest complete snapshot are
always protected. Snapshots/packs younger than 24 hours receive a grace period.
No snapshot is invented for a missed bucket; `snapshots` displays actual coverage.

Normal backup never deletes, overwrites, or trashes committed Drive objects.
Purge is explicit and permanent. Its default is a JSON preview. `--yes` is the
confirmation to expire unselected snapshots, remove unreachable data, and compact
mixed packs. Compaction writes and verifies new objects before deleting old
indexes and packs, and requires temporary extra Drive space. The reclaimable-byte
estimate excludes some index/compression overhead.

Pending upload journals block purge until `backup` resumes them. A process lock
serializes commands using the same configuration directory. **Use one designated
host and configuration directory per repository.** There is no distributed lock;
running writers or purge concurrently from multiple hosts is unsupported. Drive
does not enforce append-only permissions against another credential holder.

## Scheduling on this server

Example systemd units are in `deploy/`. They assume the binary is installed at
`/usr/local/bin/mct-backup` and configuration lives at `/root/.config/mct-backup`.
These units are deployed on nb24cdc.mevnode.com. The build itself does not install or enable them.

Perform login using that same config directory before enabling the timer:

```sh
sudo /usr/local/bin/mct-backup --config-dir /root/.config/mct-backup login --no-browser
sudo /usr/local/bin/mct-backup --config-dir /root/.config/mct-backup export-key \
  --out /root/mct-backup-recovery-key.txt
# Copy the recovery key off the server, then run the first backup manually.
sudo /usr/local/bin/mct-backup --config-dir /root/.config/mct-backup backup
```

Backups run daily at **03:00 Europe/London** and catch missed runs after reboot.
Purge has its own timer at **05:00 Europe/London**, does not catch missed runs, and
has a two-hour limit. It skips when another command holds the repository lock.
The backup service stops any running scheduled purge before starting, so backups
have priority. An interrupted purge preserves committed data and any upload
journals; the next backup resumes journaled uploads before scanning.

Exit 2 (a backup committed with capture warnings) is accepted; warnings remain
in the journal. Purge uses the same retention policy independently of that day's
backup result, always protecting the newest and newest complete snapshots.

```sh
systemctl list-timers 'mct-backup*'
journalctl -u mct-backup.service -n 50 --no-pager
journalctl -u mct-backup-purge.service -n 50 --no-pager
```

Purge uses an exact in-memory set of 32-byte chunk hashes and read-only database
lookups, instead of inserting millions of random keys into one write transaction.
Memory scales with unique retained chunks, plus one loaded snapshot. Immutable
snapshot/index ciphertext is cached privately in `metadata-cache/`; each cache
hit is authenticated and checked against the current remote object listing.
The cache is disposable and can be removed while no command is running. The old
local `live` database bucket is ignored by version 0.1.2; rebuilding the chunk
index is optional if you want to reclaim the old database's disk space.


## Offline testing

```sh
mct-backup --config-dir /tmp/mct-config init \
  --local /tmp/mct-repository --source /path/to/test-server
mct-backup --config-dir /tmp/mct-config backup
mct-backup --config-dir /tmp/mct-config verify
make test
```

Tests cover local end-to-end backup/restore, changed-region deduplication,
point-in-time selection, concurrent mutation, interrupted pack and manifest
uploads, cache reconstruction, wrong keys, corruption, retention, compaction,
OAuth state/PKCE, and a simulated Drive API including resumed uploads/range reads.

The 500GB live dataset and real Google authorization/upload have not been tested
by this build. Initial backup reads all selected bytes. Metadata manifests are
complete and loaded in memory, so memory use scales with total file/chunk count;
this format is not a constant-memory streaming tree. Measure peak RAM, duration,
Drive request count, daily changed bytes, and an isolated full-world restore
before treating this as the server's sole backup. Yearly retention grows without
a fixed storage bound. Check the Drive account's total storage and upload quota.

## References

- [Google desktop OAuth / PKCE](https://developers.google.com/identity/protocols/oauth2/native-app)
- [Google OAuth refresh-token expiration](https://developers.google.com/identity/protocols/oauth2#expiration)
- [Drive resumable uploads and pre-generated IDs](https://developers.google.com/workspace/drive/api/guides/manage-uploads)
- [Drive partial downloads](https://developers.google.com/workspace/drive/api/guides/manage-downloads)
- [Drive usage limits](https://developers.google.com/workspace/drive/api/guides/limits)
# mct-backup
