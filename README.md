# idm

A lightweight download manager with a web UI. It runs as a single Go binary (no CGO) and typically uses about 20 MB of RAM while downloading.

## Run

```bash
go build -o idm .
./idm init     # asks for the UI password and saves its hash in the database, then exits
./idm          # starts the server
```

Open `http://<host>:8080`. `idm init` reads the password from the terminal without showing it, or one line from stdin when piped. It needs at least 8 characters, and it logs out every open session. Stop the server before running it, because the database is locked while the server runs. If the server starts with no password saved, it generates one and prints it to the log once. You can also change the password in Settings.

| Flag | Default | |
|---|---|---|
| `-addr` | `0.0.0.0:8080` | listen address |
| `-data` | `~/.idm` | where `idm.db` (bbolt) lives (also for `init`) |
| `-dir` | `~/Downloads/idm` | initial download folder (also for `init`; changeable in Settings) |
| `-allow-any-dir` | off | allow folders outside the download folder |
| `-mem-limit` | `48` | soft Go memory limit in MiB |

## Debian / Ubuntu service

`deploy/install.sh` (also copied into `dist/`) creates an `idm` system user and installs the binary. On a fresh install it runs `idm init` to ask for the password, then enables and starts the systemd service. Data goes in `/var/lib/idm` and downloads in `/srv/downloads`. Running it again upgrades in place.

```bash
scp -r dist user@server:/tmp/idm
ssh -t user@server 'cd /tmp/idm && sudo sh install.sh'
```

To change the password later on the server:

```bash
sudo systemctl stop idm && sudo runuser -u idm -- idm init -data /var/lib/idm && sudo systemctl start idm
```

## Features

- **Segmented downloads**: 8 connections per file by default. When a connection finishes, it splits the largest remaining segment, so every connection stays busy until the end.
- **Pause and resume**: unfinished byte ranges are saved every few seconds and on pause or shutdown. A download restarts from zero if the server's file changed (size or ETag differs).
- **Mirrors**: mirrors that report the same size and support ranges are downloaded from in parallel. If a source fails, its segment moves to another source.
- **Queues**: *High* (unlimited, preselected) and *Low* (500 KB/s). The speed limit is shared by everything running in the queue. Each queue runs one download at a time by default. You can reorder items by dragging the grip or with the ▲/▼ buttons. A queue stops by itself when it has nothing left to download.
- **Folders**: files are sorted into category folders by extension (editable in Settings). You can set a folder for each link, or create a new folder for a whole batch.
- **Clipboard**: press Ctrl+V anywhere on the page to add links, or use the paste button. You can paste any text; the links are extracted from it. Write `url | mirror | mirror` on one line to add mirrors.

The page talks to the server with htmx and gets live progress over Server-Sent Events.

Browsers only allow reading the clipboard over HTTPS or on localhost. On a plain-HTTP LAN address the paste button will not work, so use Ctrl+V instead.
