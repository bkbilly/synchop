# SyncHop ⚡

A high-efficiency, scheduled 1:1 mirroring engine with an embedded modern Web UI written in Go. Designed specifically to tolerate intermittent connectivity, enforce strict timeouts, support stateful delta resumption, and operate with zero idle background footprint.

---

## Why SyncHop?

1. **Dedicated to ONE Primary Sync**: No confusing job dropdowns, no IDs, no clutter. Everything in the app controls your one primary scheduled sync.
2. **Strict Two-Phase Pipeline (Gather First, Then Send)**:
   - **Phase 1 (Gather)**: Consolidates files in parallel from remote SSH servers, SMB/NAS shares, WebDAV clouds, or local directories into a local staging cache.
   - **Phase 2 (Send)**: Only after gathering completes, pushes the verified snapshot to your intermittent remote destination.
3. **Multi-Page Layout**:
   - **📊 Dashboard**: High-level real-time status, 1-click **Start / Stop Sync** button, visual Two-Phase Flow diagram, progress bar, transfer throughput, and last-synced timestamp.
   - **📥 Sources (Gather)**: Configure remote servers or folders to gather files from (Server, Port, User, Password / Key, and Path). Includes an instant **Test Connection** button.
   - **📤 Destination (Send)**: Configure the intermittent remote PC where the gathered snapshot is sent (Server, Port, User, Password / Key, Path, and pre-flight connectivity check).
   - **⚙ Schedule & Settings**: Choose how often to sync with 1 click (**Manual Only**, **Every 15 min**, **Hourly**, **Every 6 Hours**, **Daily**), transfer concurrency, and local cache folder.
   - **📜 History & Audit Trail**: Chronological log of all sync runs with execution timestamps, trigger source (Manual, Scheduled, CLI), duration, status, files changed count, payload send size, total snapshot size, delta bandwidth savings, and an itemized breakdown of every file created, modified, or deleted.
4. **Full Multi-Protocol Support (SSH, SMB/NAS, WebDAV, Local)**:
   - **SSH Remote Server**: Block-level delta synchronization over SSH.
   - **SMB / NAS Share**: Pure Go SMB2/SMB3 integration without `sudo` or OS kernel mounts.
   - **WebDAV (HTTPS / Nextcloud)**: Seamless sync over standard HTTPS (Port 443), bypassing firewall/carrier port blocks. Supports Nextcloud, ownCloud, Synology, QNAP, and Hetzner Storage Boxes with byte-range resume.
   - Each endpoint has an instant **Test Connection** button that tests network reachability, authentication, and directory permissions before saving.
   - **Interactive Directory Browser (SSH, SMB, WebDAV, Local)**: Click **Browse** next to any path input to explore the remote or local filesystem in real-time. Automatically utilizes entered credentials on-the-fly to authenticate and navigate folders with 1-click selection.
5. **Flexible File Filtering (Include or Exclude)**:
   - Each source offers an intuitive choice of how to filter files:
     - **Sync All Files (No Filter)**: Transfers all files inside the source folder.
     - **Only Include Matching Files**: Gathers only the file types you care about (e.g. `*.txt, *.pdf, *.docx`). Subdirectories are automatically traversed to find matching files.
     - **Exclude Matching Files**: Syncs everything except the specified patterns (e.g. `*.tmp, cache/, node_modules/, *.log`).
   - Patterns can be entered comma-separated (e.g. `*.txt, *.pdf`) or one per line.
   - Works consistently across **SSH/rsync**, **SMB/NAS**, and **WebDAV**.
6. **Stateful Resumption & Delta Efficiency**:
   - Uses `--partial-dir=.synchop-partial` with rolling block checksums and byte ranges. Interrupted transfers do not restart from scratch; they resume at the broken byte on subsequent runs.
7. **Process Group Isolation (Zero Rogue Background Procs)**:
   - Child processes run in their own Linux Process Group (`Setpgid`). If a connection drops or times out, `SIGKILL` cleanly purges the entire process tree.
8. **No Staging Subfolder Needed**:
   - SyncHop automatically isolates and organizes files by the source's name behind the scenes.
9. **No Dummy / Demo Data**:
   - All configurations start completely clean so you don't have to clear out placeholder fields.
10. **Clear Required Indicators**:
    - Every field clearly indicates whether it is required (`*`) or optional (`(Optional)`).
11. **Fully Responsive Mobile Interface**:
    - Seamless layout across mobile phones (down to 320px screens), tablets, and desktops with touch-friendly navigation, responsive modals, and zero horizontal overflow.
12. **Versioned Backups & Automated Retention**:
    - Optional safety net against accidental deletions or overwrites. When enabled, any file replaced or deleted during sync is preserved into a timestamped directory (e.g. `.synchop-backups/YYYY-MM-DD_HH-MM-SS`) on the destination.
    - Automated pruning automatically removes older backup sets exceeding your chosen retention count (e.g. keep last 5 backups). Works across **SSH/rsync**, **Local**, **SMB/NAS**, and **WebDAV**.

---

## Quick Start

### 1. Build Single Binary
```bash
cd /home/bkbilly/Documents/synchop
go build -o synchop ./cmd/synchop
```

### 2. Run Options

**Option A: Web Interface & Daemon (Default)**
```bash
./synchop -port 8080
```
Open your browser at: **`http://localhost:8080`**

**Option B: Docker Compose (Recommended for Containers & NAS)**
```bash
docker compose up -d
```
All persistent configurations, history logs, and staging caches are stored in `./data`. Open your browser at: **`http://localhost:8080`**.

**Option C: Standalone One-Shot CLI Sync (No Web UI)**
```bash
./synchop sync
# or with a custom config:
./synchop sync -config myconfig.json
```
Executes the sync directly in your terminal and exits with status 0 upon completion (or 1 on error). Ideal for cron jobs, scripts, and terminal shortcuts.

---

## Running as a Systemd Service (Optional)

Create `/etc/systemd/system/synchop.service`:

```ini
[Unit]
Description=SyncHop Two-Phase Mirroring Engine
After=network.target

[Service]
Type=simple
User=bkbilly
WorkingDirectory=/home/bkbilly/Documents/synchop
ExecStart=/home/bkbilly/Documents/synchop/synchop -port 8080
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now synchop
```
