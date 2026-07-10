# Jiren Ads Bot — Setup & Run Guide (VPS + GitHub Codespace)

This is the single, practical guide to get the bot running:

- **GitHub Codespace** → for quick testing / trying the bot (not for 24/7 production).
- **VPS (Docker Compose)** → the real production home for 100–500+ accounts.

> Deeper references already in this repo:
> [`DEPLOYMENT.md`](./DEPLOYMENT.md) (full VPS deep-dive) and
> [`MONGO_SETUP.md`](./MONGO_SETUP.md) (self-hosted database).
> This file is the fast path and the Codespace path.

---

## 0. TL;DR

```bash
# --- VPS (production) ---
git clone https://github.com/VanshSingh2/jirenadbot.git && cd jirenadbot
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"   # -> ENCRYPTION_KEY
nano .env            # fill Telegram tokens, OWNER_ID, MONGO_*, ENCRYPTION_KEY, PROXY_LIST
docker compose up -d --build
docker compose logs -f jirenadbot
# open your bot in Telegram -> /start

# --- Codespace (testing only) ---
# 1) Create a Codespace on this repo
# 2) cp .env.example .env  and fill values (use a free MongoDB Atlas M0 URI)
# 3) pip install -r requirements.txt
# 4) python bot.py
```

---

## 1. What you are running

The bot has 3 process roles (set by `BOT_ROLE`). You rarely set them by hand —
`manager.py` runs and scales them for you.

| Entry point | What it does | Use when |
|-------------|--------------|----------|
| `python bot.py` | Single all-in-one process (UI **and** forwarding). `BOT_ROLE=all` | Codespace / small tests |
| `python manager.py` | Runs **1 UI bot + N auto-scaled forwarding workers**, respawns crashes, alerts on proxy limits | **Production** (this is the Docker default) |
| `docker compose up -d` | Runs `manager.py` + a private self-hosted MongoDB | **Recommended production** |

Forwarding is driven by an `is_forwarding` flag in MongoDB. Workers shard
accounts by `hash(account_id) % WORKER_COUNT`, so adding workers splits the load
automatically with no extra coordination.

---

## 2. Get your Telegram credentials (needed for BOTH paths)

From [my.telegram.org](https://my.telegram.org) → **API development tools**:
- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`

From [@BotFather](https://t.me/BotFather) — create **three** bots:
- `BOT_TOKEN` — the main dashboard/UI bot
- `LOGGER_BOT_TOKEN` — sends per-round forwarding logs to users
- `NOTIFICATION_BOT_TOKEN` — admin alerts (new users, payments, scaling warnings)

Other IDs:
- `OWNER_ID` — your numeric Telegram id (from [@userinfobot](https://t.me/userinfobot))
- `NOTIFICATION_CHANNEL_ID` — a channel (add the notification bot as admin); id looks like `-100…`

Generate the **session encryption key ONCE** and keep it forever (changing it
makes all saved Telegram logins undecryptable):

```bash
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"
```

> ⚠️ **Security:** the `config.py` defaults currently contain committed tokens and a
> Mongo URI. **Rotate all of them** (BotFather → revoke/regenerate, Atlas → new
> user/password) and set the fresh values only in `.env`. Never commit `.env`.

---

## 3. Path A — GitHub Codespace (testing / trial)

Codespaces are great for trying the bot, editing code, and a short live test.
They **stop on inactivity** and have monthly usage limits, so **do not** rely on
them for 24/7 production.

### 3.1 Create the Codespace
- On the GitHub repo page: **Code → Codespaces → Create codespace on main**.
- Wait for the container to build; you get a VS Code terminal in the browser.

### 3.2 Database for Codespace
Use a free **MongoDB Atlas M0** cluster (simplest for an ephemeral box):
1. Create a free cluster at [mongodb.com/atlas](https://www.mongodb.com/atlas).
2. Add a database user + password.
3. Network Access → allow `0.0.0.0/0` (test only).
4. Copy the `mongodb+srv://…` connection string.

(TLS auto-enables for `mongodb+srv://` URIs — no extra flags needed.)

### 3.3 Configure and run
```bash
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"   # -> ENCRYPTION_KEY
nano .env
```
Set at minimum in `.env`:
```ini
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
BOT_TOKEN=...
OWNER_ID=...
LOGGER_BOT_TOKEN=...
NOTIFICATION_BOT_TOKEN=...
NOTIFICATION_CHANNEL_ID=...
ENCRYPTION_KEY=...            # the key generated above
MONGO_URI=mongodb+srv://USER:PASS@cluster0.xxxx.mongodb.net/gokuads_db?retryWrites=true&w=majority
MONGO_DB_NAME=gokuads_db
BOT_ROLE=all                  # single process is fine for testing
```
Install deps and start:
```bash
pip install -r requirements.txt
python bot.py
```
You should see `Main: @yourbot`, `Bot running!`. Open the bot in Telegram and
send `/start`.

> Tip: keep the terminal open. When the Codespace sleeps, the bot stops — that's
> expected. For anything long-running, move to the VPS path below.

---

## 4. Path B — VPS with Docker Compose (production)

This runs the auto-scaling manager **and** a private, self-hosted MongoDB in one
command. Mongo is **not** exposed to the internet (no published ports).

### 4.1 Prepare the VPS
Ubuntu/Debian recommended:
```bash
curl -fsSL https://get.docker.com | sh
docker compose version    # confirm the Compose plugin exists
```

### 4.2 Get the code + config
```bash
git clone https://github.com/VanshSingh2/jirenadbot.git
cd jirenadbot
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"   # -> ENCRYPTION_KEY
nano .env
```
Fill the Telegram values + encryption key (as in §2), and configure the
**self-hosted** Mongo (host must literally be `mongo`, and include `?authSource=admin`):
```ini
# Self-hosted Mongo credentials
MONGO_ROOT_USER=jiren
MONGO_ROOT_PASS=use-a-long-strong-password
MONGO_CACHE_GB=1.5

# Point the bot at the in-compose Mongo service:
MONGO_URI=mongodb://jiren:use-a-long-strong-password@mongo:27017/gokuads_db?authSource=admin
MONGO_DB_NAME=gokuads_db
```

### 4.3 Launch
```bash
docker compose up -d --build
docker compose ps
docker compose logs -f jirenadbot     # follow bot/manager logs
```
The manager will start a `bot` process + one or more `worker` processes. Indexes
are created automatically on first run. Open the bot in Telegram → `/start`.

### 4.4 Everyday commands
```bash
docker compose restart jirenadbot     # restart the bot only
docker compose down                   # stop all (Mongo data is preserved in a volume)
docker compose up -d                  # start again
# update to latest code:
git pull && docker compose up -d --build
```
Telegram sessions persist in `./session`; Mongo data in the `mongo_data` volume.
Updates do not log accounts out or lose data (as long as `ENCRYPTION_KEY` is unchanged).

---

## 4B. Path C — Native on the VPS: local MongoDB + systemd (24/7, auto-restart)

Use this if you want the bot and MongoDB running **directly on the VPS** (no
Docker), started on boot and kept alive by **systemd**. MongoDB runs **locally on
the box** (bound to `127.0.0.1` — never exposed to the internet).

### 4B.1 Install MongoDB locally (systemd-managed)
Ubuntu (auto-detects your release codename):
```bash
sudo apt-get update && sudo apt-get install -y gnupg curl lsb-release
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
  sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/8.0 multiverse" | \
  sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org
sudo systemctl enable --now mongod        # start now + auto-start on every boot
systemctl status mongod --no-pager        # should be: active (running)
```
MongoDB listens only on `127.0.0.1:27017` by default — keep it that way (no
internet exposure, so localhost needs no password).

(Optional) cap its RAM — edit `/etc/mongod.conf`:
```yaml
storage:
  wiredTiger:
    engineConfig:
      cacheSizeGB: 1.5
```
then `sudo systemctl restart mongod`.

### 4B.2 Install the bot in a venv
```bash
sudo apt-get install -y python3-venv git
cd /opt && sudo git clone https://github.com/VanshSingh2/jirenadbot.git
cd jirenadbot
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"   # -> ENCRYPTION_KEY
nano .env
```

### 4B.3 `.env` for local MongoDB
Point the bot at the **local** Mongo and fill your Telegram values + the key.
(Replace every `<...>` with your **rotated** value — see §2.)
```ini
# --- local MongoDB on this VPS (no auth needed; localhost-only) ---
MONGO_URI=mongodb://localhost:27017/gokuads_db
MONGO_DB_NAME=gokuads_db

# --- Telegram ---
TELEGRAM_API_ID=<your api id>
TELEGRAM_API_HASH=<your rotated api hash>
BOT_TOKEN=<your rotated main bot token>
LOGGER_BOT_TOKEN=<your rotated logger bot token>
NOTIFICATION_BOT_TOKEN=<your rotated notification bot token>
NOTIFICATION_CHANNEL_ID=<your channel id, e.g. -100...>
OWNER_ID=<your numeric telegram id>
ENCRYPTION_KEY=<the key you generated above — keep it forever>

# --- add when you have 100+ accounts ---
PROXY_LIST=
```
> The bot loads `.env` automatically (python-dotenv), so you do **not** need
> systemd `EnvironmentFile`. Keep secrets in `.env` only (never `config.py`).
> `.env` is git-ignored — it stays on this VPS.

### 4B.4 Create the systemd service (24/7 + auto-restart)
```bash
sudo tee /etc/systemd/system/jirenadbot.service >/dev/null <<'UNIT'
[Unit]
Description=Jiren Ads Bot (auto-scaling manager)
After=network-online.target mongod.service
Wants=network-online.target
Requires=mongod.service

[Service]
Type=simple
WorkingDirectory=/opt/jirenadbot
ExecStart=/opt/jirenadbot/.venv/bin/python manager.py
Restart=always
RestartSec=5
TimeoutStopSec=45
User=root
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

sudo systemctl daemon-reload
sudo systemctl enable --now jirenadbot     # start now + on every boot
```
`Restart=always` brings the bot back if it ever crashes; `Requires/After=mongod`
makes it start after MongoDB. Both `mongod` and `jirenadbot` are enabled, so the
whole stack comes back automatically after a reboot — true 24/7.

> `manager.py` gives auto-scaling workers + `/health` + `/manager`. To run the
> simple single process instead, set `ExecStart=/opt/jirenadbot/.venv/bin/python bot.py`.

### 4B.5 Operate it
```bash
systemctl status jirenadbot --no-pager     # health
journalctl -u jirenadbot -f                # live logs (Ctrl+C to stop watching)
sudo systemctl restart jirenadbot          # restart
sudo systemctl stop jirenadbot             # stop
# update to latest code:
cd /opt/jirenadbot && sudo git pull && .venv/bin/pip install -r requirements.txt && sudo systemctl restart jirenadbot
```
Backup the local DB:
```bash
mongodump --db gokuads_db --archive=/root/backup-$(date +%F).gz --gzip
```
> Firewall: keep only SSH open; MongoDB stays on localhost. Sessions persist in
> `/opt/jirenadbot/session/`; never change `ENCRYPTION_KEY`.

---

## 5. Sizing the VPS (this is the part that decides "no lag")

RAM budget ≈ **~15 MB per connected account** + Mongo cache + ~1 GB OS overhead.

| Accounts | vCPU | RAM | Disk (SSD) | `MONGO_CACHE_GB` |
|----------|------|-----|------------|------------------|
| ≤ 100    | 2    | 4 GB  | 20 GB | 1.5 |
| ≤ 500    | 4    | 8 GB  | 30 GB | 2   |
| 1k–3k    | 4–8  | 16 GB | 40 GB | 3–4 |
| 5k+      | 8+   | 32 GB | 60 GB+ | 6–8 (consider a dedicated Mongo box) |

Each forwarding process ≈ one CPU core (Python GIL). The manager runs about one
worker per core and spreads accounts across them, so more cores = more parallel
throughput = lower latency.

---

## 6. Proxies / IPs — the #1 thing for 100–500+ accounts

Telegram safely allows about **40 logged-in accounts per IP** (`PER_IP_CAP=40`).
Beyond that, accounts get FloodWait'd and banned no matter how good the code is.

| Accounts | Minimum IPs/proxies needed |
|----------|----------------------------|
| ~40      | 1 (your VPS IP is enough)  |
| 100      | ~3                         |
| 300      | ~8                         |
| 500      | ~13                        |

Add proxies in `.env` (one per line or `;`-separated). They are assigned
**stickily** per account (an account always egresses from the same IP — Telegram
dislikes IP hopping):
```ini
# type is socks5 | socks4 | http
#   type:host:port            (no auth)
#   type:host:port:user:pass  (with auth)
PROXY_LIST=socks5:1.2.3.4:1080:user:pass;socks5:5.6.7.8:1080
```
Then `docker compose up -d`. The bot **DMs the admin** automatically when active
accounts approach your IP capacity, telling you how many proxies to add.

> Use clean residential/mobile SOCKS5 proxies for account safety. Cheap
> datacenter IPs get flagged faster.

---

## 7. Scaling & anti-ban knobs (already tuned; override in `.env` if needed)

```ini
# Scaling
MAX_ACCOUNTS_PER_WORKER=100   # soft cap per worker (auto-lowered on small boxes)
PER_IP_CAP=40                 # safe logged-in accounts per IP/proxy
MIN_WORKERS=1
MAX_WORKERS=16
MANAGER_INTERVAL=60           # seconds between scaling checks
DB_THREAD_POOL=64             # parallel blocking-DB ops (raise if UI feels slow)

# Send-frequency policy (per group). Admin-managed, hard-capped.
DEFAULT_TARGET_PER_HOUR=3
HARD_MAX_TARGET_PER_HOUR=3

# Anti-ban
WARMUP_ENABLED=1              # new accounts ramp up gradually (cap 2 days)
ROTATION_CHUNK=25             # groups per round when Smart Rotation is ON
HEALTH_PEERFLOOD_LIMIT=3      # auto-pause an account after N PeerFlood rounds in a row
TOXIC_PRUNE_DEFAULT=1         # auto-drop admin-only / ban-on-post / heavy-slowmode groups
MIN_MSG_DELAY=5               # hard floor between sends (anti-burst)
TG_TIMEOUT=30                 # Telethon connect/request timeout (default 10 is too low at scale)
```
Runtime resilience already built in: forever-retry reconnect + `auto_reconnect`,
FloodWait/SlowMode/PeerFlood handling, a supervisor that restarts crashed account
loops with backoff, and a startup reconciler that resumes forwarding after a restart.

---

## 8. Verify it's healthy

In Telegram, as admin: send **`/health`** to the main bot for a live snapshot
(active accounts here vs in DB, CPU, RAM, proxies, uptime).

On the box:
```bash
docker compose logs -f jirenadbot     # look for [RECONCILE], [HEALTH], [FORWARDING] lines
docker stats                          # live CPU/RAM per container
```
Expected log lines: the manager spawning `bot` + `worker N/COUNT`, and
`[HEALTH] ... local_active_accounts=…` every few minutes.

---

## 9. Backups (do this)

```bash
# One-off dump
docker compose exec mongo sh -c \
  'mongodump --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
   --authenticationDatabase admin --archive --gzip' > backup-$(date +%F).gz
```
Also back up your `.env` (it holds the irreplaceable `ENCRYPTION_KEY`) somewhere safe.

---

## 10. Troubleshooting

| Symptom | Fix |
|---------|-----|
| Container keeps restarting | `docker compose logs jirenadbot` — usually a missing/invalid env var (tokens, `MONGO_URI`). |
| `ServerSelectionTimeoutError` | `MONGO_URI` host must be `mongo`, password must match `MONGO_ROOT_PASS`, include `?authSource=admin`. |
| Accounts log out after redeploy | Keep the `./session` volume and never change `ENCRYPTION_KEY`. |
| FloodWait / bans rising with more accounts | You're over per-IP capacity — add proxies (`PROXY_LIST`). |
| UI feels slow under load | Raise `DB_THREAD_POOL`; ensure enough `MONGO_CACHE_GB`; confirm workers scaled up in logs. |
| "Add more proxies" admin DM | Expected at scale — add IPs and `docker compose up -d`. |
| Codespace bot stopped | Codespaces sleep on inactivity — normal. Use the VPS path for 24/7. |

---

## 11. Security checklist before going live

- [ ] Rotate every token/URI that was committed in `config.py` (BOT_TOKEN, logger,
      notification, `TELEGRAM_API_HASH`, old Atlas URI). Set fresh values only in `.env`.
- [ ] `ENCRYPTION_KEY` generated once, backed up, **never changed**.
- [ ] Strong `MONGO_ROOT_PASS`; Mongo has **no published `ports:`** (private network).
- [ ] `.env` stays git-ignored (it is) and is never committed.
- [ ] VPS firewall allows only SSH (and n8n's port if you use it); Mongo stays internal.
- [ ] Clean residential/mobile proxies configured for 100+ accounts.

---

## 12. Scaling to 1000–2000 accounts (single box vs. multi-VPS)

You have **two levers**, and the manager will DM you which one to pull:

- **Vertical** — give the current VPS more RAM / vCPU.
- **Horizontal** — add another VPS node. Accounts are sharded across machines by a
  stable hash of the account id, so each box independently runs ~`1/NODE_COUNT`
  of the fleet with **no cross-node coordination**.

### 12.1 Roughly where each option lands

| Accounts | Simplest path | Notes |
|----------|---------------|-------|
| ≤ 500    | 1 VPS (4 vCPU / 8 GB) | Vertical only. |
| ~1000    | 1 big VPS (8 vCPU / 16 GB) **or** 2 nodes | Either works; 2 nodes = less blast radius. |
| ~2000    | **2–4 VPS nodes** (each 4 vCPU / 8 GB) | Horizontal is cheaper & more resilient than one huge box. |

RAM is the real cap (~15 MB/account). One 16 GB box tops out near ~1000 active
accounts; beyond that, adding nodes is the clean path.

### 12.2 How multi-VPS works here

- Set the **same `NODE_COUNT`** on every machine and a **unique `NODE_ID`** (0,1,2,…).
- **Node 0 runs the Telegram UI bot** (`RUN_UI=1`); every other node is **workers-only**
  (`RUN_UI=0`) — a bot token can only long-poll from one process.
- **All nodes share ONE MongoDB** and **must use the exact same `ENCRYPTION_KEY`**
  (so any node can decrypt any account's session).
- **Each node needs its own proxies** (`PROXY_LIST`) — proxies are not shared across boxes.
- The manager on each node counts only the accounts it owns and scales its own workers.

### 12.3 Add-a-VPS runbook (going from 1 node to 2)

**On node 0** (existing box) — make its Mongo reachable by the new node over a
**private network / VPN (WireGuard/Tailscale)**, never the public internet. Then:
```ini
# node 0 .env
NODE_COUNT=2
NODE_ID=0
RUN_UI=1
```
```bash
docker compose up -d          # picks up NODE_COUNT=2; it now owns ~half the fleet
```

**On node 1** (new box):
```bash
git clone https://github.com/VanshSingh2/jirenadbot.git && cd jirenadbot
cp .env.example .env
nano .env
```
```ini
# node 1 .env
NODE_COUNT=2
NODE_ID=1
RUN_UI=0
ENCRYPTION_KEY=<EXACT same key as node 0>
MONGO_URI=mongodb://USER:PASS@<node0-private-ip>:27017/gokuads_db?authSource=admin
MONGO_DB_NAME=gokuads_db
PROXY_LIST=<node 1's own proxies>
# (all the Telegram tokens/OWNER_ID too — same values as node 0)
```
```bash
docker compose -f docker-compose.worker.yml up -d --build
```
That's it. Accounts rebalance automatically (~half move to node 1). To add a 3rd
node later: bump `NODE_COUNT=3` everywhere, boot the new box with `NODE_ID=2`.

> **Shared Mongo options:** (a) node 0's self-hosted Mongo exposed only on the
> private VPN, or (b) a managed cluster (Atlas M10+). For 1000–2000 accounts a
> dedicated/managed DB is the safer choice.

### 12.4 The manager tells you when to scale

Every cycle the manager checks this node's headroom and DMs the admin (with a
per-topic cooldown so it never spams):

- **"RAM filling up"** → increase this VPS's RAM to ~N GB, or add a VPS.
- **"Worker/CPU ceiling"** → add vCPU + raise `MAX_WORKERS`, or add a VPS.
- **"High CPU"** → sustained load per core is high → add cores, or add a VPS.
- **"Node near capacity"** (95%+) → clear call to **add a VPS** (`NODE_COUNT=N+1`).
- **"Proxy capacity"** → add ~K proxies to `PROXY_LIST` (this already existed).

So you don't have to watch dashboards — act on the DM it sends.

---

## 13. "How many workers will I actually get?" (vCPU ≠ workers)

Worker count is driven by **accounts**, not cores:

```
workers_on_this_node = ceil(node_active_accounts / MAX_ACCOUNTS_PER_WORKER)   # capped by MAX_WORKERS
```

- **RAM** decides the *total* accounts a box can hold (~15 MB each).
- **vCPU** decides how many worker processes run comfortably in parallel; the
  design targets ~1 process per core, but forwarding is **I/O-bound** (mostly
  waiting on Telegram), so one core easily handles several worker processes.
- There is always **1 extra UI-bot process** on node 0 that does not forward.

Examples (default `MAX_ACCOUNTS_PER_WORKER=100`):

| Box / node | Active accounts on node | Forwarding workers | + UI bot |
|------------|-------------------------|--------------------|----------|
| 2 vCPU / 4 GB | ~100 | 1 | +1 (node 0) |
| 4 vCPU / 8 GB | ~500 | 5 | +1 (node 0) |
| 8 vCPU / 16 GB | ~1000 | 10 | +1 (node 0) |
| 2× (4 vCPU / 8 GB) | ~2000 (≈1000/node) | 10 per node | +1 on node 0 only |

So at 500 accounts you already have *more* worker processes (5) than the "1 per
vCPU" intuition suggests — that's expected and fine. Tune with
`MAX_ACCOUNTS_PER_WORKER` (smaller = more, lighter workers) and `MAX_WORKERS`
(hard ceiling, default 16).
