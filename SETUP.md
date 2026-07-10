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
