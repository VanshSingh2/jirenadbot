# Jiren Ads Bot — Full VPS Deployment Guide

End-to-end instructions to run and deploy this bot on a single Linux VPS, with a
**self-hosted MongoDB**. The recommended path uses Docker Compose (one command
brings up Mongo + the bot's auto-scaling manager). A no-Docker (systemd) path is
included as an alternative.

> For Mongo-specific details (sizing, backups, Atlas fallback) see
> [`MONGO_SETUP.md`](./MONGO_SETUP.md). This guide covers the whole stack.

---

## 1. What you're deploying

```
                       ┌─────────────────────────────────────────────┐
                       │                 VPS (1 box)                  │
                       │                                              │
  Telegram users ──▶  │  jirenadbot (manager.py)                     │
                       │   ├── 1 × UI bot   (BOT_ROLE=bot)            │
                       │   └── N × workers  (BOT_ROLE=worker, sharded)│ ──▶ Telegram API
                       │            │                                 │
                       │            ▼                                 │
                       │        MongoDB (self-hosted, private net)    │
                       │                                              │
   (optional) n8n ────│──▶ click-tracker + group-discovery webhooks  │
                       └─────────────────────────────────────────────┘
```

- **`manager.py`** is the entrypoint in Docker. It keeps **1 UI bot + N forwarding
  workers** alive and **auto-scales N** to the active-account load (health-adaptive).
- **MongoDB** runs as its own container on a private network (never exposed publicly).
- **n8n** workflows (click tracking + group discovery) are optional side-cars.

### Run modes (`BOT_ROLE`)
| Mode | What runs | When to use |
|------|-----------|-------------|
| `all` | UI bot **and** forwarding in one process | Smallest setups / quick start |
| `bot` | Only the Telegram UI bots (no forwarding) | Managed by `manager.py` |
| `worker` | Only forwards its account shard | Managed by `manager.py` |

> Using `manager.py` (the default Docker CMD) is recommended — it runs `bot` +
> `worker` processes for you and scales them. You rarely set `BOT_ROLE` by hand.

---

## 2. VPS sizing

RAM budget ≈ **~15 MB per connected account** + **Mongo cache** + ~1 GB OS/overhead.

| Scale (accounts) | vCPU | RAM | Disk (SSD) | `MONGO_CACHE_GB` |
|------------------|------|-----|------------|------------------|
| Start / ≤ 100    | 2    | 4 GB  | 20 GB | 1.5 |
| ≤ 500            | 4    | 8 GB  | 30 GB | 2   |
| ~1,000–3,000     | 4–8  | 16 GB | 40 GB | 3–4 |
| 5,000+           | 8+   | 32 GB | 60 GB+ | 6–8 (consider a dedicated Mongo box) |

> Telegram limits ~**40 logged-in accounts per IP** safely (`PER_IP_CAP=40`). Past
> that you must add proxies (see §9). The bot DMs the admin when you get close.

---

## 3. Prerequisites on the VPS

A Linux box (Ubuntu/Debian recommended) with Docker + the Compose plugin:

```bash
curl -fsSL https://get.docker.com | sh
docker compose version    # confirm the Compose plugin is present
```

You also need, from Telegram:
- **API ID + API hash** — from <https://my.telegram.org> → API development tools.
- **Bot tokens** — create these in [@BotFather](https://t.me/BotFather):
  - main **BOT_TOKEN** (the dashboard/UI bot),
  - **LOGGER_BOT_TOKEN** (sends forwarding logs),
  - **NOTIFICATION_BOT_TOKEN** (admin alerts).
- Your numeric **OWNER_ID** (from [@userinfobot](https://t.me/userinfobot)).
- A **notification channel ID** (add the notification bot as admin; the ID looks like `-100…`).

---

## 4. Get the code

```bash
git clone https://github.com/VanshSingh2/jirenadbot.git
cd jirenadbot
```

---

## 5. Configure `.env`

```bash
cp .env.example .env
```

Generate the **session encryption key once** (keep it forever — changing it makes
all saved Telegram sessions undecryptable):

```bash
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"
# or, without local Python:
docker run --rm python:3.11-slim sh -c "pip -q install cryptography && python -c 'from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())'"
```

Edit `.env` and set at minimum:

```ini
# --- Telegram ---
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
BOT_TOKEN=xxxxxxxxx:yyyyyyyyyyyyyyyyyyyyyyyyy
OWNER_ID=123456789
LOGGER_BOT_TOKEN=xxxxxxxxx:yyyy
NOTIFICATION_BOT_TOKEN=xxxxxxxxx:yyyy
NOTIFICATION_CHANNEL_ID=-1001234567890

# --- Session encryption (generated above; never change) ---
ENCRYPTION_KEY=paste-the-generated-fernet-key

# --- Self-hosted Mongo (pick strong values) ---
MONGO_ROOT_USER=jiren
MONGO_ROOT_PASS=use-a-long-strong-password
MONGO_CACHE_GB=1.5

# Point the bot at the in-compose Mongo service. host MUST be "mongo",
# password MUST equal MONGO_ROOT_PASS, and ?authSource=admin is required:
MONGO_URI=mongodb://jiren:use-a-long-strong-password@mongo:27017/gokuads_db?authSource=admin
MONGO_DB_NAME=gokuads_db
```

> All other settings have safe defaults (see `.env.example` for the full,
> documented list: caches, scaling caps, anti-ban, frequency policy, etc.).

---

## 6. Deploy with Docker Compose (recommended)

```bash
docker compose up -d --build
```

This starts:
- **mongo** — your database (the bot waits until it's healthy),
- **jirenadbot** — `manager.py`, which runs the UI bot + auto-scaled workers.

Indexes are created automatically on first run.

Check it's alive:

```bash
docker compose ps
docker compose logs -f jirenadbot     # follow the bot/manager logs
docker compose logs -f mongo          # follow Mongo logs
```

You should see the manager start a `bot` process and one or more `worker`
processes. Open your bot in Telegram and send `/start` — the dashboard should load.

Common lifecycle commands:

```bash
docker compose restart jirenadbot     # restart just the bot
docker compose down                   # stop everything (Mongo data is preserved)
docker compose up -d                  # start again
```

---

## 7. Update / redeploy

```bash
cd jirenadbot
git pull
docker compose up -d --build          # rebuild + restart with new code
```

Telegram sessions persist in the `./session` volume and Mongo data in the
`mongo_data` volume, so updates don't log accounts out or lose data.

---

## 8. Alternative: run without Docker (systemd)

If you prefer bare-metal:

```bash
# 1) Install MongoDB (see MONGO_SETUP.md) OR use Atlas, then set MONGO_URI in .env.
# 2) Python deps:
sudo apt-get update && sudo apt-get install -y python3-venv gcc
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
# 3) Load env + run the manager:
set -a; . ./.env; set +a
python manager.py        # or: python bot.py   (single all-in-one process)
```

Run it as a service so it restarts on reboot/crash — create
`/etc/systemd/system/jirenadbot.service`:

```ini
[Unit]
Description=Jiren Ads Bot (manager)
After=network-online.target mongod.service
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/jirenadbot
EnvironmentFile=/opt/jirenadbot/.env
ExecStart=/opt/jirenadbot/.venv/bin/python /opt/jirenadbot/manager.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now jirenadbot
sudo journalctl -u jirenadbot -f      # logs
```

---

## 9. Scaling: workers, IP/proxy capacity

The manager auto-scales workers; you mostly tune **caps** and **proxies** in `.env`.

Key knobs (defaults shown):
```ini
MAX_ACCOUNTS_PER_WORKER=100   # soft cap per worker (auto-lowered on small boxes)
PER_IP_CAP=40                 # safe logged-in accounts per IP/proxy (Telegram limit)
MIN_WORKERS=1
MAX_WORKERS=16
MANAGER_INTERVAL=60           # seconds between scaling checks
```

**Proxies** — start on your single VPS IP. When active accounts approach
`PER_IP_CAP × (number of IPs)`, the bot DMs the admin to add more. Add them in
`.env` (one per line or `;`-separated), then `docker compose up -d`:

```ini
# type is socks5 | socks4 | http
PROXY_LIST=socks5:1.2.3.4:1080:user:pass;socks5:5.6.7.8:1080
```

Proxies are assigned **stickily** per account (an account always reuses the same one).

> The manager also reacts to runtime **health**: under connection-timeout stress it
> shrinks accounts-per-worker (spawning more workers); on persistent FloodWait it
> **alerts** you (the fix there is more proxies / lower frequency, not more workers).

---

## 10. Optional: n8n side-car workflows

Two optional automations live in `n8n/`. Import them into any n8n instance and run
it alongside the bot (same Mongo). Each needs a **MongoDB credential** pointing at
the **same database** as the bot.

| Workflow file | Purpose | Bot setting |
|---------------|---------|-------------|
| `n8n/click-tracker.workflow.json` | Redirects + counts clicks for tracked links | set `N8N_TRACK_BASE` in `.env` to the redirect webhook base |
| `n8n/group-discovery.workflow.json` | Scrapes search engines + Telegram directory sites for `t.me` group links → writes to `discovered_groups` | none required; **"🔎 Find Groups"** merges results in automatically |

**Group discovery usage:** POST `{ "niche": "crypto", "user_id": 123 }` to the
workflow's `/discover` webhook (manually, on a schedule, or wired from the bot).
Discovered public groups then appear in the in-app finder and "Join All Found".

> Compliance note: the discovery workflow scrapes third-party pages — prefer an
> official search API (SerpAPI / Bing / Google CSE) where possible, and respect
> each site's ToS and rate limits. The bot verifies every result is a real public
> group before showing it; private invite links are skipped.

---

## 11. Backups

Back up the `mongo_data` volume regularly. One-off dump:

```bash
docker compose exec mongo sh -c \
  'mongodump --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
   --authenticationDatabase admin --archive --gzip' > backup-$(date +%F).gz
```

Restore:

```bash
cat backup-YYYY-MM-DD.gz | docker compose exec -T mongo sh -c \
  'mongorestore --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
   --authenticationDatabase admin --archive --gzip --drop'
```

Also back up your `.env` (it holds the irreplaceable `ENCRYPTION_KEY`) somewhere safe.

---

## 12. Security checklist

- [ ] **Rotate the tokens that were committed in `config.py`** (BOT_TOKEN, logger,
      notification, API hash, the old Atlas URI). Set fresh values only in `.env`.
- [ ] `ENCRYPTION_KEY` set, generated once, backed up, **never changed**.
- [ ] Strong `MONGO_ROOT_PASS`; Mongo has **no published `ports:`** (private network only).
- [ ] `.env` is git-ignored (it is) and never committed.
- [ ] Firewall the VPS: allow only SSH (and n8n's port if you expose it). Mongo
      stays internal.
- [ ] If you ever expose Mongo with `ports:`, firewall it and use strong creds —
      open MongoDB instances get wiped within hours.

---

## 13. Troubleshooting

| Symptom | Likely cause / fix |
|---------|--------------------|
| Bot container restarts / exits | `docker compose logs jirenadbot`. Usually a missing/invalid env var (tokens, `MONGO_URI`). |
| `ServerSelectionTimeoutError` | `MONGO_URI` wrong: host must be `mongo`, password must match `MONGO_ROOT_PASS`, and include `?authSource=admin`. |
| Sessions log out after redeploy | The `./session` volume must persist (it's mounted in compose) and `ENCRYPTION_KEY` must be unchanged. |
| FloodWait / timeouts rising as accounts grow | You're near per-IP capacity — add proxies (`PROXY_LIST`). The bot also DMs the admin. |
| UI feels slow at high load | Raise `DB_THREAD_POOL`; ensure Mongo has enough `MONGO_CACHE_GB`; confirm workers scaled up in logs. |
| "Add more proxies" admin DM | Expected at scale — add IPs to `PROXY_LIST` and redeploy. |
| Mongo using too much RAM | Lower `MONGO_CACHE_GB`. |

Useful checks:

```bash
docker compose ps
docker compose logs -f jirenadbot
docker stats                      # live CPU/RAM per container
```

---

## 14. Quick start (TL;DR)

```bash
git clone https://github.com/VanshSingh2/jirenadbot.git && cd jirenadbot
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"   # -> ENCRYPTION_KEY
nano .env            # fill Telegram tokens, OWNER_ID, MONGO_* (and the key above)
docker compose up -d --build
docker compose logs -f jirenadbot
# open your bot in Telegram -> /start
```
