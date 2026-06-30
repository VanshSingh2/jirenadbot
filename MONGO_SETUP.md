# Self-Hosted MongoDB Setup

This bot ships with a self-hosted MongoDB in `docker-compose.yml`, so you don't
need a paid Atlas cluster. Mongo runs in its own container, on a private network
(not exposed to the internet), with auth enabled and data persisted in a volume.

The bot's dataset is small (tens of MB even at thousands of accounts), so Mongo
stays light — **2 GB RAM is plenty for ≤500 accounts; 4 GB for a few thousand.**

---

## 1. Prerequisites

- A Linux box (VPS) with **Docker** and the **Docker Compose plugin** installed.
- This repository cloned onto the box.

Quick Docker install (Ubuntu/Debian):

```bash
curl -fsSL https://get.docker.com | sh
```

---

## 2. Create your `.env`

```bash
cp .env.example .env
```

Then edit `.env` and set at minimum:

```ini
# Telegram (rotate the old leaked tokens in BotFather first!)
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
BOT_TOKEN=...
OWNER_ID=...
LOGGER_BOT_TOKEN=...
NOTIFICATION_BOT_TOKEN=...
NOTIFICATION_CHANNEL_ID=...

# Encryption key for stored sessions — generate ONCE, then NEVER change it
# (changing it makes all saved Telegram sessions undecryptable):
ENCRYPTION_KEY=...

# Self-hosted Mongo credentials (pick strong values)
MONGO_ROOT_USER=jiren
MONGO_ROOT_PASS=change-me-to-a-strong-password
MONGO_CACHE_GB=1.5

# Point the bot at the in-compose Mongo service (host is literally "mongo"):
MONGO_URI=mongodb://jiren:change-me-to-a-strong-password@mongo:27017/gokuads_db?authSource=admin
MONGO_DB_NAME=gokuads_db
```

Generate the encryption key once and paste it into `ENCRYPTION_KEY`:

```bash
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"
```

> The `MONGO_URI` username/password **must match** `MONGO_ROOT_USER` / `MONGO_ROOT_PASS`,
> and must include `?authSource=admin`.

---

## 3. Launch everything

```bash
docker compose up -d --build
```

This starts:
- **mongo** — your database (waits until healthy),
- **jirenadbot** — the manager, which runs the UI bot + auto-scaled forwarding workers.

The bot creates all indexes automatically on first run.

Check status / logs:

```bash
docker compose ps
docker compose logs -f jirenadbot
docker compose logs -f mongo
```

Stop / restart:

```bash
docker compose down          # stop (data is kept in the volume)
docker compose up -d         # start again
docker compose restart jirenadbot
```

---

## 4. Sizing

| Accounts | Mongo RAM (`MONGO_CACHE_GB`) | Mongo vCPU | Disk (SSD) |
|----------|------------------------------|------------|------------|
| ≤ 500            | 1.5–2 GB | 1–2 | 10 GB |
| ~1,000–3,000     | 3–4 GB   | 2   | 20 GB |
| 5,000+           | 6–8 GB   | 2–4 | 40 GB |

`MONGO_CACHE_GB` caps Mongo's WiredTiger cache so it never eats the whole box.
Leave headroom for the bot workers (≈15 MB RAM per active account) on the same host.

---

## 5. Security (important)

- Mongo has **no published ports** in compose, so it's only reachable by the bot
  on the private Docker network — never from the internet. Keep it that way.
- Auth is enabled via `MONGO_ROOT_USER` / `MONGO_ROOT_PASS`. Use a strong password.
- If you ever add `ports:` to expose Mongo, firewall it and never use weak creds —
  unsecured MongoDB instances get wiped by bots within hours.

---

## 6. Backups

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

Automate with a daily cron job.

---

## 7. Switching back to Atlas (optional)

No code change needed — just set in `.env`:

```ini
MONGO_URI=mongodb+srv://USER:PASS@cluster0.xxxx.mongodb.net/gokuads_db?retryWrites=true&w=majority
```

TLS auto-enables for `mongodb+srv://` URIs. You can drop the `mongo` service from
compose if you no longer self-host.

---

## 8. Scaling to multiple machines (later)

For very large fleets, run MongoDB as its own dedicated instance (or a 3-node
replica set) and point every bot/worker box at its **private** address via
`MONGO_URI`. Keep `MONGO_MAX_POOL` modest since total connections ≈
`(workers + 2) × MONGO_MAX_POOL` across all processes.
