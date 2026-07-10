# Run Jiren Ads Bot on a VPS (with n8n Group Discovery) — Single Runbook

This is the **one document** to stand up the whole system on a fresh VPS:
the bot + auto-scaling manager + self-hosted MongoDB + the n8n group-discovery
workflow + the responsive AI ops-manager (`/manager`) and `/health`.

> Related docs (optional deeper dives): [`SETUP.md`](./SETUP.md) (VPS + Codespace
> quick start, sizing, multi-VPS), [`DEPLOYMENT.md`](./DEPLOYMENT.md),
> [`MONGO_SETUP.md`](./MONGO_SETUP.md). **This file is enough on its own.**

---

## 0. What you'll end up running

On one VPS, via Docker Compose:

| Service | What it is |
|---------|-----------|
| `jirenadbot` | `manager.py` → the Telegram UI bot + auto-scaled forwarding workers |
| `mongo` | Private, self-hosted MongoDB (not exposed to the internet) |
| `n8n` | Automation server hosting the **group-discovery** workflow |

Plus, inside Telegram: `/health` (live cluster status) and `/manager` (chat with
an AI ops-manager that can tune the fleet).

---

## 1. Prerequisites

- A VPS (Ubuntu/Debian). Sizing: **≤100 accounts** 2 vCPU/4 GB; **≤500** 4 vCPU/8 GB;
  **~1000** 8 vCPU/16 GB. (See §9 for 1000–2000 and multi-VPS.)
- Docker + Compose plugin:
  ```bash
  curl -fsSL https://get.docker.com | sh
  docker compose version
  ```
- Telegram API creds from [my.telegram.org](https://my.telegram.org): `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`.
- **Three** bots from [@BotFather](https://t.me/BotFather): main `BOT_TOKEN`, `LOGGER_BOT_TOKEN`, `NOTIFICATION_BOT_TOKEN`.
- Your numeric `OWNER_ID` (from [@userinfobot](https://t.me/userinfobot)) and a notification channel id (`-100…`).
- (For 100+ accounts) clean residential/mobile **SOCKS5 proxies**.
- (Optional) an AI API key (OpenAI / Anthropic / Gemini / Groq) for full `/manager` chat.

---

## 2. Get the code & configure

```bash
git clone https://github.com/VanshSingh2/jirenadbot.git
cd jirenadbot
cp .env.example .env
# Generate the session encryption key ONCE and keep it forever:
python3 -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"
nano .env
```

Fill `.env`. Minimum to boot:
```ini
TELEGRAM_API_ID=...
TELEGRAM_API_HASH=...
BOT_TOKEN=...
LOGGER_BOT_TOKEN=...
NOTIFICATION_BOT_TOKEN=...
OWNER_ID=...
NOTIFICATION_CHANNEL_ID=-100...
ENCRYPTION_KEY=<the key you just generated>

# Self-hosted Mongo (host MUST be "mongo", include authSource=admin):
MONGO_ROOT_USER=jiren
MONGO_ROOT_PASS=<long-strong-password>
MONGO_CACHE_GB=1.5
MONGO_URI=mongodb://jiren:<long-strong-password>@mongo:27017/gokuads_db?authSource=admin
MONGO_DB_NAME=gokuads_db

# Proxies (needed at 100+ accounts; ~40 accounts per IP). One per line or ;-separated:
PROXY_LIST=socks5:1.2.3.4:1080:user:pass;socks5:5.6.7.8:1080

# Optional: full natural-language /manager chat (leave AI_API_KEY blank to skip)
AI_API_KEY=
AI_PROVIDER=openai
```

> ⚠️ **Rotate the tokens/URI committed in `config.py`** before going live and set
> fresh values only in `.env`. Never commit `.env`. Never change `ENCRYPTION_KEY`
> after accounts are added (it makes saved sessions undecryptable).

---

## 3. Launch the bot + database

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f jirenadbot
```
Expect log lines like `Main: @yourbot`, the manager printing `node 1/1`, then
`Spawned worker …` and `[HEALTH] …`. Open your bot in Telegram → `/start`.

Everyday ops:
```bash
docker compose logs -f jirenadbot     # follow logs
docker compose restart jirenadbot     # restart bot only
git pull && docker compose up -d --build   # update to latest code
docker compose down                   # stop all (Mongo data persists in the volume)
```

---

## 4. Add n8n + the Group-Discovery workflow

The bot's “🔎 Find Groups” feature reads Telegram group handles from the
`discovered_groups` collection. n8n populates that collection.

### 4.1 Add an n8n service

Create `docker-compose.n8n.yml` next to `docker-compose.yml`:
```yaml
services:
  n8n:
    image: n8nio/n8n:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:5678:5678"      # bind to localhost; reach via SSH tunnel or a reverse proxy
    environment:
      N8N_HOST: localhost
      N8N_PORT: "5678"
      N8N_PROTOCOL: http
      GENERIC_TIMEZONE: Asia/Kolkata
      # Set a login so it isn't open:
      N8N_BASIC_AUTH_ACTIVE: "true"
      N8N_BASIC_AUTH_USER: admin
      N8N_BASIC_AUTH_PASSWORD: ${N8N_PASSWORD}
    volumes:
      - n8n_data:/home/node/.n8n
volumes:
  n8n_data:
```
Add `N8N_PASSWORD=<strong-pass>` to `.env`, then start it **on the same Docker
network** as Mongo (so n8n can reach `mongo:27017`):
```bash
docker compose -f docker-compose.yml -f docker-compose.n8n.yml up -d
```
Open n8n from your laptop through an SSH tunnel (keeps it off the public net):
```bash
ssh -L 5678:127.0.0.1:5678 user@your-vps-ip
# then browse http://localhost:5678
```

### 4.2 Import the workflow & set the Mongo credential

1. In n8n: **Workflows → Import from File** → upload
   [`n8n/group-discovery.workflow.json`](./n8n/group-discovery.workflow.json).
2. Create a **MongoDB credential** named “Jiren Mongo”:
   - Connection string: `mongodb://jiren:<pass>@mongo:27017/gokuads_db?authSource=admin`
   - Database: `gokuads_db`
   - Assign it to the **“Mongo: save discovered_groups”** node (replace `REPLACE_MONGO_CRED_ID`).
3. **Activate** the workflow.

### 4.3 How it works / trigger it

- Flow: `POST /webhook/discover {niche, user_id}` → build search URLs → fetch pages
  → regex-extract `t.me/<username>` → dedupe → insert into `discovered_groups` → respond.
- Test it:
  ```bash
  curl -X POST http://localhost:5678/webhook/discover \
       -H 'Content-Type: application/json' \
       -d '{"niche":"crypto","user_id":123456789}'
  # -> {"ok":true,"discovered":N}
  ```
- The bot merges these into “🔎 Find Groups”, verifies each is a real public
  **group**, and lets users Join All.

> **Reliability tip:** the default workflow scrapes Bing/DuckDuckGo/directories,
> which get blocked/rate-limited. For production, swap the “Fetch pages” step for
> an official search API (SerpAPI, Bing Web Search, Google CSE). Respect each
> site's ToS and add delays. Private invite links (`t.me/+…`, `joinchat`) are
> intentionally skipped — only resolvable public @usernames are kept.

---

## 5. `/health` — live cluster status (admin)

Send **`/health`** to your bot. You get: fleet totals (accounts, forwarding,
users), running accounts, **per-node** workers/cap/CPU/RAM/proxies, last-window
send/fail/flood/timeout counts, and current control settings.

> `/health` shows per-node stats only when you run `manager.py` (the default in
> Docker). Node status auto-hides after `OPS_STALE_SECONDS` (150s) if a node dies.

---

## 6. `/manager` — talk to the ops-manager (admin)

Send **`/manager`** to enter a chat, then talk normally. Examples:

- “how are the workers doing?”
- “we have 50 accounts, can we add a worker if more come?”
- “one worker has too many accounts — reduce per-worker to 40”
- “set per proxy 30”, “set frequency 2”, “set max workers 24”

It reads live state and, when you ask to change something, writes the setting and
tells you — the manager applies it within ~1 minute (no restart of your side).
Type `exit` or `/endmanager` to leave.

- **With `AI_API_KEY`** set → full natural-language chat.
- **Without a key** → it still answers status and applies explicit commands.

Direct commands (no AI needed) do the same thing:
```
/setcap 40         # accounts per worker (5–500); lower = more, lighter workers
/setproxycap 30    # accounts per proxy/IP (1–500)
/setworkers 2 24   # min and max worker count
```

**Mental model the manager uses:**
- Timeouts high → lower per-worker cap (more, lighter workers).
- FloodWait/bans high → account-level: add proxies or lower frequency (more
  workers won't help).
- Workers auto-scale: lowering the per-worker cap is how you “add workers”.

---

## 7. Proxies (the #1 factor for 100+ accounts)

Telegram safely allows ~**40 accounts per IP**. So: 100 acc → ~3 IPs, 500 → ~13,
1000 → ~25, 2000 → ~50. Put them in `PROXY_LIST` (assigned stickily per account).
The manager **DMs you** when you're near capacity. Use clean residential/mobile
SOCKS5 — cheap datacenter IPs get flagged fast.

---

## 8. Backups

```bash
docker compose exec mongo sh -c \
 'mongodump --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" \
  --authenticationDatabase admin --archive --gzip' > backup-$(date +%F).gz
```
Also back up `.env` (holds the irreplaceable `ENCRYPTION_KEY`).

---

## 9. Scaling to 1000–2000 accounts (vertical vs. add-a-VPS)

The manager DMs you which lever to pull. Two options:

- **Vertical:** give the box more RAM/vCPU (RAM caps total accounts at ~15 MB each).
- **Horizontal (multi-VPS):** add another node. On **every** box set the same
  `NODE_COUNT`; give each a unique `NODE_ID` (0,1,2…). Node 0 runs the UI bot
  (`RUN_UI=1`); others are workers-only (`RUN_UI=0`) via
  [`docker-compose.worker.yml`](./docker-compose.worker.yml). All nodes share ONE
  Mongo (over a private VPN, or a managed cluster) and the **same `ENCRYPTION_KEY`**;
  each node needs its **own** proxies. Full runbook in [`SETUP.md`](./SETUP.md) §12.

Recommended: **~1000** = one 8 vCPU/16 GB box (or 2 nodes); **~2000** = 2–4 nodes.

---

## 10. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `jirenadbot` keeps restarting | `docker compose logs jirenadbot` — usually a missing env var or bad `MONGO_URI`. |
| `ServerSelectionTimeoutError` | `MONGO_URI` host must be `mongo`, password must match `MONGO_ROOT_PASS`, include `?authSource=admin`. |
| Accounts log out after redeploy | Keep the `./session` volume and never change `ENCRYPTION_KEY`. |
| FloodWait/bans climbing | Over per-IP capacity → add proxies to `PROXY_LIST`. |
| `/health` shows no nodes | You're running plain `python bot.py`; use `manager.py` (Docker default). |
| `/manager` says “AI unavailable” | Bad/empty `AI_API_KEY` or wrong `AI_PROVIDER`; explicit commands still work. |
| n8n can't reach Mongo | Start n8n with the same compose project so it shares the network; use host `mongo`. |
| n8n `/discover` returns 0 | Search engines blocked the scrape — switch “Fetch pages” to an official search API. |
| Worker crashed repeatedly (DM) | Check `docker compose logs jirenadbot` for the worker traceback (env/DB/auth). |

---

## 11. Pre-launch checklist

- [ ] Rotated every token/URI that was committed in `config.py`; fresh values only in `.env`.
- [ ] `ENCRYPTION_KEY` generated once, backed up, never changed.
- [ ] Strong `MONGO_ROOT_PASS`; Mongo has no public `ports:`.
- [ ] n8n behind basic-auth + localhost/SSH-tunnel (not open to the internet).
- [ ] Proxies configured for your account count (~40/IP).
- [ ] Firewall: only SSH open; Mongo and n8n stay private.
