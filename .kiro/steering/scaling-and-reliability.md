# Scaling & Reliability Guide (jirenadbot)

How this bot behaves under load, what was changed to make it scale, and the
playbook for taking it to 1000+ concurrent users without Telegram errors.

## The core constraint

The bot runs **everything on a single asyncio event loop** in one process:
the command handlers, the logger bot, and one `run_forwarding_loop` task per
forwarding account all share that loop. It talks to MongoDB with the
**synchronous** `pymongo` driver.

A synchronous DB call (`find_one`, `update_one`, `count_documents`) **blocks the
entire event loop** until it returns. With a handful of users this is invisible.
As active accounts grow, the cumulative blocking time per second is what makes
the bot "lag" and what makes message delays drift far above their configured
values. Reducing the *number* and *cost* of blocking calls is the highest-impact
lever, followed by moving blocking work off the loop entirely.

## What was already changed (perf/scale-fixes)

- **Bounded Mongo connection pool + timeouts** so concurrency reuses connections
  and a slow Atlas response can't hang the loop forever.
- **Short-TTL caches** (`get_user_cached`, cached `is_admin`, cached logs lookup)
  collapse the bursts of identical reads in hot paths. Caches are invalidated on
  the relevant writes (premium grant/revoke, admin add/remove, logs on/off).
- **Removed per-second DB polling**: the round-delay wait was querying the DB
  every second per account. It now sleeps in larger chunks and relies on task
  cancellation (which is instant) for stops. The per-group stop-check is
  throttled to every 10th group.
- **Batched stats**: one `update_account_stats` write per round instead of one
  per message.
- **Entity cache across rounds** so groups aren't re-resolved every round
  (repeated `get_entity` resolution is a common `FloodWait` trigger).
- **Send jitter** so many accounts don't hit Telegram in lockstep.
- **FloodWait / SlowMode / PeerFlood handling**: FloodWait and SlowMode set a
  per-group cooldown; PeerFlood stops the round and cools the whole account down
  (default 1h) to avoid bans.

Tunables (env vars): `MONGO_MAX_POOL`, `MONGO_SERVER_SELECTION_MS`,
`MONGO_SOCKET_MS`, `USER_CACHE_TTL`, `ADMIN_CACHE_TTL`, `LOGS_CACHE_TTL`,
`PEERFLOOD_COOLDOWN`.

## Conventions going forward

- **Never add a blocking DB call inside a hot loop** (per-message / per-group).
  Read once per round and reuse, or batch the write.
- **Prefer `get_user_cached` for read-only checks** in forwarding/permission
  paths. Keep `get_user` (uncached) for ban checks and the new-user flow, which
  must be live.
- **When adding a new user/account/logs mutation**, invalidate the matching
  cache (`invalidate_user_cache` / `invalidate_admin_cache` /
  `invalidate_logs_cache`).
- **Pass resolved entities, not username strings**, to send/forward calls; cache
  resolved entities and only re-resolve on failure.
- **Every Telegram send must tolerate `FloodWaitError`/`SlowModeWaitError`/
  `PeerFloodError`** and back off instead of retrying tightly.

## Roadmap to 1000+ users (recommended next steps)

1. **Move DB off the event loop.** Either migrate `pymongo` → `motor` (async,
   `await` every call) or wrap blocking calls with the provided `db_call`
   (`asyncio.to_thread`) helper in hot paths. This is the single biggest
   structural win and removes the root cause of load-dependent lag.
2. **Resume forwarding on startup.** On restart, accounts with
   `is_forwarding=True` are left with no running task. Add a staggered resume
   (with delays between accounts) so a restart doesn't drop everyone or trigger a
   reconnect storm.
3. **Shard across processes/workers.** One process/event loop cannot babysit
   thousands of live Telethon sessions. Partition accounts across multiple
   worker processes (e.g. by `owner_id` hash), each owning a subset of sessions,
   coordinated through MongoDB. The bot front-end stays separate from the
   forwarding workers.
4. **Centralize rate limiting per account** (a token-bucket keyed by account) so
   global send pacing is enforced regardless of how many groups/rounds run.
5. **Health/observability**: track event-loop lag, per-account send rate, and
   FloodWait frequency so regressions are visible before users notice.

## Known pre-existing issues to address separately

- `config.py` contains **real committed secrets** (bot tokens, Mongo URI with
  credentials, API hash). Rotate them and load from environment/secret storage.
- Latent `NameError`s exist in some legacy paths (e.g. an undefined `settings`
  in the legacy `forwarder_loop` auto-reply setup, an undefined `plan_id` in a
  callback). These predate the perf work and should be triaged.


## Production hardening (now implemented on main)

These were added on top of the perf fixes to keep the bot up and self-healing:

- **Telethon connection resilience** on every client (forwarding accounts + the
  3 bots): `connection_retries=None`, `retry_delay`, `auto_reconnect=True`,
  `request_retries`, `flood_sleep_threshold=60`. Transient network drops and
  small floods are handled automatically instead of crashing a loop.
- **Per-account supervisor** (`supervise_forwarding`): the task registered in
  `forwarding_tasks` is now the supervisor, which restarts the forwarding loop
  with exponential backoff (cap 5 min) if it crashes, and stops cleanly on
  cancel / when `is_forwarding` is cleared / when the session is unauthorized.
- **Startup resume** (`resume_active_forwarding`): on boot, accounts still marked
  `is_forwarding=true` are resumed, staggered (`RESUME_STAGGER_SECONDS`, default
  2s) to avoid a reconnect storm.
- **Unauthorized sessions** auto-disable themselves (`is_forwarding=false`,
  `auth_invalid=true`) and notify, instead of dying silently.
- **Loop exception handler** logs stray background-task errors instead of letting
  them take down the event loop; fatal exits print a full traceback.
- **Per-round DB preloading**: failed-group set and flood-wait map are loaded
  once per round (in a worker thread) instead of one query per group; the hot
  per-round account/group reads use the `db_call` thread-offload helper.

### Still recommended for true 1000+ scale

- **Run under a process manager** (systemd `Restart=always`, or Docker
  `restart: unless-stopped` / Kubernetes). In-process supervision covers task
  crashes; a process manager covers process-level crashes and OOM. This is the
  standard production setup and intentionally not reimplemented in-process
  (the Telethon clients are module-level and bound to one event loop).
- **Shard accounts across worker processes** once you outgrow a single loop.
- **Full `motor` async migration** for the remaining synchronous call sites
  outside the forwarding hot path (command handlers).


## Deployment & horizontal scaling (implemented)

The bot is now driven by a **desired-state reconciler**: pressing Start/Stop just
flips `is_forwarding` in MongoDB; each process runs a reconciler that owns a
**shard** of accounts and starts/stops supervisors to match. This makes scaling
a matter of running more processes — no code changes.

### Roles & workers (env vars)
- `BOT_ROLE=all` (default) — one process does UI + forwarding. Use this now.
- `BOT_ROLE=bot` — only the Telegram UI bot (no forwarding).
- `BOT_ROLE=worker` — only forwards its shard (no UI bot).
- `WORKER_COUNT` / `WORKER_ID` — accounts are split across `WORKER_COUNT`
  forwarding processes by a stable hash; each worker sets a unique `WORKER_ID`
  (0-based). Single process => `WORKER_COUNT=1, WORKER_ID=0` (owns everything).

Example multi-process layout (later):
- 1 × `BOT_ROLE=bot`
- N × `BOT_ROLE=worker WORKER_COUNT=N WORKER_ID=0..N-1`

### Proxies (sticky per account)
Set `PROXY_LIST` (see `.env.example`). Each account is pinned to one proxy by a
stable hash, so it always egresses from the same IP. Proxies apply to the
persistent forwarding clients automatically via `make_account_client`.

### When to add more IPs / proxies
Rules of thumb (Telegram is stricter than these in practice — stay conservative):
- **1 IP** is fine for your own handful of accounts (now).
- Keep roughly **≤ 30–50 logged-in account sessions per IP**. Past that, Telegram
  starts flagging the IP and you'll see more `PeerFlood`/auth issues.
- So: **add a proxy/IP for roughly every ~30–50 accounts.** 200 accounts ⇒
  ~4–6 IPs; 1000 accounts ⇒ ~20–30 IPs (residential/mobile proxies are safest).
- Add a **second machine + split workers** once one box passes ~150–300 active
  accounts (CPU/RAM and single-event-loop limits), or sooner if loop-lag grows.

### Process manager (required for production)
Run under systemd (`Restart=always`) or Docker (`restart: unless-stopped`).
In-process supervision restarts crashed account loops; the process manager
restarts the whole process on fatal crashes/OOM. Install `cryptg` (now in
requirements) so MTProto AES runs in C — a major CPU saving at scale.

### Ops
- `/health` (admin) and the periodic `[HEALTH]` log show role, worker, active
  accounts (local + DB), proxy count, uptime, RSS and CPU.


## Auto-scaling manager (`manager.py`)

For hands-off scaling on a single box, run `python manager.py` instead of
`bot.py`. It:
- keeps **1 UI bot** process (`BOT_ROLE=bot`) alive,
- counts active forwarding accounts and runs
  `ceil(active / MAX_ACCOUNTS_PER_WORKER)` **worker** processes (with hysteresis;
  scales down only after `DOWN_STABLE_CYCLES` calm cycles),
- respawns any process that dies,
- **DMs the admin** (via the Telegram Bot API) when proxy/IP capacity is about to
  run out — it cannot create proxies itself, so it tells you how many to add.

New users/accounts are picked up automatically by the reconciler; the manager
only adjusts the number of worker *processes*. Multi-machine elastic scaling is
still out of scope (one manager per box, or use Kubernetes).

Note: changing the worker count restarts the worker pool together (account
ownership is `hash % WORKER_COUNT`), a brief reshuffle. `is_forwarding` flags
persist, so workers immediately reclaim their accounts.

## Capacity is bounded by SEND RATE, not account count

A worker is one event loop. What saturates it is **aggregate sends/sec**, which
depends on each account's `msg_delay`:

    sends/sec per account  ≈ 1 / msg_delay
    accounts per worker    ≈ target_sends_per_sec / (1 / msg_delay)

With `cryptg` and the trimmed per-send path, target ~30 sends/sec/worker safely
(~50 ceiling). Examples: `msg_delay=5s` ⇒ ~150 accounts/worker; `msg_delay=30s`
⇒ the worker is limited by RAM/IP long before send rate. Set
`MAX_ACCOUNTS_PER_WORKER` from YOUR delay, not a fixed 150/200.

Per-send overhead was trimmed for throughput: user "sent to X" DB logs are now
batched into the round summary, and the live logger message is fire-and-forget +
bounded (`LIVE_LOG_MAX_PENDING`), so a flood-limited logger bot can never stall
forwarding. At very high scale set `LIVE_SEND_LOGS=0` (round summaries only).

## ⚠️ Account-level flood risk (most important)

Worker capacity is a *software* limit. The harder limit is Telegram's: an account
blasting **100+ distinct groups every 5s** is very likely to be PeerFlood-limited
or banned no matter how many workers/IPs you have. This is per-account behavior,
not hardware. Safer patterns: larger `msg_delay`, fewer groups per round, or
rotating which groups each round targets. Treat 5s/100-groups as the aggressive
end and expect account churn there.
