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
