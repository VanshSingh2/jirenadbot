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


## Target-frequency pacing (set sends/hour per group)

Cycle delay is the pause *after* a full round; the real per-group interval is
`groups × msg_delay + cycle_delay`. With 100 groups × 5s, a round alone is ~8 min,
so tuning raw cycle delay is unintuitive.

Instead, users can set a **target frequency** — "message each group N times/hour":
- UI: Interval Settings → **🎯 Times/Hour per Group**, or command **`/freq <n>`** (0 = off).
- The forwarding loop auto-computes `round_delay = (3600/N) − groups×msg_delay`
  per account, so different group counts still hit ~N/hour.
- If an account has too many groups to reach N/hour even at full speed, it runs
  as fast as it safely can and the round-complete log shows a warning with the
  achievable rate.

Recommended **2–3/hour per group** — far safer for the accounts than the ~6–7/hr
that 5s/100-groups produces by default. Stored as `target_per_hour` on the user;
`0`/unset falls back to the interval preset's fixed cycle delay.

### Now: frequency is a GLOBAL admin-only policy
Frequency is no longer user-settable. It's a single global value
(`bot_settings.target_per_hour`), **default 3/hour, hard-capped at 3**
(`HARD_MAX_TARGET_PER_HOUR`). It always applies to every account.
- Only admins change it: `/freq <1-3>` or Interval Settings → 🎯 (admin only).
- Regular users just see "Frequency: ~3/hour per group (managed by admin)".
- Recommended rollout: set `/freq 2` to test, then `/freq 3`.



## Telegram connection timeouts / disconnects — causes & mitigations

The #1 cause of "connection timeout / websocket disconnect" in this bot was the
**synchronous Mongo driver blocking the event loop**: while a DB call ran,
Telethon couldn't service its socket (read data / send pings), so Telegram
dropped the connection and the next request timed out. That root cause is fixed
(caches, thread-offloaded DB, no per-send writes, capped accounts/worker).

On top of that, every account client now has:
- `connection_retries=None`, `retry_delay=5`, `auto_reconnect=True`,
  `request_retries=5`, and `timeout=TG_TIMEOUT` (30s).
- A **per-round connection watchdog** that reconnects if `is_connected()` is false.
- **Round-level handling of `asyncio.TimeoutError` / `ConnectionError` / `OSError`**:
  the loop backs off 15s and retries the round (keeps the client) instead of
  crashing — so a transient timeout never stops an account.
- `MIN_MSG_DELAY=5` floor so an account can't burst 100 groups in seconds.

If timeouts persist after this, the remaining causes are external: too many
connections per IP (add proxies), an overloaded box (add workers / lower
`MAX_ACCOUNTS_PER_WORKER`), or a flaky host network.



## Auto per-worker cap (CPU/RAM aware)

The manager auto-sizes `MAX_ACCOUNTS_PER_WORKER` from the machine:

    usable_RAM = total_RAM − RAM_RESERVE_MB
    auto_cap   = (usable_RAM / PER_ACCOUNT_MB) / CPU_cores   # ~1 worker per core
    effective  = clamp(min(baseline=100, auto_cap), WORKER_CAP_MIN, WORKER_CAP_MAX)

- **Baseline is 100** accounts/worker. Auto only **lowers** it on constrained
  boxes (it never raises above the configured baseline unless you raise
  `MAX_ACCOUNTS_PER_WORKER` yourself).
- RAM is per-account (≈15 MB), so it caps total accounts; cores cap parallelism
  (one process ≈ one core). The formula spreads RAM across ~1 worker/core.
- Examples: 8 GB / 4-core → ~100; 4 GB / 2-core → ~100; a constrained box →
  auto-lowers (e.g., 31). Set `AUTO_WORKER_CAP=0` + `MAX_ACCOUNTS_PER_WORKER=N`
  to force a fixed value. The boot log prints the machine specs and chosen cap.



## Health-adaptive scaling (manager reacts to runtime errors)

Workers write a health snapshot to `worker_health` every `HEALTH_REPORT_INTERVAL`
(sends, fails, FloodWait, PeerFlood, connection timeouts). The manager reads the
aggregate each cycle and adapts — distinguishing two very different problems:

- **Connection/loop stress** (timeouts/op ≥ `STRESS_TIMEOUT_RATE`, sustained for
  `STRESS_CYCLES`): the event loop is overloaded → **shrink accounts/worker**
  (`CAP_STEP_DOWN`), which makes the manager spawn more workers and lighten each
  loop. When calm for `RECOVER_CYCLES`, it grows the cap back toward baseline
  (`CAP_STEP_UP`). Changes are rate-limited by `CAP_CHANGE_COOLDOWN` to avoid
  restart thrash, and the worker pool restarts to apply the new cap.
- **Telegram flood** (FloodWait/PeerFlood rate ≥ `FLOOD_ALERT_RATE`): this is an
  account/IP limit — more workers can't fix it. The manager **alerts the admin**
  to add proxies or lower `/freq`, and does NOT churn workers for it.

So the manager now adjusts BOTH levers automatically: worker count (by account
load) and accounts-per-worker (by connection health), while flagging flood for
human action (proxies). All thresholds are env-tunable.



## Hardening pass (high-scale review)

- **Index on `is_forwarding`** added (`accounts.is_forwarding` + compound
  `[(is_forwarding,1),(owner_id,1)]`). The reconciler/manager query this every
  cycle; previously it was a collection scan at scale. The compound index also
  *covers* the reconciler's `{_id, owner_id}` projection (index-only read).
- **Mongo pool default lowered to 50/process.** Total connections ≈
  `(workers+2) × MONGO_MAX_POOL`; keep this within your cluster's limit
  (Atlas M10 ≈ 1500). Lower `MONGO_MAX_POOL` further if you run many workers.

### Known remaining limits to plan for (not yet done)
1. **Logger bot across multiple workers** shares one bot token; many Telethon
   connections on the same token can conflict. At multi-worker scale, route
   worker logs via the HTTP Bot API (stateless) or only log from the UI process.
2. **UI command handlers still use synchronous Mongo** — fine for forwarding
   (separate workers) but can lag the UI bot under thousands of *simultaneous*
   button taps. Migrate hot handler reads to the TTL cache / `db_call`.
3. **Worker pool restarts fully on cap/count change** (brief pause). Consistent
   hashing for shard ownership would let workers join/leave without a full
   reshuffle.
4. **MongoDB is the single coordination point / SPOF.** Use a real cluster
   (M10+), and at very high read volume enable secondary reads for the
   reconciler/health queries.
5. **Process-level restarts** still need a supervisor (systemd `Restart=always`
   or Docker `restart: unless-stopped`) in front of `manager.py`.



## Hardening pass 2 (implemented)

- **Logging via HTTP Bot API** (`_tg_send_http`): `send_log` and stop-logs now post
  to `api.telegram.org` in a worker thread. Workers no longer start a logger
  Telethon client (no shared-token / getUpdates conflict). The logger Telethon
  client runs ONLY on the UI process for its incoming `/start` link handler.
- **Per-round/error DB writes offloaded** (`update_account_stats`, `set_flood_wait`,
  `mark_group_failed`) via `db_call` so they never block the event loop.
- **Docker**: `Dockerfile` + `docker-compose.yml` run `manager.py` with
  `restart: unless-stopped` (covers process/host crashes — item #5). Sessions
  persist via the `./session` volume.
- **`ENCRYPTION_KEY` env**: stored sessions can be decrypted from a stable env key
  (set it once and keep it constant) instead of a file — clean for containers/secrets.

### Deliberately NOT changed
- **Consistent-hashing live rebalance (#3):** kept the safe modulo + coordinated
  restart. A live rebalance based on heartbeats risks two workers briefly owning
  the same account → double-send → exactly the FloodWait/ban we avoid. The brief,
  rare, cooldown-gated restart pause is the safer trade-off at this scale.
- **Async UI handlers (#2):** the UI bot runs as its own process, so its
  synchronous Mongo only affects UI responsiveness, never forwarding. A full
  async migration of the ~7k-line handler is high-risk for low gain here.



## UI speed at scale (fixed)

The UI bot lagged as users grew because every handler made several *synchronous*
pymongo calls on the single event loop, serializing all concurrent users.

- **`get_user` is now cached** (TTL `USER_CACHE_TTL`). Renders call it several
  times per tap; this collapses to one DB read per window. **All user writes go
  through `uupdate()`** which invalidates the cache, so reads are never stale
  after a write (toggles/settings reflect immediately). Verified: 5 reads → 1 DB
  hit; a write → next read is fresh.
- **Bigger loop thread pool** (`DB_THREAD_POOL`, default 64): offloaded blocking
  DB (`db_call`) runs in parallel across threads instead of blocking the loop, so
  concurrent users don't queue behind each other.
- **Run the UI as its own process** (`BOT_ROLE=bot`, which the manager does) so
  forwarding never competes with the UI loop. If you run `bot.py` directly as
  `all`, the UI shares the loop with forwarding — use `manager.py` in production.

Remaining per-render reads (`get_user_accounts`, some `count_documents`) are now
the long pole; they can be cached/offloaded next if a very large UI still lags,
but with `get_user` cached + the thread pool, UI stays responsive into the
thousands-of-users range.



## Self-hosted MongoDB (docker-compose)

`docker-compose.yml` now includes a `mongo` service so you can self-host instead
of paying for Atlas:

1. In `.env` set `MONGO_ROOT_USER` / `MONGO_ROOT_PASS` and
   `MONGO_URI=mongodb://USER:PASS@mongo:27017/gokuads_db?authSource=admin`.
2. `docker compose up -d --build`.

Notes:
- Mongo has **no published `ports:`** → reachable only on the private compose
  network (never exposed to the internet). Auth is enabled via the root creds.
- Data persists in the `mongo_data` named volume; cache is capped via
  `MONGO_CACHE_GB` (default 1.5) so it won't eat all the box's RAM.
- The client only enables TLS for Atlas (`mongodb+srv`) or `tls=true` URIs;
  local `mongodb://` connects without TLS (pymongo would otherwise error).
- Sizing: 2 GB is plenty for ≤500 accounts; 4 GB for a few thousand (this bot's
  dataset is tens of MB). Use SSD; back up the volume.



## Anti-ban features (Tier 1) — keeping accounts alive

1. **Spintax message variation** — `spin()` resolves `{a|b|c}` (nested) in the
   custom ad text, picking a random option per send so accounts don't broadcast
   identical text to many groups (the classic spam fingerprint). Automatic: no
   braces = unchanged.
2. **Smart rotation** (per-user toggle) — sends one rotating chunk of
   `ROTATION_CHUNK` groups per round instead of all at once; `round_delay` is
   scaled by the bucket count so each group still hits the 3/hr target, just in
   smaller, more human bursts.
3. **Account warmup** — accounts younger than `WARMUP_DAYS` (hard-capped at 2)
   ramp group volume from `WARMUP_MIN_FRACTION` up to 100% over the window, so a
   fresh account isn't instantly flagged. Uses the account's `added_at`.
4. **Per-account health auto-pause** — after `HEALTH_PEERFLOOD_LIMIT` consecutive
   PeerFlood rounds, the account is auto-paused (`is_forwarding=false`,
   `health_paused=true`) and the user is notified, instead of being pushed toward
   a ban. Restarting the account clears the health/auth flags.

All four respect the global 3/hr-per-group frequency policy. Campaign scheduling
(future) will only gate the time windows; the 3/hr rule still governs sends.
