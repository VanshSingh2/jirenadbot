#!/usr/bin/env python3
"""
Auto-scaling manager for Jiren Ads Bot.

Run this INSTEAD of bot.py in production to get automatic horizontal scaling on
one machine:

    python manager.py

What it does, every MANAGER_INTERVAL seconds:
  1. Keeps exactly one UI bot process alive (BOT_ROLE=bot).
  2. Counts active forwarding accounts in MongoDB and computes how many worker
     processes are needed (ceil(active / MAX_ACCOUNTS_PER_WORKER)), with
     hysteresis so it doesn't flap.
  3. Spawns / kills BOT_ROLE=worker subprocesses to match, passing a coordinated
     WORKER_COUNT / WORKER_ID so they shard accounts cleanly.
  4. Respawns any worker/bot process that dies.
  5. Checks proxy/IP capacity and, when you're about to run out, DMs the admin
     via the Telegram Bot API asking you to add more proxies (it cannot create
     proxies itself).

Scaling across MULTIPLE machines (VPS nodes) IS supported: run one manager per
box with a shared NODE_COUNT and a unique NODE_ID (0-based). Accounts are sharded
across nodes by a stable hash of the account id, so each box independently owns
and runs ~1/NODE_COUNT of the fleet with no cross-node coordination. The UI bot
runs on node 0 only (RUN_UI). The manager also DMs the admin when the box is
running low on RAM, CPU, worker slots, or proxies, and when it's time to add
another VPS node.
"""
import os
import sys
import time
import math
import signal
import hashlib
import subprocess
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import re
import certifi
import requests
from pymongo import MongoClient

from config import BOT_CONFIG

# ----------------------------- configuration --------------------------------
# Per-worker account cap. Default baseline is 100; with AUTO_WORKER_CAP on
# (default), the manager auto-detects the machine's CPU/RAM and LOWERS the cap if
# the box can't safely hold 100/worker (it never raises above the configured value
# unless you raise MAX_ACCOUNTS_PER_WORKER yourself).
WORKER_CAP_MIN = int(os.getenv('WORKER_CAP_MIN', '20'))
WORKER_CAP_MAX = int(os.getenv('WORKER_CAP_MAX', '200'))
PER_ACCOUNT_MB = float(os.getenv('PER_ACCOUNT_MB', '15'))   # est. RAM per connected account
RAM_RESERVE_MB = float(os.getenv('RAM_RESERVE_MB', '1024')) # RAM left for OS + UI bot + overhead
_CONFIGURED_CAP = int(os.getenv('MAX_ACCOUNTS_PER_WORKER', '100'))
_AUTO_WORKER_CAP = os.getenv('AUTO_WORKER_CAP', '1').strip().lower() in ('1', 'true', 'yes', 'on')


def _machine_info():
    cpu = os.cpu_count() or 1
    try:
        import psutil
        total_mb = psutil.virtual_memory().total / (1024 * 1024)
    except Exception:
        total_mb = float(os.getenv('ASSUMED_RAM_MB', '2048'))
    return cpu, total_mb


def _auto_worker_cap():
    """Accounts/worker the machine can safely hold: spread usable RAM across
    ~1 worker per CPU core. One process ≈ one core (GIL), so cores cap parallelism
    and RAM caps total accounts."""
    cpu, total_mb = _machine_info()
    usable = max(0.0, total_mb - RAM_RESERVE_MB)
    cap = int((usable / PER_ACCOUNT_MB) / max(1, cpu))
    return max(WORKER_CAP_MIN, min(WORKER_CAP_MAX, cap))


if _AUTO_WORKER_CAP:
    # Use the smaller of the configured baseline (100) and what the box can handle.
    MAX_ACCOUNTS_PER_WORKER = max(WORKER_CAP_MIN, min(_CONFIGURED_CAP, _auto_worker_cap()))
else:
    MAX_ACCOUNTS_PER_WORKER = _CONFIGURED_CAP

PER_IP_CAP = int(os.getenv('PER_IP_CAP', '40'))
MIN_WORKERS = int(os.getenv('MIN_WORKERS', '1'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '16'))
MANAGER_INTERVAL = int(os.getenv('MANAGER_INTERVAL', '60'))

# ---- Multi-VPS (horizontal) scaling ----
# Each machine runs its own manager. NODE_COUNT is the total number of VPS nodes;
# NODE_ID is THIS machine's 0-based index. With NODE_COUNT=1 nothing changes.
NODE_ID = int(os.getenv('NODE_ID', '0'))
NODE_COUNT = max(1, int(os.getenv('NODE_COUNT', '1')))
# Only ONE machine may run the Telegram UI bot (a bot token can only long-poll
# from one process). Defaults to ON for node 0, OFF for the rest.
RUN_UI = os.getenv('RUN_UI', '1' if NODE_ID == 0 else '0').strip().lower() in ('1', 'true', 'yes', 'on')

# ---- Capacity advisor (DMs admin which lever to pull) ----
CAP_WARN_FILL = float(os.getenv('CAP_WARN_FILL', '0.85'))   # warn at this fraction of node capacity
CAP_CRIT_FILL = float(os.getenv('CAP_CRIT_FILL', '0.95'))   # "add a VPS" at this fraction
CPU_WARN_LOAD = float(os.getenv('CPU_WARN_LOAD', '0.85'))   # per-core 5-min load avg to warn
CPU_WARN_CYCLES = int(os.getenv('CPU_WARN_CYCLES', '3'))    # consecutive hot cycles before alerting
ADVISOR_INTERVAL = int(os.getenv('ADVISOR_INTERVAL', '3600'))  # min seconds between same-topic DMs
CAP_TARGET_FILL = float(os.getenv('CAP_TARGET_FILL', '0.70'))  # sizing recommendations aim for this fill
# Scale down only after this many consecutive "could be smaller" checks (anti-flap).
DOWN_STABLE_CYCLES = int(os.getenv('DOWN_STABLE_CYCLES', '5'))
# Re-alert about proxies at most this often (seconds).
PROXY_ALERT_INTERVAL = int(os.getenv('PROXY_ALERT_INTERVAL', '3600'))
# Warn when we cross this fraction of current proxy capacity.
PROXY_WARN_FILL = float(os.getenv('PROXY_WARN_FILL', '0.85'))

# ---- Health-adaptive per-worker cap ----
# Workers report sends/fails/floods/timeouts to Mongo. The manager shrinks the
# per-worker cap (=> more workers, fewer accounts each) when CONNECTION stress is
# high (loop overloaded), and recovers it when calm. Telegram FLOOD is handled by
# alerting (add proxies / lower frequency) since more workers won't fix it.
STRESS_TIMEOUT_RATE = float(os.getenv('STRESS_TIMEOUT_RATE', '0.05'))  # timeouts/op to call it stressed
FLOOD_ALERT_RATE = float(os.getenv('FLOOD_ALERT_RATE', '0.10'))        # (floods+peerfloods)/sends to alert
MIN_OPS_FOR_SIGNAL = int(os.getenv('MIN_OPS_FOR_SIGNAL', '50'))        # ignore tiny samples
CAP_STEP_DOWN = float(os.getenv('CAP_STEP_DOWN', '0.8'))               # shrink to 80% under stress
CAP_STEP_UP = float(os.getenv('CAP_STEP_UP', '1.15'))                 # grow 15% when healthy
STRESS_CYCLES = int(os.getenv('STRESS_CYCLES', '2'))                   # stressed cycles before shrinking
RECOVER_CYCLES = int(os.getenv('RECOVER_CYCLES', '10'))               # calm cycles before growing
CAP_CHANGE_COOLDOWN = int(os.getenv('CAP_CHANGE_COOLDOWN', '600'))     # min seconds between cap changes
HEALTH_STALE_SECONDS = int(os.getenv('HEALTH_STALE_SECONDS', '120'))   # ignore health older than this

BOT_TOKEN = os.getenv('BOT_TOKEN') or BOT_CONFIG.get('bot_token')
OWNER_ID = int(os.getenv('OWNER_ID') or BOT_CONFIG.get('owner_id') or 0)
MONGO_URI = os.getenv('MONGO_URI') or BOT_CONFIG.get('mongo_uri')
DB_NAME = os.getenv('MONGO_DB_NAME') or BOT_CONFIG.get('db_name')

_use_tls = MONGO_URI.startswith('mongodb+srv://') or 'tls=true' in MONGO_URI.lower() or 'ssl=true' in MONGO_URI.lower()
if _use_tls:
    _mongo = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=8000)
else:
    _mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)
_db = _mongo[DB_NAME]
_accounts = _db['accounts']
_health = _db['worker_health']

# {worker_id: Popen}, plus the UI bot under key 'bot'
_procs = {}
_running_worker_count = 0     # WORKER_COUNT the live workers were started with
_down_counter = 0
_effective_cap = MAX_ACCOUNTS_PER_WORKER   # adaptive; starts at the configured/auto baseline
_stress_count = 0
_healthy_count = 0
_last_cap_change = 0.0
_last_flood_alert = 0.0
_last_proxy_alert = 0.0
_last_proxy_alert_needed = 0
_advice_state = {}   # topic -> {'last': ts, 'metric': float} for de-duping capacity DMs
_cpu_hot = 0         # consecutive high-CPU cycles
_stop = False


def _proxy_count():
    """Number of configured proxies (config PROXIES + PROXY_LIST env)."""
    count = 0
    raw = os.getenv('PROXY_LIST', '').strip()
    if raw:
        for line in re.split(r'[\n;]+', raw):
            if line.strip() and len(line.strip().split(':')) >= 3:
                count += 1
    try:
        from config import PROXIES as CFG_PROXIES
        count += len(CFG_PROXIES or [])
    except Exception:
        pass
    return count


def alert_admin(text):
    if not BOT_TOKEN or not OWNER_ID:
        print(f"[MANAGER] (no bot token/owner to alert) {text}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={'chat_id': OWNER_ID, 'text': text},
            timeout=15,
        )
    except Exception as e:
        print(f"[MANAGER] Failed to alert admin: {e}")


def _spawn(role, worker_id=None, worker_count=None):
    env = dict(os.environ)
    env['BOT_ROLE'] = role
    # Propagate this node's identity so child processes shard within the node.
    env['NODE_ID'] = str(NODE_ID)
    env['NODE_COUNT'] = str(NODE_COUNT)
    if role == 'worker':
        env['WORKER_ID'] = str(worker_id)
        env['WORKER_COUNT'] = str(worker_count)
        env['MAX_ACCOUNTS_PER_WORKER'] = str(_effective_cap)
        env['AUTO_WORKER_CAP'] = '0'  # manager already decided the cap
    here = os.path.dirname(os.path.abspath(__file__))
    p = subprocess.Popen([sys.executable, os.path.join(here, 'bot.py')], env=env)
    label = role if role == 'bot' else f"worker {worker_id}/{worker_count}"
    print(f"[MANAGER] Spawned {label} (pid {p.pid})")
    return p


def _stop_proc(p):
    if p is None or p.poll() is not None:
        return
    try:
        p.terminate()
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            p.kill()
    except Exception:
        pass


def _set_worker_count(n):
    """(Re)start the worker pool at size n with a coordinated WORKER_COUNT.
    Because account ownership is hash % WORKER_COUNT, changing the count requires
    restarting all workers together (brief reshuffle). The bot UI process is left
    untouched, and is_forwarding flags persist so workers reclaim accounts."""
    global _running_worker_count
    # Stop existing workers.
    for k in [k for k in _procs if k != 'bot']:
        _stop_proc(_procs.pop(k))
    # Start n fresh workers.
    for i in range(n):
        _procs[i] = _spawn('worker', worker_id=i, worker_count=n)
    _running_worker_count = n
    print(f"[MANAGER] Worker pool set to {n}")


def _ensure_bot_alive():
    if not RUN_UI:
        return  # UI bot lives on node 0 only; this node runs workers only.
    p = _procs.get('bot')
    if p is None or p.poll() is not None:
        _procs['bot'] = _spawn('bot')


def _stable_hash(value):
    """Same hash the bot uses for account ownership (keep in sync with bot.py)."""
    return int(hashlib.md5(str(value).encode()).hexdigest(), 16)


def _node_owns(account_id):
    if NODE_COUNT <= 1:
        return True
    return (_stable_hash(account_id) % NODE_COUNT) == (NODE_ID % NODE_COUNT)


def _respawn_dead_workers():
    for i in list(k for k in _procs if k != 'bot'):
        p = _procs.get(i)
        if p is not None and p.poll() is not None:
            print(f"[MANAGER] Worker {i} died (exit {p.returncode}); respawning")
            _procs[i] = _spawn('worker', worker_id=i, worker_count=_running_worker_count)


def _desired_workers(active):
    raw = math.ceil(active / _effective_cap) if active > 0 else MIN_WORKERS
    return max(MIN_WORKERS, min(MAX_WORKERS, raw))


def _read_health():
    """Aggregate recent per-worker health docs from Mongo."""
    agg = {'sends': 0, 'fails': 0, 'floods': 0, 'peerfloods': 0, 'timeouts': 0}
    try:
        cutoff = datetime.now() - timedelta(seconds=HEALTH_STALE_SECONDS)
        # Only this node's workers (each node's manager adapts independently).
        q = {'updated_at': {'$gte': cutoff}}
        if NODE_COUNT > 1:
            q['node_id'] = NODE_ID
        for d in _health.find(q):
            for k in agg:
                agg[k] += int(d.get(k, 0) or 0)
    except Exception as e:
        print(f"[MANAGER] health read failed: {e}")
    return agg


def _adapt_cap():
    """Adjust the effective per-worker cap from worker health.
    Returns True if the cap changed (caller should re-scale the pool)."""
    global _effective_cap, _stress_count, _healthy_count, _last_cap_change, _last_flood_alert
    agg = _read_health()
    ops = agg['sends'] + agg['fails']
    if ops < MIN_OPS_FOR_SIGNAL:
        return False  # not enough activity to judge

    timeout_rate = agg['timeouts'] / max(1, ops)
    flood_total = agg['floods'] + agg['peerfloods']
    flood_rate = flood_total / max(1, agg['sends'])
    now = time.time()

    # Telegram flood -> alert only (more workers won't help; needs proxies/slower).
    if flood_rate >= FLOOD_ALERT_RATE and (now - _last_flood_alert) >= PROXY_ALERT_INTERVAL:
        _last_flood_alert = now
        alert_admin(
            "⚠️ Jiren Ads Bot — high Telegram flood rate\n\n"
            f"~{flood_rate*100:.0f}% of sends hit FloodWait/PeerFlood in the last window.\n"
            "➡️ Add more proxies/IPs and/or lower the send frequency (/freq). "
            "Adding workers will NOT fix account-level flood limits."
        )
        print(f"[MANAGER] flood alert (rate={flood_rate:.2f})")

    # Connection stress -> shrink cap (=> more workers, lighter loops).
    if timeout_rate >= STRESS_TIMEOUT_RATE:
        _stress_count += 1
        _healthy_count = 0
    elif timeout_rate < STRESS_TIMEOUT_RATE / 2:
        _healthy_count += 1
        _stress_count = 0
    else:
        _stress_count = 0  # neutral zone

    if (now - _last_cap_change) < CAP_CHANGE_COOLDOWN:
        return False

    if _stress_count >= STRESS_CYCLES and _effective_cap > WORKER_CAP_MIN:
        new_cap = max(WORKER_CAP_MIN, int(_effective_cap * CAP_STEP_DOWN))
        if new_cap < _effective_cap:
            old = _effective_cap
            _effective_cap = new_cap
            _last_cap_change = now
            _stress_count = 0
            print(f"[MANAGER] CONNECTION STRESS (timeouts {timeout_rate*100:.0f}%): "
                  f"cap {old} -> {new_cap} (spreading accounts thinner)")
            alert_admin(f"⚙️ Auto-tuned: connection timeouts high (~{timeout_rate*100:.0f}%). "
                        f"Reduced accounts/worker {old}→{new_cap} and adding workers.")
            return True

    if _healthy_count >= RECOVER_CYCLES and _effective_cap < MAX_ACCOUNTS_PER_WORKER:
        new_cap = min(MAX_ACCOUNTS_PER_WORKER, int(_effective_cap * CAP_STEP_UP) + 1)
        if new_cap > _effective_cap:
            old = _effective_cap
            _effective_cap = new_cap
            _last_cap_change = now
            _healthy_count = 0
            print(f"[MANAGER] Healthy: cap {old} -> {new_cap} (consolidating)")
            return True

    return False


def _check_proxies(active):
    global _last_proxy_alert, _last_proxy_alert_needed
    proxies = _proxy_count()
    effective_ips = max(1, proxies)          # 0 proxies => host's single IP
    capacity = effective_ips * PER_IP_CAP
    needed_ips = math.ceil(active / PER_IP_CAP) if active > 0 else 0

    over = needed_ips > effective_ips
    approaching = active > capacity * PROXY_WARN_FILL
    if not (over or approaching):
        return

    now = time.time()
    # Only re-alert periodically, or sooner if the shortfall grew.
    if (now - _last_proxy_alert) < PROXY_ALERT_INTERVAL and needed_ips <= _last_proxy_alert_needed:
        return
    _last_proxy_alert = now
    _last_proxy_alert_needed = needed_ips

    to_add = max(0, needed_ips - effective_ips)
    if over:
        msg = (
            "⚠️ Jiren Ads Bot — PROXY CAPACITY\n\n"
            f"Active accounts: {active}\n"
            f"Proxies/IPs available: {effective_ips} (safe capacity ~{capacity})\n"
            f"You need about {needed_ips} IPs.\n\n"
            f"➡️ Please add ~{to_add} more proxy/IP(s) to PROXY_LIST and restart, "
            "or accounts will be limited/banned. (I can't create proxies myself.)"
        )
    else:
        msg = (
            "ℹ️ Jiren Ads Bot — proxies filling up\n\n"
            f"Active accounts: {active} / safe capacity ~{capacity} "
            f"({effective_ips} IP(s) × {PER_IP_CAP}).\n"
            "➡️ Consider adding more proxies/IPs soon."
        )
    alert_admin(msg)
    print(f"[MANAGER] Proxy alert sent (active={active}, ips={effective_ips}, needed={needed_ips})")


def _advise(topic, message, metric):
    """DM the admin at most once per ADVISOR_INTERVAL per topic, or sooner if the
    situation got materially worse (metric increased). Prevents alert spam while
    still escalating when things degrade."""
    st = _advice_state.get(topic, {'last': 0.0, 'metric': -1.0})
    now = time.time()
    if (now - st['last']) < ADVISOR_INTERVAL and metric <= st['metric'] + 1e-9:
        return
    _advice_state[topic] = {'last': now, 'metric': metric}
    alert_admin(message)
    print(f"[MANAGER] advisory[{topic}] sent (metric={metric:.2f})")


def _recommend_ram_gb(n_accounts):
    """GB of RAM to comfortably run n_accounts at CAP_TARGET_FILL headroom."""
    mb = (n_accounts * PER_ACCOUNT_MB) / max(0.1, CAP_TARGET_FILL) + RAM_RESERVE_MB
    return max(1, math.ceil(mb / 1024))


def _cpu_load_ratio(cpu):
    """5-minute load average per core (0..1+). 0 if unavailable (e.g. Windows)."""
    try:
        return os.getloadavg()[1] / max(1, cpu)
    except (OSError, AttributeError):
        return 0.0


def _node_label():
    return f"node {NODE_ID + 1}/{NODE_COUNT}"


def _capacity_advisor(owned):
    """Look at THIS node's headroom and DM the admin the specific lever to pull:
    add RAM, add vCPU/cores, add a whole VPS node, (proxies handled separately).
    `owned` = active accounts this node is running."""
    global _cpu_hot
    cpu, total_mb = _machine_info()
    usable = max(1.0, total_mb - RAM_RESERVE_MB)
    ram_cap = max(1, int(usable / PER_ACCOUNT_MB))              # accounts RAM can hold
    worker_cap = max(1, MAX_WORKERS * MAX_ACCOUNTS_PER_WORKER)  # accounts all workers can hold
    node_cap = min(ram_cap, worker_cap)                        # binding capacity
    fill = owned / node_cap

    # ---- Box near its TOTAL ceiling -> recommend adding a VPS (horizontal) ----
    if fill >= CAP_CRIT_FILL:
        target_gb = _recommend_ram_gb(owned)
        vertical = f"scale this box to ~{target_gb} GB RAM"
        if worker_cap <= ram_cap:
            vertical += " and more vCPU (raise MAX_WORKERS)"
        _advise(
            'add_node',
            "🚨 Jiren Ads Bot — NODE NEAR CAPACITY\n\n"
            f"{_node_label()} is running {owned} of ~{node_cap} safe accounts "
            f"({fill * 100:.0f}% full).\n\n"
            "Do ONE of these:\n"
            f"• ADD A VPS (recommended): set NODE_COUNT={NODE_COUNT + 1} on every node, "
            f"then boot the new box with NODE_ID={NODE_COUNT}. Accounts auto-rebalance.\n"
            f"• Or {vertical}.",
            fill,
        )
        return  # the critical alert already tells them everything; don't double-DM

    # ---- Warn zone: point at the specific binding resource ----
    if fill >= CAP_WARN_FILL:
        if ram_cap <= worker_cap:
            target_gb = _recommend_ram_gb(owned)
            _advise(
                'ram',
                "⚠️ Jiren Ads Bot — RAM FILLING UP\n\n"
                f"{_node_label()}: {owned} active accounts vs safe RAM capacity ~{ram_cap} "
                f"(~{PER_ACCOUNT_MB:.0f} MB/account).\n\n"
                f"➡️ Increase this VPS to ~{target_gb} GB RAM, "
                f"or add a VPS (NODE_COUNT={NODE_COUNT + 1}).",
                fill,
            )
        else:
            _advise(
                'cores',
                "⚠️ Jiren Ads Bot — WORKER/CPU CEILING\n\n"
                f"{_node_label()}: {owned} accounts, but only MAX_WORKERS={MAX_WORKERS} × "
                f"{MAX_ACCOUNTS_PER_WORKER}/worker = {worker_cap} can run here.\n\n"
                f"➡️ Add vCPU cores and raise MAX_WORKERS, "
                f"or add a VPS (NODE_COUNT={NODE_COUNT + 1}).",
                fill,
            )

    # ---- Sustained high CPU load -> recommend more cores ----
    load_ratio = _cpu_load_ratio(cpu)
    if load_ratio >= CPU_WARN_LOAD:
        _cpu_hot += 1
    else:
        _cpu_hot = 0
    if _cpu_hot >= CPU_WARN_CYCLES:
        _advise(
            'cpu',
            "⚠️ Jiren Ads Bot — HIGH CPU\n\n"
            f"{_node_label()}: 5-min load ~{load_ratio * 100:.0f}% per core ({cpu} vCPU) "
            f"for several minutes.\n\n"
            f"➡️ Add vCPU cores to this VPS, or add a VPS (NODE_COUNT={NODE_COUNT + 1}).",
            load_ratio,
        )
        _cpu_hot = 0


def _count_active():
    """Return (owned_by_this_node, fleet_total) active forwarding accounts.
    On a single node we use a cheap count; multi-node fetches ids to shard."""
    if NODE_COUNT <= 1:
        n = _accounts.count_documents({'is_forwarding': True})
        return n, n
    owned = total = 0
    for d in _accounts.find({'is_forwarding': True}, {'_id': 1}):
        total += 1
        if _node_owns(d['_id']):
            owned += 1
    return owned, total


def _handle_signal(signum, frame):
    global _stop
    _stop = True


def main():
    global _down_counter
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    print("=" * 50)
    print("Jiren Ads Bot — Auto-Scaling Manager")
    _cpu, _ram = _machine_info()
    _how = "auto" if _AUTO_WORKER_CAP else "fixed"
    print(f"node: {NODE_ID + 1}/{NODE_COUNT} | run_ui={RUN_UI}")
    print(f"machine: {_cpu} CPU, {_ram/1024:.1f} GB RAM | cap/worker={MAX_ACCOUNTS_PER_WORKER} ({_how}, baseline {_CONFIGURED_CAP})")
    print(f"per_ip_cap={PER_IP_CAP} workers[{MIN_WORKERS}..{MAX_WORKERS}] interval={MANAGER_INTERVAL}s")
    print("=" * 50)
    if not RUN_UI:
        print("[MANAGER] RUN_UI=0 -> workers-only node (UI bot runs on node 0).")

    _ensure_bot_alive()
    _owned0, _ = _safe_count()
    _set_worker_count(_desired_workers(_owned0))

    while not _stop:
        try:
            _ensure_bot_alive()
            _respawn_dead_workers()

            # active = accounts THIS node owns; total = whole fleet (for context).
            active, total = _safe_count()

            # Health-adaptive: shrink/grow accounts-per-worker from runtime errors.
            cap_changed = _adapt_cap()

            desired = _desired_workers(active)

            if cap_changed:
                # Cap changed -> restart pool so workers pick up the new cap (and
                # the worker count that matches it).
                print(f"[MANAGER] Re-scaling for new cap={_effective_cap} -> {desired} workers")
                _set_worker_count(desired)
                _down_counter = 0
            elif desired > _running_worker_count:
                print(f"[MANAGER] Scaling UP {_running_worker_count} -> {desired} (active={active})")
                _set_worker_count(desired)
                _down_counter = 0
            elif desired < _running_worker_count:
                _down_counter += 1
                if _down_counter >= DOWN_STABLE_CYCLES:
                    print(f"[MANAGER] Scaling DOWN {_running_worker_count} -> {desired} (active={active})")
                    _set_worker_count(desired)
                    _down_counter = 0
            else:
                _down_counter = 0

            _check_proxies(active)
            _capacity_advisor(active)
        except Exception as e:
            print(f"[MANAGER] Loop error: {e}")
        # Sleep in small steps so signals are handled promptly.
        for _ in range(MANAGER_INTERVAL):
            if _stop:
                break
            time.sleep(1)

    print("[MANAGER] Shutting down child processes...")
    for k in list(_procs):
        _stop_proc(_procs.pop(k))
    print("[MANAGER] Bye")


def _safe_count():
    try:
        return _count_active()
    except Exception as e:
        print(f"[MANAGER] active count failed: {e}")
        return 0, 0


if __name__ == '__main__':
    main()
