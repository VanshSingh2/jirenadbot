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

Scaling across MULTIPLE machines is out of scope here (use one manager per box,
or Kubernetes). This manager scales workers on the box it runs on.
"""
import os
import sys
import time
import math
import signal
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

_mongo = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=8000)
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
    p = _procs.get('bot')
    if p is None or p.poll() is not None:
        _procs['bot'] = _spawn('bot')


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
        for d in _health.find({'updated_at': {'$gte': cutoff}}):
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


def _count_active():
    return _accounts.count_documents({'is_forwarding': True})


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
    print(f"machine: {_cpu} CPU, {_ram/1024:.1f} GB RAM | cap/worker={MAX_ACCOUNTS_PER_WORKER} ({_how}, baseline {_CONFIGURED_CAP})")
    print(f"per_ip_cap={PER_IP_CAP} workers[{MIN_WORKERS}..{MAX_WORKERS}] interval={MANAGER_INTERVAL}s")
    print("=" * 50)

    _ensure_bot_alive()
    _set_worker_count(_desired_workers(_safe_count()))

    while not _stop:
        try:
            _ensure_bot_alive()
            _respawn_dead_workers()

            active = _safe_count()

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
        return 0


if __name__ == '__main__':
    main()
