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
MAX_ACCOUNTS_PER_WORKER = int(os.getenv('MAX_ACCOUNTS_PER_WORKER', '150'))
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

BOT_TOKEN = os.getenv('BOT_TOKEN') or BOT_CONFIG.get('bot_token')
OWNER_ID = int(os.getenv('OWNER_ID') or BOT_CONFIG.get('owner_id') or 0)
MONGO_URI = os.getenv('MONGO_URI') or BOT_CONFIG.get('mongo_uri')
DB_NAME = os.getenv('MONGO_DB_NAME') or BOT_CONFIG.get('db_name')

_mongo = MongoClient(MONGO_URI, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=8000)
_db = _mongo[DB_NAME]
_accounts = _db['accounts']

# {worker_id: Popen}, plus the UI bot under key 'bot'
_procs = {}
_running_worker_count = 0     # WORKER_COUNT the live workers were started with
_down_counter = 0
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
        env['MAX_ACCOUNTS_PER_WORKER'] = str(MAX_ACCOUNTS_PER_WORKER)
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
    raw = math.ceil(active / MAX_ACCOUNTS_PER_WORKER) if active > 0 else MIN_WORKERS
    return max(MIN_WORKERS, min(MAX_WORKERS, raw))


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
    print(f"cap/worker={MAX_ACCOUNTS_PER_WORKER} per_ip_cap={PER_IP_CAP} "
          f"workers[{MIN_WORKERS}..{MAX_WORKERS}] interval={MANAGER_INTERVAL}s")
    print("=" * 50)

    _ensure_bot_alive()
    _set_worker_count(_desired_workers(_safe_count()))

    while not _stop:
        try:
            _ensure_bot_alive()
            _respawn_dead_workers()

            active = _safe_count()
            desired = _desired_workers(active)

            if desired > _running_worker_count:
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
