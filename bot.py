import os
import asyncio
import sys
import psutil
import random
import string
import re
import hashlib
import concurrent.futures
from datetime import datetime, timedelta
from telethon import TelegramClient, Button, events
from telethon.sessions import StringSession
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    PhoneNumberInvalidError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    PasswordHashInvalidError,
    ChannelPrivateError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
    MessageNotModifiedError,
    UserNotParticipantError,
    PeerFloodError,
    SlowModeWaitError,
)
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.functions.channels import GetParticipantRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ForwardMessagesRequest, DeleteChatUserRequest
from telethon.tl.types import Channel, Chat, User, InputPeerChannel, InputPeerChat, BotCommand, BotCommandScopeDefault
from cryptography.fernet import Fernet
from pymongo import MongoClient
import certifi
import time
import requests
import qrcode
import random

# Load environment variables from a local .env file if present (production secrets).
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from config import BOT_CONFIG, FREE_TIER, PREMIUM_TIER, MESSAGES, TOPICS, INTERVAL_PRESETS, FORCE_JOIN, PLANS, PLAN_SCOUT, PLAN_IMAGES, UPI_PAYMENT, PROXIES as CONFIG_PROXIES

# Proxies list (round-robin)
PROXIES = CONFIG_PROXIES
import python_socks

CONFIG = BOT_CONFIG

# Default quiet hours for new users and users without a setting.
DEFAULT_QUIET_HOURS = {
    'enabled': True,
    'start': '01:00',
    'end': '07:00',
    'label': '01:00-07:00',
}

# Helper function to get username from user ID
async def get_username_from_id(client, user_id: int):
    """Fetch username from Telegram using user ID"""
    try:
        user = await client.get_entity(user_id)
        return user.username  # None if no username
    except Exception:
        return None

async def resolve_target_user_id(raw: str, client):
    value = (raw or "").strip()
    if not value:
        return None
    if value.isdigit():
        return int(value)
    username = value[1:] if value.startswith('@') else value
    if not username:
        return None
    user_doc = users_col.find_one({'username': {'$regex': f'^{re.escape(username)}$', '$options': 'i'}})
    if user_doc and user_doc.get('user_id'):
        return int(user_doc['user_id'])
    try:
        entity = await client.get_entity(username)
        target_id = int(entity.id)
        if getattr(entity, 'username', None):
            uupdate(
                {'user_id': target_id},
                {'$set': {'username': entity.username}},
                upsert=True
            )
        else:
            get_user(target_id)
        return target_id
    except Exception:
        return None

def check_config():
    required = ['api_id', 'api_hash', 'bot_token', 'owner_id', 'mongo_uri']
    missing = []
    for key in required:
        val = CONFIG.get(key)
        if not val or val == '' or val == 0:
            missing.append(key.upper())
    return missing

missing_config = check_config()
if missing_config:
    print("\n" + "="*50)
    print("CONFIGURATION ERROR")
    print("="*50)
    print(f"Missing required secrets: {', '.join(missing_config)}")
    print("\nPlease add these secrets in the Secrets tab:")
    print("- TELEGRAM_API_ID")
    print("- TELEGRAM_API_HASH")
    print("- BOT_TOKEN")
    print("- OWNER_ID")
    print("- MONGO_URI")
    print("="*50)
    exit(1)

_env_key = os.getenv('ENCRYPTION_KEY', '').strip()
if _env_key:
    key = _env_key
elif not os.path.exists('encryption.key'):
    key = Fernet.generate_key().decode()
    with open('encryption.key', 'w') as f:
        f.write(key)
else:
    with open('encryption.key', 'r') as f:
        key = f.read().strip()
cipher_suite = Fernet(key.encode())

allow_invalid_tls = os.getenv('MONGO_TLS_INSECURE', '').strip().lower() in ('1', 'true', 'yes')
_mongo_uri = CONFIG['mongo_uri']
_mongo_kwargs = dict(
    # ---- Connection pool / timeout tuning (scales to many concurrent accounts) ----
    # The bot uses a synchronous driver shared by many forwarding tasks. A bounded,
    # reused pool prevents connection storms and the timeouts stop a slow Atlas
    # response from hanging the whole event loop indefinitely.
    # NOTE: this is PER PROCESS. With the manager running N workers, total Mongo
    # connections ≈ (N+2) * maxPoolSize, so keep it modest (Atlas M10 ~1500 max).
    maxPoolSize=int(os.getenv('MONGO_MAX_POOL', '50')),
    minPoolSize=int(os.getenv('MONGO_MIN_POOL', '5')),
    maxIdleTimeMS=60000,
    serverSelectionTimeoutMS=int(os.getenv('MONGO_SERVER_SELECTION_MS', '8000')),
    connectTimeoutMS=int(os.getenv('MONGO_CONNECT_MS', '8000')),
    socketTimeoutMS=int(os.getenv('MONGO_SOCKET_MS', '20000')),
    retryWrites=True,
    retryReads=True,
    appname='jirenadbot',
)
# Only enable TLS for Atlas (srv) or when explicitly requested. Self-hosted/local
# Mongo (mongodb://...) connects WITHOUT TLS — pymongo errors if TLS options are
# passed while TLS is disabled, so only add them when actually needed.
if _mongo_uri.startswith('mongodb+srv://') or 'tls=true' in _mongo_uri.lower() or 'ssl=true' in _mongo_uri.lower():
    _mongo_kwargs['tlsCAFile'] = certifi.where()
    _mongo_kwargs['tlsAllowInvalidCertificates'] = allow_invalid_tls
mongo_client = MongoClient(_mongo_uri, **_mongo_kwargs)
db = mongo_client[CONFIG['db_name']]

users_col = db['users']
accounts_col = db['accounts']
account_topics_col = db['account_topics']
account_settings_col = db['account_settings']
account_stats_col = db['account_stats']
account_auto_groups_col = db['account_auto_groups']
account_failed_groups_col = db['account_failed_groups']
account_flood_waits_col = db['account_flood_waits']
logger_tokens_col = db['logger_tokens']
admins_col = db['admins']
settings_col = db['bot_settings']
worker_health_col = db['worker_health']

# ---- Global, admin-controlled settings (e.g. the per-group send frequency) ----
# Frequency is a GLOBAL policy: default 3 sends/hour per group, hard-capped at 3.
# Users cannot change it; only admins can (for testing/rollout).
HARD_MAX_TARGET_PER_HOUR = int(os.getenv('HARD_MAX_TARGET_PER_HOUR', '3'))
DEFAULT_TARGET_PER_HOUR = int(os.getenv('DEFAULT_TARGET_PER_HOUR', '3'))
_global_settings_cache = {}  # key -> (expires_monotonic, value)


def get_global_setting(key, default):
    import time as _t
    now = _t.monotonic()
    hit = _global_settings_cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    val = default
    try:
        doc = settings_col.find_one({'_id': 'global'})
        if doc and key in doc:
            val = doc[key]
    except Exception:
        pass
    _global_settings_cache[key] = (now + 30, val)
    return val


def set_global_setting(key, value):
    settings_col.update_one({'_id': 'global'}, {'$set': {key: value}}, upsert=True)
    _global_settings_cache.pop(key, None)


def get_effective_target_per_hour():
    """Global per-group send frequency, clamped to [1, HARD_MAX_TARGET_PER_HOUR]."""
    try:
        v = int(get_global_setting('target_per_hour', DEFAULT_TARGET_PER_HOUR))
    except Exception:
        v = DEFAULT_TARGET_PER_HOUR
    return max(1, min(HARD_MAX_TARGET_PER_HOUR, v))

def ensure_indexes():
    """Create MongoDB indexes (idempotent)."""
    try:
        users_col.create_index('user_id', unique=True)
        users_col.create_index('username')

        accounts_col.create_index('owner_id')
        accounts_col.create_index([('owner_id', 1), ('is_forwarding', 1)])
        # The reconciler/manager query is_forwarding directly every cycle. Without a
        # leading-is_forwarding index these were COLLECTION SCANS at scale. This
        # compound index also covers the reconciler's {_id, owner_id} projection.
        accounts_col.create_index([('is_forwarding', 1), ('owner_id', 1)])

        account_topics_col.create_index([('account_id', 1), ('topic', 1)])
        account_auto_groups_col.create_index([('account_id', 1), ('group_id', 1)])
        account_failed_groups_col.create_index([('account_id', 1), ('group_key', 1)])
        account_flood_waits_col.create_index([('account_id', 1), ('group_key', 1)])
        account_flood_waits_col.create_index('wait_until')

        account_settings_col.create_index('account_id', unique=True)
        account_stats_col.create_index('account_id', unique=True)

        logger_tokens_col.create_index('token', unique=True)
        logger_tokens_col.create_index('account_id')

        admins_col.create_index('user_id', unique=True)
    except Exception as e:
        print(f"[DB] Index creation failed: {e}")

def ensure_user_defaults():
    """Backfill defaults for existing users (quiet hours)."""
    try:
        users_col.update_many(
            {'quiet_hours': {'$exists': False}},
            {'$set': {'quiet_hours': DEFAULT_QUIET_HOURS}}
        )
    except Exception as e:
        print(f"[DB] Default user settings update failed: {e}")

# --- Session directory setup ---
# Always store Telethon sqlite session files inside ./session/
SESSION_DIR = 'session'
os.makedirs(SESSION_DIR, exist_ok=True)

# Migrate any legacy session files from project root into ./session/
# (e.g. main_bot.session, logger_bot.session, and their -journal files)
for _name in ('main_bot', 'logger_bot'):
    for _suffix in ('.session', '.session-journal'):
        _src = f"{_name}{_suffix}"
        _dst = os.path.join(SESSION_DIR, _src)
        try:
            if os.path.exists(_src) and not os.path.exists(_dst):
                os.replace(_src, _dst)
        except Exception:
            # Non-fatal; bot can still run
            pass

# Point Telethon at the session base path (Telethon adds .session)
_BOT_CLIENT_KW = dict(
    connection_retries=None,   # keep retrying connection forever
    retry_delay=5,
    auto_reconnect=True,       # survive transient network drops without dying
    request_retries=5,
    flood_sleep_threshold=60,
)
main_bot = TelegramClient(os.path.join(SESSION_DIR, 'main_bot'), CONFIG['api_id'], CONFIG['api_hash'], **_BOT_CLIENT_KW)
logger_bot = TelegramClient(os.path.join(SESSION_DIR, 'logger_bot'), CONFIG['api_id'], CONFIG['api_hash'], **_BOT_CLIENT_KW)
notification_bot = TelegramClient(os.path.join(SESSION_DIR, 'notification_bot'), CONFIG['api_id'], CONFIG['api_hash'], **_BOT_CLIENT_KW)

# ===================== Global Text Styling =====================
# Telegram doesn't allow changing the app UI font, but we can stylize outgoing
# text using Unicode "small caps-ish" letters and make HTML messages bold.
# This is applied to all outgoing captions/messages and inline button labels.

# Small-caps-ish mapping (not all letters exist in Unicode; fallback keeps original)
_SMALLCAPS_MAP = {
    'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ꜰ', 'g': 'ɢ', 'h': 'ʜ',
    'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ', 'o': 'ᴏ', 'p': 'ᴘ',
    'q': 'ꞯ', 'r': 'ʀ', 's': 'ꜱ', 't': 'ᴛ', 'u': 'ᴜ', 'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x',
    'y': 'ʏ', 'z': 'ᴢ',
}
_SMALLCAPS_REVERSE_MAP = {v: k for k, v in _SMALLCAPS_MAP.items()}


def _to_smallcaps_char(ch: str) -> str:
    # Only stylize latin letters; keep everything else (emojis, punctuation, RTL, etc.)
    lower = ch.lower()
    if lower in _SMALLCAPS_MAP and ch.isalpha():
        return _SMALLCAPS_MAP[lower]
    return ch


def _from_smallcaps_char(ch: str) -> str:
    """Reverse mapping so HTML tag names aren't corrupted if already stylized."""
    if ch in _SMALLCAPS_REVERSE_MAP:
        return _SMALLCAPS_REVERSE_MAP[ch]
    return ch


def _normalize_html_tag(tag_text: str) -> str:
    """Normalize a single <...> tag by converting any small-caps letters back to ASCII."""
    return ''.join(_from_smallcaps_char(c) for c in tag_text)


def _stylize_plain(text: str) -> str:
    if not text:
        return text
    return ''.join(_to_smallcaps_char(c) for c in str(text))


def _stylize_html(html: str) -> str:
    """Stylize text while preserving HTML tags/entities and leaving <code>/<pre> blocks untouched.

    - Converts plain text to small-caps-ish Unicode
    - Normalizes tag names if they were previously stylized
    - Wraps the final output in <b>...</b> to give a consistent bold look
    """
    if not html:
        return html

    s = str(html)
    out = []

    in_entity = False
    in_code = False

    i = 0
    while i < len(s):
        ch = s[i]

        # Capture full HTML tag and normalize it
        if ch == '<':
            j = s.find('>', i + 1)
            if j == -1:
                out.append(_to_smallcaps_char(ch) if not in_code else ch)
                i += 1
                continue

            tag = s[i:j + 1]
            norm_tag = _normalize_html_tag(tag)

            lower = norm_tag.lower()
            if lower.startswith('<code') or lower.startswith('<pre'):
                in_code = True
            elif lower.startswith('</code') or lower.startswith('</pre'):
                in_code = False

            out.append(norm_tag)
            i = j + 1
            continue

        # Track HTML entities (&amp; etc.) so we don't corrupt them
        if ch == '&':
            in_entity = True
            out.append(ch)
            i += 1
            continue

        if in_entity:
            out.append(ch)
            if ch == ';':
                in_entity = False
            i += 1
            continue

        out.append(_to_smallcaps_char(ch) if not in_code else ch)
        i += 1

    styled = ''.join(out)

    # Make everything bold consistently (Telegram HTML). Safe even if nested.
    return f"<b>{styled}</b>"


def _stylize_buttons(buttons):
    """Recursively rebuild Telethon Button structures with stylized labels."""
    if not buttons:
        return buttons

    def rebuild(btn):
        # Telethon buttons are lightweight objects created by telethon.Button
        try:
            txt = getattr(btn, 'text', None)
            data = getattr(btn, 'data', None)
            url = getattr(btn, 'url', None)

            if url is not None:
                return Button.url(_stylize_plain(txt), url)
            if data is not None:
                return Button.inline(_stylize_plain(txt), data)
        except Exception:
            return btn
        return btn

    try:
        # buttons can be a list[list[Button]] or list[Button]
        if isinstance(buttons, list):
            rebuilt = []
            for row in buttons:
                if isinstance(row, list):
                    rebuilt.append([rebuild(b) for b in row])
                else:
                    rebuilt.append(rebuild(row))
            return rebuilt
    except Exception:
        return buttons

    return buttons


def _patch_client_text_methods(client: TelegramClient):
    """Patch send_message/send_file/edit_message to stylize outgoing text/captions + button labels."""
    orig_send_message = client.send_message
    orig_send_file = client.send_file
    orig_edit_message = client.edit_message

    async def send_message_wrapped(*args, **kwargs):
        # Telethon signature: send_message(entity, message=None, ...)
        # Check for _no_style flag to bypass font transformation
        no_style = kwargs.pop('_no_style', False)
        
        if not no_style:
            if len(args) >= 2 and isinstance(args[1], str) and 'message' not in kwargs:
                parse_mode = kwargs.get('parse_mode')
                args = list(args)
                args[1] = _stylize_html(args[1]) if str(parse_mode).lower() == 'html' else _stylize_plain(args[1])
            elif isinstance(kwargs.get('message'), str):
                parse_mode = kwargs.get('parse_mode')
                kwargs['message'] = _stylize_html(kwargs['message']) if str(parse_mode).lower() == 'html' else _stylize_plain(kwargs['message'])

            if 'buttons' in kwargs:
                kwargs['buttons'] = _stylize_buttons(kwargs['buttons'])

        return await orig_send_message(*args, **kwargs)

    async def send_file_wrapped(*args, **kwargs):
        # send_file(entity, file, caption=..., ...)
        if isinstance(kwargs.get('caption'), str):
            parse_mode = kwargs.get('parse_mode')
            kwargs['caption'] = _stylize_html(kwargs['caption']) if str(parse_mode).lower() == 'html' else _stylize_plain(kwargs['caption'])

        if 'buttons' in kwargs:
            kwargs['buttons'] = _stylize_buttons(kwargs['buttons'])

        return await orig_send_file(*args, **kwargs)

    async def edit_message_wrapped(*args, **kwargs):
        # edit_message(entity, message, text=..., ...)
        parse_mode = kwargs.get('parse_mode')

        # Handle positional text argument (common when calling client.edit_message(entity, msg_id, text, ...))
        if len(args) >= 3 and isinstance(args[2], str) and 'text' not in kwargs:
            args = list(args)
            args[2] = _stylize_html(args[2]) if str(parse_mode).lower() == 'html' else _stylize_plain(args[2])

        # Handle keyword text
        if isinstance(kwargs.get('text'), str):
            kwargs['text'] = _stylize_html(kwargs['text']) if str(parse_mode).lower() == 'html' else _stylize_plain(kwargs['text'])

        if 'buttons' in kwargs:
            kwargs['buttons'] = _stylize_buttons(kwargs['buttons'])

        return await orig_edit_message(*args, **kwargs)

    client.send_message = send_message_wrapped
    client.send_file = send_file_wrapped
    client.edit_message = edit_message_wrapped


# Apply patch to both bots
_patch_client_text_methods(main_bot)
_patch_client_text_methods(logger_bot)

user_states = {}
forwarding_tasks = {}
auto_reply_clients = {}
last_replied = {}

# Auto group-join cancellation flags (uid -> bool)
auto_join_cancel = {}

# Per-user forwarding loop (so all accounts send in parallel, then round delay once)
user_forwarding_tasks = {}  # user_id -> asyncio.Task

# Payment tracking (gateway.py integration)
# (Removed) gateway payment tracking (manual UPI now)

ACCOUNTS_PER_PAGE = 7

# (Removed) External payment gateway integration
# ===================== Manual UPI Payment Helpers =====================

# In-memory pending payments
# pending_upi_payments[request_id] = {
#   'user_id': int, 'username': str|None, 'plan_key': str, 'plan_name': str,
#   'price': int, 'created_at': datetime, 'status': 'awaiting_screenshot'|'submitted'
# }
pending_upi_payments = {}

# Map admin message -> request_id so approve/reject can find it
admin_payment_message_map = {}


def _new_payment_request_id(uid: int, plan_key: str) -> str:
    # short unique id for callbacks
    return f"p{uid}_{plan_key}_{int(datetime.now().timestamp())}{random.randint(100,999)}"


def _upi_payment_caption(plan: dict, plan_key: str) -> str:
    upi_id = UPI_PAYMENT.get('upi_id', '')
    payee = UPI_PAYMENT.get('payee_name', '')
    return (
        f"<b>🧾 Manual UPI Payment</b>\n\n"
        f"<b>Plan:</b> {plan.get('name', plan_key).title()}\n"
        f"<b>Price:</b> {plan.get('price_display', plan.get('price', ''))}\n\n"
        f"<b>UPI ID:</b> <code>{_h(upi_id)}</code>\n"
        f"<b>Name:</b> {_h(payee)}\n\n"
        f"<blockquote>Scan the QR and pay. Then tap <b>Payment Done</b> and send payment screenshot.</blockquote>"
    )
# ===================== Force Join (Config-based: Channel + Group) =====================

def _forcejoin_usernames():
    # Channel-only force join
    ch = (FORCE_JOIN.get('channel_username') or '').strip().lstrip('@')
    return ch, ''

async def _is_member_of(username: str, user_id: int) -> bool:
    if not username:
        return True
    try:
        entity = await main_bot.get_entity(username)
        await main_bot(GetParticipantRequest(entity, user_id))
        return True
    except (UserNotParticipantError, ChannelPrivateError, ValueError):
        return False
    except Exception:
        # Fail-open to avoid locking everyone out if Telegram errors
        return True

async def is_user_passed_forcejoin(user_id: int) -> bool:
    if is_admin(user_id):
        return True
    if not FORCE_JOIN.get('enabled', False):
        return True

    channel_username, group_username = _forcejoin_usernames()
    # If misconfigured (missing usernames), don't block
    if not channel_username and not group_username:
        return True

    ok_channel = await _is_member_of(channel_username, user_id)
    return ok_channel

def forcejoin_keyboard():
    channel_username, _ = _forcejoin_usernames()
    buttons = []
    if channel_username:
        buttons.append([Button.url("Join Channel", f"https://t.me/{channel_username}")])
    buttons.append([Button.inline("Verify", b"force_verify")])
    return buttons

async def send_forcejoin_prompt(event, edit=False):
    msg = FORCE_JOIN.get('message') or "**Access Locked**\n\nPlease join required chats and verify."
    img = (FORCE_JOIN.get('image_url') or '').strip()

    if edit:
        # can't edit media easily; edit text only
        await event.edit(msg, buttons=forcejoin_keyboard())
        return

    if img:
        await event.respond(file=img, message=msg, buttons=forcejoin_keyboard())
    else:
        await event.respond(msg, buttons=forcejoin_keyboard())

async def enforce_forcejoin_or_prompt(event, edit=False) -> bool:
    uid = event.sender_id
    if await is_user_passed_forcejoin(uid):
        return True
    await send_forcejoin_prompt(event, edit=edit)
    return False

# ===================== Lightweight TTL caches (performance) =====================
# This bot uses a *synchronous* Mongo driver on a single asyncio event loop, so
# every DB read blocks ALL tasks (command handling + every account's forwarding
# loop). As the number of active users grows, the cumulative blocking time is
# what makes the bot "lag" and delays balloon.
#
# These short-lived caches collapse the bursts of repeated identical reads that
# happen in hot paths (the forwarding loop reads the same user/account doc many
# times per round; send_log + is_admin run on every single message). TTLs are
# deliberately tiny so interactive correctness is effectively unchanged, while
# the per-second DB pressure drops dramatically.
import threading as _threading

_cache_lock = _threading.Lock()
_user_cache = {}        # user_id(int)      -> (expires_monotonic, user_doc)
_admin_cache = {}       # user_id(int)      -> (expires_monotonic, bool)
_logs_chat_cache = {}   # account_id(str)   -> (expires_monotonic, chat_id)

_USER_CACHE_TTL = float(os.getenv('USER_CACHE_TTL', '5'))
_ADMIN_CACHE_TTL = float(os.getenv('ADMIN_CACHE_TTL', '30'))
_LOGS_CACHE_TTL = float(os.getenv('LOGS_CACHE_TTL', '20'))


def invalidate_user_cache(user_id=None):
    with _cache_lock:
        if user_id is None:
            _user_cache.clear()
        else:
            _user_cache.pop(int(user_id), None)


def invalidate_admin_cache(user_id=None):
    with _cache_lock:
        if user_id is None:
            _admin_cache.clear()
        else:
            _admin_cache.pop(int(user_id), None)


def invalidate_logs_cache():
    with _cache_lock:
        _logs_chat_cache.clear()


def uupdate(flt, update, *args, **kwargs):
    """users_col.update_one wrapper that auto-invalidates the user cache. ALL user
    writes go through this so cached get_user() reads are never stale after a write
    (toggles/settings reflect immediately)."""
    res = users_col.update_one(flt, update, *args, **kwargs)
    try:
        uid = flt.get('user_id') if isinstance(flt, dict) else None
        if uid is not None:
            invalidate_user_cache(uid)
    except Exception:
        pass
    return res


async def db_call(func, *args, **kwargs):
    """Run a blocking (pymongo) callable in a worker thread so it never freezes
    the event loop. Use this for DB work inside hot async paths; if Atlas is slow
    the rest of the bot stays responsive instead of stalling for everyone."""
    return await asyncio.to_thread(func, *args, **kwargs)


def is_admin(user_id):
    # Owner is always admin
    try:
        uid = int(user_id)
    except Exception:
        return False
    if uid == int(CONFIG['owner_id']):
        return True
    now = time.monotonic()
    with _cache_lock:
        hit = _admin_cache.get(uid)
        if hit and hit[0] > now:
            return hit[1]
    try:
        result = admins_col.find_one({'user_id': uid}) is not None
    except Exception as e:
        print(f"[ERROR] is_admin check failed for {user_id}: {e}")
        return False
    with _cache_lock:
        _admin_cache[uid] = (now + _ADMIN_CACHE_TTL, result)
    return result

def get_user(user_id):
    uid = int(user_id)
    now = time.monotonic()
    # Cache hit: UI renders call get_user several times per tap; caching collapses
    # those to one DB read per USER_CACHE_TTL. Writes go through uupdate() which
    # invalidates this cache, so reads are never stale after a write.
    with _cache_lock:
        hit = _user_cache.get(uid)
        if hit and hit[0] > now:
            return dict(hit[1])
    user = users_col.find_one({'user_id': uid})
    if not user:
        quiet_default = DEFAULT_QUIET_HOURS.copy()
        user = {
            'user_id': uid,
            'tier': 'free',
            'max_accounts': FREE_TIER['max_accounts'],
            'approved': False,
            'autoreply_enabled': False,
            'interval_preset': 'fast',
            'forwarding_mode': 'auto',
            'ads_mode': 'saved',
            'smart_rotation': False,
            'quiet_hours': quiet_default,
            'created_at': datetime.now(),
            '_is_new_user': True
        }
        users_col.insert_one(user)
    elif not user.get('quiet_hours'):
        quiet_default = DEFAULT_QUIET_HOURS.copy()
        uupdate({'user_id': uid}, {'$set': {'quiet_hours': quiet_default}})
        user['quiet_hours'] = quiet_default
    with _cache_lock:
        _user_cache[uid] = (now + _USER_CACHE_TTL, user)
    return dict(user)

def get_user_cached(user_id):
    """Short-TTL cached read of get_user for hot paths (forwarding loop + derived
    permission helpers). Returns a shallow copy so callers can't pollute the
    cache. Deliberately NOT used for ban-checks / new-user flows, which stay on
    the uncached get_user()."""
    uid = int(user_id)
    now = time.monotonic()
    with _cache_lock:
        hit = _user_cache.get(uid)
        if hit and hit[0] > now:
            return dict(hit[1])
    user = get_user(uid)
    with _cache_lock:
        _user_cache[uid] = (now + _USER_CACHE_TTL, user)
    return dict(user)

def is_premium(user_id):
    """Premium check with expiry enforcement (auto-downgrade when expired)."""
    if is_admin(user_id):
        return True

    user = get_user_cached(user_id)
    if user.get('tier') != 'premium':
        return False

    expires_at = user.get('premium_expires_at') or user.get('premium_expiry') or user.get('plan_expiry')
    if expires_at:
        try:
            # If stored datetime is naive, treat as local and compare with now()
            if expires_at < datetime.now():
                remove_user_premium(user_id)
                return False
        except Exception:
            # If comparison fails, fail-open (keep premium) to avoid breaking users
            return True

    return True

def has_per_account_config_access(user_id):
    """Check if user can access per-account config (Super/Ultra only)."""
    if is_admin(user_id):
        return True
    return get_user_max_accounts(user_id) >= 5

def get_user_tier_settings(user_id):
    if is_premium(user_id):
        return PREMIUM_TIER.copy()
    return FREE_TIER.copy()

def get_user_auto_group_limit(user_id):
    """Plan-specific cap for auto groups per account."""
    if is_admin(user_id):
        return None
    user = get_user_cached(user_id)
    if not is_premium(user_id):
        return FREE_TIER.get('max_auto_groups', 0)
    plan_key = normalize_plan_key(user.get('plan') or user.get('plan_name'))
    if plan_key in PLANS:
        return PLANS[plan_key].get('max_auto_groups')
    return PREMIUM_TIER.get('max_auto_groups')

def get_user_max_accounts(user_id):
    if is_admin(user_id):
        return 999  # Admins get unlimited accounts
    user = get_user_cached(user_id)
    return get_plan_max_accounts(user)

def normalize_plan_key(value: str) -> str:
    key = (value or "").strip().lower()
    if not key:
        return ""
    name_map = {
        'kai': 'grow',
        'super': 'prime',
        'ultra': 'dominion',
    }
    if key in name_map:
        return name_map[key]
    if key == 'domi':
        return 'dominion'
    if key in ('grow', 'prime', 'dominion'):
        return key
    return ""

def get_plan_label(plan_key: str) -> str:
    key = normalize_plan_key(plan_key)
    return {
        'grow': 'Kai',
        'prime': 'Super',
        'dominion': 'Ultra',
    }.get(key, plan_key.capitalize() if plan_key else "No Plan")

def get_display_plan_name(user: dict) -> str:
    plan_name = (user.get('plan_name') or "").strip()
    if plan_name:
        normalized = plan_name.lower()
        name_map = {
            'grow': 'Kai',
            'prime': 'Super',
            'dominion': 'Ultra',
            'kai': 'Kai',
            'super': 'Super',
            'ultra': 'Ultra',
        }
        return name_map.get(normalized, plan_name)
    plan_key = normalize_plan_key(user.get('plan'))
    return {
        'grow': 'Kai',
        'prime': 'Super',
        'dominion': 'Ultra',
    }.get(plan_key, "No Plan")

def get_plan_max_accounts(user: dict) -> int:
    if not user or user.get('tier') != 'premium':
        return FREE_TIER['max_accounts']
    plan_key = normalize_plan_key(user.get('plan') or user.get('plan_name'))
    if plan_key in PLANS:
        return PLANS[plan_key]['max_accounts']
    old_max = user.get('max_accounts', PREMIUM_TIER['max_accounts'])
    if old_max >= 15:
        return PLANS['dominion']['max_accounts']
    if old_max >= 7:
        return PLANS['prime']['max_accounts']
    if old_max >= 3:
        return PLANS['grow']['max_accounts']
    return PLANS['grow']['max_accounts']

def is_approved(user_id):
    if is_admin(user_id):
        return True
    user = get_user_cached(user_id)
    return user.get('approved', False)

def approve_user(user_id):
    uupdate(
        {'user_id': int(user_id)},
        {'$set': {'approved': True, 'approved_at': datetime.now()}},
        upsert=True
    )
    invalidate_user_cache(user_id)

def set_user_premium(user_id, max_accounts, plan_name='premium'):
    """Grant premium with 30-day expiry (monthly subscription)."""
    expires_at = datetime.now() + timedelta(days=30)
    
    # Determine plan key (grow, prime, dominion) from plan_name
    plan_key = normalize_plan_key(plan_name) or 'grow'
    plan_label = get_plan_label(plan_key)
    
    uupdate(
        {'user_id': int(user_id)},
        {'$set': {
            'tier': 'premium',
            'plan': plan_key,  # Store plan key (grow/prime/dominion) for profile display
            'max_accounts': max_accounts,
            'plan_name': plan_label,  # Store actual plan name (Kai/Super/Ultra)
            'premium_granted_at': datetime.now(),
            'premium_expires_at': expires_at,
            'plan_expiry': expires_at,  # Add this for profile display
            'approved': True
        }},
        upsert=True
    )
    invalidate_user_cache(user_id)

def remove_user_premium(user_id):
    """Downgrade user to free and clear premium-related fields."""
    uupdate(
        {'user_id': int(user_id)},
        {'$set': {
            'tier': 'free',
            'plan': 'scout',
            'plan_name': 'No Plan',
            'max_accounts': FREE_TIER['max_accounts'],
            'premium_expires_at': None,
            'premium_expiry': None,
            'plan_expiry': None,
        }}
    )
    invalidate_user_cache(user_id)

def get_all_users():
    return list(users_col.find({}))

def get_premium_users():
    return list(users_col.find({'tier': 'premium'}))

def get_user_accounts(user_id):
    return list(accounts_col.find({'owner_id': user_id}).sort('added_at', 1))

async def start_broadcast_for_user(target_id: int) -> int:
    accounts = get_user_accounts(target_id)
    if not accounts:
        return 0
    user = get_user(target_id)
    fwd_mode = user.get('forwarding_mode', 'topics')
    started = 0
    for acc in accounts:
        acc_id = str(acc['_id'])
        is_fwd = acc.get('is_forwarding', False)
        print(f"[ADS DEBUG] Account {acc_id}: is_forwarding={is_fwd}, fwd_mode={fwd_mode}")
        if is_fwd:
            print(f"[ADS DEBUG] Account {acc_id} already forwarding, skipped")
            continue
        has_groups = False
        if fwd_mode in ('topics', 'both'):
            topic_count = account_topics_col.count_documents({'account_id': {'$in': _account_id_variants(acc['_id'])}})
            print(f"[ADS DEBUG] Topics count: {topic_count}")
            has_groups = topic_count > 0
        if fwd_mode in ('auto', 'both') and not has_groups:
            auto_count = account_auto_groups_col.count_documents({'account_id': {'$in': _account_id_variants(acc['_id'])}})
            print(f"[ADS DEBUG] Auto groups count: {auto_count}")
            has_groups = auto_count > 0
        print(f"[ADS DEBUG] has_groups={has_groups}")
        accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': True}})
        # Start locally now if we own this account; else the owning worker's
        # reconciler picks it up from the is_forwarding flag.
        if ensure_account_running(target_id, acc['_id']):
            status_msg = " (⚠️ No groups configured!)" if not has_groups else ""
            print(f"[ADS] Started forwarding task for account {acc['_id']}{status_msg}")
        started += 1
    if started:
        print(f"[ADS] Started {started} accounts for user {target_id}")
    return started

async def stop_broadcast_for_user(target_id: int, *, by_admin: bool = False) -> int:
    accounts = get_user_accounts(target_id)
    stopped = 0
    for acc in accounts:
        if acc.get('is_forwarding'):
            accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': False}})
            if acc['_id'] in forwarding_tasks:
                forwarding_tasks[acc['_id']].cancel()
                del forwarding_tasks[acc['_id']]
            stopped += 1
    if stopped > 0:
        try:
            user_doc = get_user(target_id)
            logs_chat_id = user_doc.get('logs_chat_id')
            if logs_chat_id and CONFIG.get('logger_bot_token'):
                reason = "by admin" if by_admin else "by user"
                log_msg = (
                    f"<b>⏹️ Broadcast Stopped</b>\n\n"
                    f"<b>Accounts Stopped:</b> <code>{stopped}</code>\n\n"
                    f"<i>Broadcasts stopped {reason}.</i>"
                )
                await _tg_send_http(CONFIG['logger_bot_token'], int(logs_chat_id), log_msg)
        except Exception as e:
            print(f"[LOG ERROR] Failed to send stop log to user {target_id}: {e}")
    return stopped

async def stop_all_broadcasts() -> int:
    stopped = 0
    for acc in accounts_col.find({'is_forwarding': True}):
        accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': False}})
        if acc['_id'] in forwarding_tasks:
            forwarding_tasks[acc['_id']].cancel()
            del forwarding_tasks[acc['_id']]
        stopped += 1
    if stopped:
        print(f"[ADS] Stopped {stopped} accounts (global)")
    return stopped

def get_account_by_id(account_id):
    from bson.objectid import ObjectId
    try:
        return accounts_col.find_one({'_id': ObjectId(account_id)})
    except:
        return None

def get_account_by_index(user_id, index):
    accounts = get_user_accounts(user_id)
    if 0 < index <= len(accounts):
        return accounts[index - 1]
    return None

def get_account_settings(account_id):
    settings = account_settings_col.find_one({'account_id': account_id})
    if not settings:
        settings = {
            'account_id': account_id,
            # group_delay deprecated (no longer used)
            'msg_delay': FREE_TIER['msg_delay'],
            'round_delay': FREE_TIER['round_delay'],
            'logs_chat_id': None,
        }
        account_settings_col.insert_one(settings)
    return settings

def update_account_settings(account_id, updates):
    account_settings_col.update_one(
        {'account_id': account_id},
        {'$set': updates},
        upsert=True
    )

def get_account_stats(account_id):
    stats = account_stats_col.find_one({'account_id': account_id})
    if not stats:
        stats = {'account_id': account_id, 'total_sent': 0, 'total_failed': 0, 'last_forward': None}
        account_stats_col.insert_one(stats)
    return stats

def update_account_stats(account_id, sent=0, failed=0):
    account_stats_col.update_one(
        {'account_id': account_id},
        {'$inc': {'total_sent': sent, 'total_failed': failed}, '$set': {'last_forward': datetime.now()}},
        upsert=True
    )

# ---- Batched, single-query helpers (replace N+1 per-account loops in renders) ----
def any_account_has_autoreply(accounts):
    ids = [str(acc['_id']) for acc in accounts]
    if not ids:
        return False
    return account_settings_col.count_documents(
        {'account_id': {'$in': ids}, 'auto_reply': {'$nin': [None, '']}}
    ) > 0

def bulk_topic_counts(accounts, topics):
    """One aggregation -> {topic: count} across all the user's accounts."""
    ids = [acc['_id'] for acc in accounts]
    if not ids:
        return {}
    agg = account_topics_col.aggregate([
        {'$match': {'account_id': {'$in': ids}, 'topic': {'$in': list(topics)}}},
        {'$group': {'_id': '$topic', 'n': {'$sum': 1}}},
    ])
    return {d['_id']: d['n'] for d in agg}

def bulk_auto_group_count(accounts):
    ids = [str(acc['_id']) for acc in accounts]
    if not ids:
        return 0
    return account_auto_groups_col.count_documents({'account_id': {'$in': ids}})

def bulk_total_sent(accounts):
    ids = [str(acc['_id']) for acc in accounts]
    if not ids:
        return 0
    return sum(s.get('total_sent', 0) for s in
               account_stats_col.find({'account_id': {'$in': ids}}, {'total_sent': 1}))

def is_group_failed(account_id, group_key):
    failed = account_failed_groups_col.find_one({'account_id': account_id, 'group_key': group_key})
    return failed is not None

def mark_group_failed(account_id, group_key, error):
    account_failed_groups_col.update_one(
        {'account_id': account_id, 'group_key': group_key},
        {'$set': {'error': str(error)[:200], 'failed_at': datetime.now()}},
        upsert=True
    )

def clear_failed_groups(account_id):
    account_failed_groups_col.delete_many({'account_id': account_id})

def get_flood_wait(account_id, group_key):
    doc = account_flood_waits_col.find_one({'account_id': account_id, 'group_key': group_key})
    if doc:
        wait_until = doc.get('wait_until')
        if wait_until and wait_until > datetime.now():
            remaining = (wait_until - datetime.now()).total_seconds()
            return int(remaining)
        else:
            account_flood_waits_col.delete_one({'account_id': account_id, 'group_key': group_key})
    return 0

def set_flood_wait(account_id, group_key, group_name, seconds):
    wait_until = datetime.now() + timedelta(seconds=seconds)
    account_flood_waits_col.update_one(
        {'account_id': account_id, 'group_key': group_key},
        {'$set': {
            'group_name': group_name,
            'wait_seconds': seconds,
            'wait_until': wait_until,
            'created_at': datetime.now()
        }},
        upsert=True
    )

def clear_flood_waits(account_id):
    account_flood_waits_col.delete_many({'account_id': account_id})

def get_active_flood_waits(account_id):
    now = datetime.now()
    return account_flood_waits_col.count_documents({
        'account_id': account_id,
        'wait_until': {'$gt': now}
    })

def generate_token(length=16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

proxy_index = 0

def get_next_proxy():
    global proxy_index
    if not PROXIES:
        return None
    proxy = PROXIES[proxy_index % len(PROXIES)]
    proxy_index += 1
    
    proxy_type = python_socks.ProxyType.SOCKS5
    if proxy['type'].lower() == 'socks4':
        proxy_type = python_socks.ProxyType.SOCKS4
    elif proxy['type'].lower() == 'http':
        proxy_type = python_socks.ProxyType.HTTP
    
    return (proxy_type, proxy['host'], proxy['port'], True, proxy.get('username'), proxy.get('password'))

def get_proxy_candidates():
    if not PROXIES:
        return [None]
    return [get_next_proxy() for _ in range(len(PROXIES))]

# ===================== Scaling: proxies, sharding, client factory =====================
# Runtime proxy pool = config PROXIES + anything from the PROXY_LIST env var.
# Format per entry (line- or ';'-separated):  type:host:port[:user:pass]
#   type in {socks5, socks4, http}. Proxies are assigned STICKILY per account so
#   an account always egresses from the same IP (Telegram dislikes IP hopping).

def _load_runtime_proxies():
    proxies = list(PROXIES) if PROXIES else []
    raw = os.getenv('PROXY_LIST', '').strip()
    if raw:
        for line in re.split(r'[\n;]+', raw):
            line = line.strip()
            if not line:
                continue
            parts = line.split(':')
            if len(parts) < 3:
                print(f"[PROXY] Ignoring malformed proxy entry: {line}")
                continue
            entry = {'type': parts[0].strip().lower(), 'host': parts[1].strip(), 'port': int(parts[2].strip())}
            if len(parts) >= 5:
                entry['username'] = parts[3].strip()
                entry['password'] = parts[4].strip()
            proxies.append(entry)
    return proxies

RUNTIME_PROXIES = _load_runtime_proxies()


def _proxy_tuple(entry):
    ptype = python_socks.ProxyType.SOCKS5
    t = str(entry.get('type', 'socks5')).lower()
    if t == 'socks4':
        ptype = python_socks.ProxyType.SOCKS4
    elif t == 'http':
        ptype = python_socks.ProxyType.HTTP
    return (ptype, entry['host'], int(entry['port']), True, entry.get('username'), entry.get('password'))


def _stable_hash(value):
    return int(hashlib.md5(str(value).encode()).hexdigest(), 16)


def _proxy_for_account(account_id):
    """Pick a sticky proxy for an account (same account -> same proxy)."""
    if not RUNTIME_PROXIES:
        return None
    idx = _stable_hash(account_id) % len(RUNTIME_PROXIES)
    try:
        return _proxy_tuple(RUNTIME_PROXIES[idx])
    except Exception as e:
        print(f"[PROXY] Failed to build proxy for {account_id}: {e}")
        return None


# Shared resilience settings for every Telethon client we open.
_CLIENT_RESILIENCE_KW = dict(
    connection_retries=None,
    retry_delay=5,
    auto_reconnect=True,
    request_retries=5,
    flood_sleep_threshold=60,
    timeout=int(os.getenv('TG_TIMEOUT', '30')),  # connect/request timeout (default Telethon is 10s)
)


def make_account_client(session, account_id=None):
    """Create a resilient Telethon client for a user account, behind that
    account's sticky proxy (if any). Use everywhere we open an account session."""
    proxy = _proxy_for_account(account_id) if account_id is not None else None
    return TelegramClient(
        StringSession(session), CONFIG['api_id'], CONFIG['api_hash'],
        proxy=proxy,
        **_CLIENT_RESILIENCE_KW,
    )


# ---- Worker sharding (lets you run multiple forwarding processes later) ----
WORKER_COUNT = max(1, int(os.getenv('WORKER_COUNT', '1')))
WORKER_ID = int(os.getenv('WORKER_ID', '0'))
BOT_ROLE = os.getenv('BOT_ROLE', 'all').strip().lower()   # all | bot | worker
RECONCILE_INTERVAL = int(os.getenv('RECONCILE_INTERVAL', '15'))
START_BATCH = int(os.getenv('START_BATCH', '25'))
# Soft cap: max accounts a single worker will run (0 = unlimited). The manager
# uses this to decide how many workers to spawn; the reconciler enforces it so a
# worker never overloads its event loop / IP.
MAX_ACCOUNTS_PER_WORKER = int(os.getenv('MAX_ACCOUNTS_PER_WORKER', '0'))
# Minimum pause between rounds when target-frequency pacing is on.
TARGET_MIN_CYCLE_FLOOR = int(os.getenv('TARGET_MIN_CYCLE_FLOOR', '30'))
# Hard floor for per-message delay (anti-burst safety; prevents flooding 100 groups
# in seconds even when the cadence is capped). Applies even to old stored values.
MIN_MSG_DELAY = int(os.getenv('MIN_MSG_DELAY', '5'))
# Live per-send logging: OFF by default (round summaries already report results).
# Turn ON (LIVE_SEND_LOGS=1) only at small scale; per-send logger calls do not
# scale (a bot FloodWaits past ~30 msgs/sec).
LIVE_SEND_LOGS = os.getenv('LIVE_SEND_LOGS', '0').strip().lower() in ('1', 'true', 'yes', 'on')
LIVE_LOG_MAX_PENDING = int(os.getenv('LIVE_LOG_MAX_PENDING', '200'))


def _owns_account(account_id):
    """Does THIS process own (forward) the given account?
    - role 'bot' never forwards.
    - otherwise, shard accounts across WORKER_COUNT by a stable hash."""
    if BOT_ROLE == 'bot':
        return False
    return (_stable_hash(account_id) % WORKER_COUNT) == (WORKER_ID % WORKER_COUNT)


def parse_link(link):
    topic_id = None
    match = re.search(r'/(\d+)$', link)
    if match:
        topic_id = int(match.group(1))
    base = re.sub(r'/\d+$', '', link).rstrip('/')
    if '/c/' in base:
        cid = base.split('/c/')[-1]
        peer = int('-100' + cid)
        url = f"https://t.me/c/{cid}"
    else:
        username = base.split('t.me/')[-1]
        peer = username
        url = f"https://t.me/{username}"
    return peer, url, topic_id


def _account_id_variants(account_id):
    """Return possible stored variants for account_id field (ObjectId vs str)."""
    return [account_id, str(account_id)]

async def delete_account_and_related(account_id):
    from bson.objectid import ObjectId

    variants = set(_account_id_variants(account_id))
    obj_id = None
    try:
        obj_id = ObjectId(str(account_id))
        variants.add(obj_id)
    except Exception:
        obj_id = None

    if obj_id is not None:
        accounts_col.delete_one({'_id': obj_id})
    else:
        accounts_col.delete_one({'_id': account_id})

    variant_list = list(variants)
    account_topics_col.delete_many({'account_id': {'$in': variant_list}})
    account_settings_col.delete_many({'account_id': {'$in': variant_list}})
    account_stats_col.delete_many({'account_id': {'$in': variant_list}})
    account_auto_groups_col.delete_many({'account_id': {'$in': variant_list}})
    account_failed_groups_col.delete_many({'account_id': {'$in': variant_list}})
    account_flood_waits_col.delete_many({'account_id': {'$in': variant_list}})
    logger_tokens_col.delete_many({'account_id': {'$in': variant_list}})

    for key in list(variants):
        if key in forwarding_tasks:
            forwarding_tasks[key].cancel()
            del forwarding_tasks[key]
        if key in auto_reply_clients:
            try:
                await auto_reply_clients[key].disconnect()
            except Exception:
                pass
            del auto_reply_clients[key]

async def safe_leave_chat(client, target):
    """Best-effort leave for channels/supergroups and basic groups.

    `target` can be an entity, username, chat id, or input peer.
    """
    if target is None:
        return False

    try:
        entity = target
        # Resolve to an entity if needed
        if isinstance(target, (str, int)):
            entity = await client.get_entity(target)

        # Channels / supergroups
        if isinstance(entity, Channel) or isinstance(entity, InputPeerChannel):
            peer = await client.get_input_entity(entity)
            await client(LeaveChannelRequest(peer))
            return True

        # Basic groups
        if isinstance(entity, Chat) or isinstance(entity, InputPeerChat):
            chat_id = entity.id if hasattr(entity, 'id') else getattr(entity, 'chat_id', None)
            await client(DeleteChatUserRequest(chat_id=chat_id, user_id='me'))
            return True

        # Fallback: try leave as channel
        peer = await client.get_input_entity(entity)
        await client(LeaveChannelRequest(peer))
        return True

    except Exception:
        return False


def _is_auto_leave_enabled(user_id: int) -> bool:
    """User-level toggle for whether bot should auto-leave groups on permanent send failures."""
    try:
        doc = get_user_cached(int(user_id))
        return bool(doc.get('auto_leave_groups', True))
    except Exception:
        return True

def _parse_time_24h(value: str):
    value = (value or "").strip()
    m = re.match(r'^([01]?\d|2[0-3]):([0-5]\d)$', value)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    return f"{hh:02d}:{mm:02d}"

def _get_quiet_hours_wait(user_id: int, user_doc=None):
    if user_doc is None:
        user_doc = get_user_cached(user_id)
    q = user_doc.get('quiet_hours') or {}
    if not q.get('enabled'):
        return 0, None
    start = _parse_time_24h(q.get('start'))
    end = _parse_time_24h(q.get('end'))
    if not start or not end:
        return 0, None
    label = q.get('label') or f"{start}-{end}"

    now = datetime.now()
    start_h, start_m = map(int, start.split(':'))
    end_h, end_m = map(int, end.split(':'))
    start_dt = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end_dt = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)

    start_min = start_h * 60 + start_m
    end_min = end_h * 60 + end_m
    now_min = now.hour * 60 + now.minute

    if start_min <= end_min:
        if now_min < start_min or now_min >= end_min:
            return 0, label
        wait_secs = int((end_dt - now).total_seconds())
        return max(wait_secs, 0), label
    else:
        if now_min >= start_min:
            end_dt = end_dt + timedelta(days=1)
            wait_secs = int((end_dt - now).total_seconds())
            return max(wait_secs, 0), label
        if now_min < end_min:
            wait_secs = int((end_dt - now).total_seconds())
            return max(wait_secs, 0), label
        return 0, label

async def _interruptible_round_sleep(total_seconds, account_id, check_interval=15):
    """Sleep up to total_seconds while still honoring stop requests.

    Stopping a broadcast cancels this task, so asyncio cancellation is the primary
    (instant) stop mechanism. The periodic is_forwarding check is only a fallback
    for flag-based stops. This replaces the old per-second DB poll, cutting idle
    DB load by ~check_interval x per active account (the main cause of lag at
    scale)."""
    remaining = int(max(0, total_seconds))
    while remaining > 0:
        step = min(int(check_interval), remaining)
        await asyncio.sleep(step)
        remaining -= step
        acc = get_account_by_id(account_id)
        if not acc or not acc.get('is_forwarding', False):
            return False
    return True


async def _sleep_quiet_hours(wait_seconds: int, account_id):
    remaining = int(wait_seconds)
    while remaining > 0:
        acc = get_account_by_id(account_id)
        if not acc or not acc.get('is_forwarding'):
            return False
        step = min(60, remaining)
        await asyncio.sleep(step)
        remaining -= step
    return True

def _format_duration(seconds: int) -> str:
    seconds = int(max(0, seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m"
    return f"{seconds}s"

def _quiet_hours_label_from_doc(user_doc: dict) -> str:
    q = user_doc.get('quiet_hours') or {}
    if not q.get('enabled'):
        return "Off"
    start = _parse_time_24h(q.get('start'))
    end = _parse_time_24h(q.get('end'))
    if not start or not end:
        return "Off"
    return f"{start}-{end}"


def remove_group_from_db(account_id, target_type, group_key, data=None):
    """Remove a group/topic target permanently from DB for this account."""
    try:
        # Clear failure/flood tracking too
        account_failed_groups_col.delete_one({'account_id': account_id, 'group_key': group_key})
        account_flood_waits_col.delete_one({'account_id': account_id, 'group_key': group_key})

        if target_type == 'topic':
            data = data or {}
            link = data.get('url') or data.get('link') or group_key
            # Backwards compatibility: some docs might store as url
            account_topics_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}, '$or': [{'link': link}, {'url': link}]})
            return True

        if target_type == 'auto':
            data = data or {}
            gid = data.get('group_id')
            if gid is None:
                try:
                    gid = int(str(group_key))
                except Exception:
                    gid = None
            q = {'account_id': account_id}
            if gid is not None:
                q['group_id'] = gid
            else:
                q['group_id'] = {'$exists': True}
            account_auto_groups_col.delete_many(q)
            return True

        return False
    except Exception:
        return False


async def notify_auto_left(account_id, phone, group_name, group_key, reason=None):
    """Send logger notification when a group is auto-left."""
    try:
        reason_txt = f"\nReason: {reason}" if reason else ""
        msg = (
            "🚪 <b>Auto Left Group</b>\n"
            f"Phone: <code>{_h(phone or 'Unknown')}</code>\n"
            f"Group: <code>{_h(group_name or 'Unknown')}</code>\n"
            f"Key: <code>{_h(str(group_key))}</code>"
            f"{reason_txt}"
        )
        await send_log(account_id, msg)
    except Exception:
        pass


async def _get_user_logs_chat_id_for_account(account_id):
    """Logs are configured once per USER and apply to all their accounts."""
    key = str(account_id)
    now = time.monotonic()
    with _cache_lock:
        hit = _logs_chat_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    result = None
    try:
        acc = accounts_col.find_one({'_id': account_id}, {'owner_id': 1})
        if acc:
            owner_id = acc.get('owner_id')
            if owner_id:
                user_doc = users_col.find_one({'user_id': int(owner_id)}, {'logs_chat_id': 1})
                if user_doc:
                    result = user_doc.get('logs_chat_id')
    except Exception:
        result = None
    with _cache_lock:
        _logs_chat_cache[key] = (now + _LOGS_CACHE_TTL, result)
    return result

def _is_logmode_active_for_owner(owner_id: int) -> bool:
    try:
        user_doc = users_col.find_one({'user_id': int(owner_id)}, {'logmode_until': 1})
        if not user_doc:
            return False
        until = user_doc.get('logmode_until')
        if isinstance(until, datetime) and until > datetime.now():
            return True
    except Exception:
        return False
    return False


async def _tg_send_http(token, chat_id, text, url_button=None):
    """Send a message via the Telegram Bot HTTP API in a worker thread. Stateless,
    so it's safe to call from ANY worker process — no shared Telethon bot
    connection / getUpdates conflict (which broke multi-worker logging)."""
    payload = {
        'chat_id': int(chat_id),
        'text': text,
        'parse_mode': 'HTML',
        'disable_web_page_preview': True,
    }
    if url_button:
        label, url = url_button
        payload['reply_markup'] = {'inline_keyboard': [[{'text': label, 'url': url}]]}

    def _post():
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload, timeout=15)
        except Exception as e:
            print(f"[LOG HTTP] {e}")
    await db_call(_post)


async def send_log(account_id, message, view_link=None, group_name=None, delay_sec=None):
    """Send logs to the user's logs chat via the logger bot's HTTP API.
    Stateless and multi-worker safe (workers no longer need a logger Telethon client)."""
    try:
        token = CONFIG.get('logger_bot_token')
        if not token:
            return
        chat_id = await _get_user_logs_chat_id_for_account(account_id)
        if not chat_id:
            return
        acc = await db_call(get_account_by_id, account_id)
        owner_id = acc.get('owner_id') if acc else None
        phone = acc.get('phone') if acc else None
        is_owner_admin = bool(owner_id and is_admin(owner_id))
        allow_view = is_owner_admin or bool(owner_id and _is_logmode_active_for_owner(owner_id))

        if view_link and group_name:
            account_text = ""
            if is_owner_admin:
                account_text = f"\n<b>Account:</b> <code>{_h(phone or 'Unknown')}</code>"
            delay_text = ""
            if delay_sec is not None and is_owner_admin:
                delay_text = f"\n<b>Delay:</b> <code>{delay_sec:.2f}s</code>"
            full_msg = f"<b>Sent to {_h(group_name)}</b>{account_text}{delay_text}"
            await _tg_send_http(token, chat_id, full_msg,
                                url_button=("View Message", view_link) if allow_view else None)
        elif message:
            msg_text = str(message) if not isinstance(message, str) else message
            await _tg_send_http(token, chat_id, msg_text)
    except Exception as e:
        print(f"[LOG ERROR] {e}")

async def add_user_log(user_id, log_msg):
    # `recent_logs` is never read or shown anywhere, so we no longer persist a
    # per-user activity log to MongoDB (it was pure write load on hot user docs).
    # A cheap stdout line keeps the info in server logs at no DB/RAM cost.
    print(f"[USERLOG {user_id}] {log_msg}")


# Bounded fire-and-forget live logging: never let a slow/flood-limited logger bot
# stall forwarding. Beyond LIVE_LOG_MAX_PENDING in-flight logs we simply drop them.
_pending_live_logs = 0

async def _fire_live_log(account_id, view_link, group_name, send_gap):
    global _pending_live_logs
    if _pending_live_logs >= LIVE_LOG_MAX_PENDING:
        return
    _pending_live_logs += 1
    try:
        await send_log(account_id, None, view_link=view_link, group_name=group_name, delay_sec=send_gap)
    except Exception:
        pass
    finally:
        _pending_live_logs -= 1


async def run_forwarding_loop(user_id, account_id):
    print(f"[FORWARDING] Starting loop for account {account_id}")
    client = None
    
    try:
        acc = accounts_col.find_one({'_id': account_id})
        if not acc:
            print(f"[FORWARDING] Account {account_id} not found")
            return
        
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        client = make_account_client(session, account_id)
        await client.connect()
        
        if not await client.is_user_authorized():
            print(f"[FORWARDING] Account {account_id} not authorized - disabling")
            try:
                accounts_col.update_one(
                    {'_id': account_id},
                    {'$set': {'is_forwarding': False, 'auth_invalid': True}}
                )
            except Exception:
                pass
            try:
                await send_log(
                    account_id,
                    "<b>⚠ Session no longer authorized</b>\n\n"
                    "<i>Forwarding stopped for this account. Please re-login to continue.</i>"
                )
            except Exception:
                pass
            return
        
        print(f"[FORWARDING] Client connected for account {account_id}")
        
        # Attach auto-reply handler to the SAME client (best practice)
        owner_id = acc.get('owner_id')
        user = get_user(owner_id)
        if user.get('autoreply_enabled', False):
            # Only use custom message - no default fallback
            settings_doc = account_settings_col.find_one({'account_id': str(account_id)})
            
            reply_text = None
            if settings_doc and 'auto_reply' in settings_doc:
                reply_text = settings_doc.get('auto_reply')
            
            if reply_text:
                @client.on(events.NewMessage(incoming=True))
                async def autoreply_handler(event):
                    # ONLY private messages
                    if not event.is_private:
                        return
                    
                    # Ignore bots
                    if isinstance(event.sender, User) and event.sender.bot:
                        return
                    
                    try:
                        await event.reply(reply_text)
                        
                        # Track auto-reply in stats (fire-and-forget; never block the
                        # reply path / worker loop on a DB write).
                        asyncio.create_task(db_call(
                            account_stats_col.update_one,
                            {'account_id': str(account_id)},
                            {'$inc': {'auto_replies': 1}},
                            upsert=True
                        ))
                        
                        print(f"[AUTO-REPLY] Replied to {event.sender_id} with: {reply_text[:30]}...")
                    except Exception as e:
                        print(f"[AUTO-REPLY ERROR] {e}")
                
                print(f"[AUTO-REPLY] Attached to account {account_id} with message: {reply_text[:30]}...")
        
        round_num = 0
        entity_cache = {}  # group_key -> resolved Telethon entity (reused across rounds)
        while True:
            try:
                round_num += 1
                # Connection watchdog: if the link dropped (timeout/network), bring it
                # back before doing work. auto_reconnect usually handles this; this is a
                # belt-and-suspenders check that prevents "stuck disconnected" loops.
                if not client.is_connected():
                    try:
                        await client.connect()
                        print(f"[FORWARDING] Reconnected account {account_id}")
                    except Exception as e:
                        _health_bump('timeouts')
                        print(f"[FORWARDING] Reconnect failed {account_id}: {str(e)[:80]}; retrying")
                        await asyncio.sleep(15)
                        continue
                acc = await db_call(accounts_col.find_one, {'_id': account_id})
                if not acc or not acc.get('is_forwarding'):
                    print(f"[FORWARDING] Account {account_id} stopped")
                    break
                
                user = get_user_cached(user_id)
                tier_settings = get_user_tier_settings(user_id)
                fwd_mode = user.get('forwarding_mode', 'topics')
                
                # Use user-level interval presets (including custom)
                preset = user.get('interval_preset', 'medium')
                if preset == 'custom' and user.get('custom_interval'):
                    custom = user.get('custom_interval', {})
                    msg_delay = custom.get('msg_delay', 30)
                    round_delay = custom.get('round_delay', 600)
                else:
                    interval_data = INTERVAL_PRESETS.get(preset, INTERVAL_PRESETS['medium'])
                    msg_delay = interval_data.get('msg_delay', tier_settings['msg_delay'])
                    round_delay = interval_data.get('round_delay', tier_settings['round_delay'])
                
                # Safety floor: never burst faster than MIN_MSG_DELAY between sends,
                # even if an old/custom value is lower.
                try:
                    msg_delay = max(MIN_MSG_DELAY, int(msg_delay))
                except Exception:
                    msg_delay = MIN_MSG_DELAY
                
                # ===================== Ads Source (Ads Mode) =====================
                ads_mode = user.get('ads_mode', 'saved')

                ads = []
                custom_text = None
                post_source_entity = None
                post_source_msg_id = None
                post_source_input_peer = None

                if ads_mode == 'custom':
                    custom_text = (user.get('ads_custom_message') or '').strip()
                    if not custom_text:
                        print(f"[FORWARDING] Custom message not set for {account_id}")
                        await add_user_log(user_id, "Custom message not set - Settings → Ads Mode → Set Custom Message")
                        await asyncio.sleep(60)
                        continue
                    ads = [None]
                    print(f"[FORWARDING] Using Custom Message mode")

                elif ads_mode == 'post':
                    link = (user.get('ads_post_link') or '').strip()
                    if not link:
                        print(f"[FORWARDING] Post link not set for {account_id}")
                        await add_user_log(user_id, "Post link not set - Settings → Ads Mode → Set Post Link")
                        await asyncio.sleep(60)
                        continue

                    try:
                        tail = link.replace('https://t.me/', '')
                        parts = [p for p in tail.split('/') if p]
                        if parts and parts[0] == 'c' and len(parts) >= 3:
                            cid = parts[1]
                            post_source_entity = int('-100' + str(cid))
                            post_source_msg_id = int(parts[2])
                        else:
                            post_source_entity = parts[0]
                            post_source_msg_id = int(parts[-1])

                        _m = await client.get_messages(post_source_entity, ids=post_source_msg_id)
                        if not _m:
                            raise Exception('Message not found / no access')
                        post_source_input_peer = await client.get_input_entity(post_source_entity)
                        ads = [None]
                        print(f"[FORWARDING] Using Post Link mode")
                    except Exception as e:
                        print(f"[FORWARDING] Invalid post link: {e}")
                        await add_user_log(user_id, f"Invalid post link or no access: {str(e)[:120]}")
                        await asyncio.sleep(60)
                        continue

                else:
                    async for msg in client.iter_messages('me', limit=10):
                        if msg.text or msg.media:
                            ads.append(msg)
                    ads.reverse()

                    if not ads:
                        print(f"[FORWARDING] No ads in Saved Messages for {account_id}")
                        await add_user_log(user_id, "No ads in Saved Messages - add messages to Saved Messages")
                        await asyncio.sleep(60)
                        continue

                    print(f"[FORWARDING] Loaded {len(ads)} ads from Saved Messages")
                
                # Round start log so user can confirm next round started
                try:
                    # Get user settings for display
                    user_doc = get_user_cached(user_id)
                    
                    # Fix mode display to show user-friendly text
                    if fwd_mode == 'topics':
                        mode_display = "Topics Only"
                    elif fwd_mode == 'auto':
                        mode_display = "Groups Only"
                    elif fwd_mode == 'both':
                        mode_display = "Topics & Groups"
                    else:
                        mode_display = fwd_mode.capitalize()
                    
                    # Get auto leave and auto reply status
                    auto_leave = "✅ ON" if user_doc.get('auto_leave_groups', True) else "❌ OFF"
                    auto_reply = "✅ ON" if user_doc.get('auto_reply_enabled', False) else "❌ OFF"
                    
                    log_msg = (
                        f"<b>🔄 Starting Round</b>\n\n"
                        f"<b>Mode:</b> <code>{mode_display}</code>\n"
                        f"<b>Ads Mode:</b> <code>{ads_mode.upper()}</code>\n"
                        f"<b>Auto Leave:</b> {auto_leave}\n"
                        f"<b>Auto Reply:</b> {auto_reply}"
                    )
                    await send_log(account_id, log_msg)
                except Exception:
                    pass

                groups_to_forward = []
                
                acc_id_str = str(account_id)
                
                # Preload per-account failure + flood-wait state ONCE per round so we
                # don't fire a blocking DB query for every single group (this was a
                # major source of event-loop blocking for accounts with many groups).
                _now = datetime.now()
                failed_set = set()
                flood_map = {}
                try:
                    def _load_round_state():
                        fset = {d.get('group_key') for d in account_failed_groups_col.find(
                            {'account_id': acc_id_str}, {'group_key': 1})}
                        fmap = {d.get('group_key'): d.get('wait_until') for d in account_flood_waits_col.find(
                            {'account_id': account_id, 'wait_until': {'$gt': _now}}, {'group_key': 1, 'wait_until': 1})}
                        return fset, fmap
                    failed_set, flood_map = await db_call(_load_round_state)
                except Exception as _e:
                    print(f"[FORWARDING] Preload round state failed: {_e}")
                
                if fwd_mode in ('topics', 'both'):
                    topic_groups = await db_call(lambda: list(account_topics_col.find({'account_id': acc_id_str})))
                    if not topic_groups:
                        topic_groups = await db_call(lambda: list(account_topics_col.find({'account_id': {'$in': _account_id_variants(account_id)}})))
                    
                    for tg in topic_groups:
                        link = tg.get('link') or tg.get('url')
                        if link and 't.me/' in link:
                            if '?' in link:
                                link = link.split('?')[0]
                            peer, url, topic_id = parse_link(link)
                            group_key = link
                            if group_key not in failed_set:
                                groups_to_forward.append({
                                    'peer': peer,
                                    'url': url,
                                    'topic_id': topic_id,
                                    'title': tg.get('title', link.split('/')[-2] if '/' in link else 'Unknown'),
                                    'type': 'topic',
                                    'key': group_key
                                })
                    print(f"[FORWARDING] Added {len(groups_to_forward)} topic groups")
                
                if fwd_mode in ('auto', 'both'):
                    auto_limit = get_user_auto_group_limit(user_id)
                    count = 0
                    auto_groups = await db_call(lambda: list(account_auto_groups_col.find(
                        {'account_id': {'$in': _account_id_variants(account_id)}}
                    ).sort('_id', 1)))
                    
                    for ag in auto_groups:
                        group_key = str(ag['group_id'])
                        if group_key not in failed_set:
                            groups_to_forward.append({
                                'group_id': ag['group_id'],
                                'access_hash': ag.get('access_hash'),
                                'username': ag.get('username'),
                                'title': ag.get('title', 'Unknown'),
                                'type': 'auto',
                                'key': group_key
                            })
                            count += 1
                            if auto_limit and count >= auto_limit:
                                break
                    print(f"[FORWARDING] Added {count} auto groups")
                
                if not groups_to_forward:
                    print(f"[FORWARDING] No groups to forward to")
                    await add_user_log(user_id, "No groups configured - waiting")
                    await asyncio.sleep(60)
                    continue
                
                # ---- Target-frequency pacing -------------------------------
                # Frequency is a GLOBAL admin-managed policy (default 3/hr, max 3).
                # Auto-compute the cycle delay so each group is messaged ~that often,
                # regardless of how many groups this account has.
                n_groups = len(groups_to_forward)
                est_round_send = n_groups * msg_delay  # seconds (approx; msg_delay dominates)
                target_per_hour = get_effective_target_per_hour()
                target_cycle = 3600.0 / target_per_hour
                round_delay = max(TARGET_MIN_CYCLE_FLOOR, int(target_cycle - est_round_send))
                achievable = 3600.0 / max(1.0, est_round_send + round_delay)
                if est_round_send + TARGET_MIN_CYCLE_FLOOR >= target_cycle:
                    freq_note = (f" | WARN {n_groups} groups x {msg_delay}s = {est_round_send//60}m/round; "
                                 f"max ~{achievable:.1f}/hr (target {target_per_hour}/hr)")
                else:
                    freq_note = f" | ~{achievable:.1f}/hr per group (target {target_per_hour}/hr)"
                print(f"[FORWARDING] {account_id}: {n_groups} groups, msg_delay={msg_delay}s, "
                      f"round_delay={round_delay}s{freq_note}")

                sent = 0
                failed = 0
                skipped = 0
                stats_failed = 0
                peerflood_hit = False
                last_send_started_at = None
                
                for i, group in enumerate(groups_to_forward):
                    # Re-check the stop flag only periodically. Stopping cancels this
                    # task (instant), so this DB read is just a fallback; doing it every
                    # 10th group instead of every group avoids hundreds of blocking
                    # queries per round when an account has many groups.
                    if i % 10 == 0:
                        fresh_acc = await db_call(accounts_col.find_one, {'_id': account_id})
                        if not fresh_acc or not fresh_acc.get('is_forwarding'):
                            break
                        acc = fresh_acc

                    wait_seconds, quiet_label = _get_quiet_hours_wait(user_id, user)
                    if wait_seconds > 0:
                        try:
                            wait_text = _format_duration(wait_seconds)
                            quiet_label = quiet_label or "Quiet Hours"
                            await send_log(
                                account_id,
                                "<b>Quiet Hours Active</b>\n\n"
                                f"<b>Window:</b> <code>{quiet_label}</code>\n"
                                f"<b>Pausing:</b> <code>{wait_text}</code>"
                            )
                        except Exception:
                            pass
                        ok = await _sleep_quiet_hours(wait_seconds, account_id)
                        if not ok:
                            break
                    
                    group_key = group.get('key', group.get('title', 'unknown'))
                    _wait_until = flood_map.get(group_key)
                    if _wait_until and _wait_until > datetime.now():
                        skipped += 1
                        remaining_m = int((_wait_until - datetime.now()).total_seconds()) // 60
                        print(f"[FORWARDING] Skipped {group['title']} (flood wait: {remaining_m}m)")
                        continue
                    
                    msg = ads[i % len(ads)] if ads_mode == 'saved' else None
                    
                    try:
                        sent_msg_id = None
                        current_entity = None
                        current_topic_id = None
                        send_gap = None
                        send_started = None
                        
                        if group['type'] == 'topic':
                            peer = group['peer']
                            current_topic_id = group.get('topic_id')
                            current_entity = entity_cache.get(group_key)
                            
                            if current_entity is None:
                                try:
                                    if isinstance(peer, str):
                                        current_entity = await client.get_entity(peer)
                                    elif isinstance(peer, int):
                                        if peer > 0:
                                            peer = int('-100' + str(peer))
                                        current_entity = await client.get_entity(peer)
                                except:
                                    pass
                                if current_entity is not None:
                                    entity_cache[group_key] = current_entity
                            
                            if current_entity is None:
                                raise Exception(f"Cannot resolve topic peer: {peer}")
                            
                            group_name = getattr(current_entity, 'title', group['title'])[:30]
                            
                            if ads_mode == 'custom':
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                if current_topic_id:
                                    r = await client.send_message(current_entity, custom_text, reply_to=current_topic_id)
                                else:
                                    r = await client.send_message(current_entity, custom_text)
                                sent_msg_id = getattr(r, 'id', None)

                            elif ads_mode == 'post':
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                if current_topic_id:
                                    sent_msg_id = await forward_message(client, current_entity, post_source_msg_id, post_source_input_peer, current_topic_id)
                                else:
                                    result = await client.forward_messages(current_entity, post_source_msg_id, post_source_entity)
                                    if result:
                                        if isinstance(result, list):
                                            sent_msg_id = result[0].id if len(result) > 0 else None
                                        else:
                                            sent_msg_id = result.id

                            else:
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                if current_topic_id:
                                    sent_msg_id = await forward_message(client, current_entity, msg.id, msg.peer_id, current_topic_id)
                                else:
                                    result = await client.forward_messages(current_entity, msg.id, 'me')
                                    if result:
                                        if isinstance(result, list):
                                            sent_msg_id = result[0].id if len(result) > 0 else None
                                        else:
                                            sent_msg_id = result.id
                        else:
                            current_entity = entity_cache.get(group_key)
                            group_id = group['group_id']
                            
                            if current_entity is None and group.get('username'):
                                try:
                                    current_entity = await client.get_entity(group['username'])
                                except:
                                    pass
                            
                            if current_entity is None:
                                try:
                                    full_id = int('-100' + str(abs(group_id))) if group_id > 0 else group_id
                                    current_entity = await client.get_entity(full_id)
                                except:
                                    pass
                            
                            if current_entity is None and group.get('access_hash'):
                                try:
                                    current_entity = InputPeerChannel(channel_id=abs(group_id), access_hash=group['access_hash'])
                                except:
                                    pass
                            
                            if current_entity is None:
                                raise Exception(f"Cannot resolve entity for group {group_id}")
                            
                            entity_cache[group_key] = current_entity
                            group_name = group['title'][:30]

                            if ads_mode == 'custom':
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                r = await client.send_message(current_entity, custom_text)
                                sent_msg_id = getattr(r, 'id', None)

                            elif ads_mode == 'post':
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                result = await client.forward_messages(current_entity, post_source_msg_id, post_source_entity)
                                if result:
                                    if isinstance(result, list):
                                        sent_msg_id = result[0].id if len(result) > 0 else None
                                    else:
                                        sent_msg_id = result.id

                            else:
                                send_started = time.monotonic()
                                if last_send_started_at is not None:
                                    send_gap = send_started - last_send_started_at
                                result = await client.forward_messages(current_entity, msg.id, 'me')
                                if result:
                                    if isinstance(result, list):
                                        sent_msg_id = result[0].id if len(result) > 0 else None
                                    else:
                                        sent_msg_id = result.id
                        
                        sent += 1
                        if send_started is not None:
                            last_send_started_at = send_started
                        print(f"[FORWARDING] Sent to {group_name} ({i+1}/{len(groups_to_forward)})")
                        # Per-send user logs are batched into the round summary (end
                        # of loop) instead of a DB write on every message.
                        # Live "sent to X" log is fire-and-forget + bounded so a slow
                        # or flood-limited logger bot never throttles forwarding.
                        if LIVE_SEND_LOGS and sent_msg_id and current_entity:
                            view_link = build_message_link(current_entity, sent_msg_id, current_topic_id)
                            if view_link:
                                asyncio.create_task(_fire_live_log(account_id, view_link, group_name, send_gap))
                        
                        # Stats are flushed once per round (see end of loop) to avoid
                        # a blocking DB write on every single message.
                        
                    except FloodWaitError as e:
                        wait_time = e.seconds
                        failed += 1
                        await db_call(set_flood_wait, account_id, group_key, group['title'], wait_time)
                        _health_bump('floods')
                        print(f"[FORWARDING] FloodWait {wait_time // 60}m for {group['title']} - will skip until expires")
                        await add_user_log(user_id, f"FloodWait {wait_time // 60}m in {group['title'][:20]}")
                        
                    except (ChannelPrivateError, ChatWriteForbiddenError, UserBannedInChannelError) as e:
                        failed += 1
                        entity_cache.pop(group_key, None)
                        await db_call(mark_group_failed, account_id, group_key, str(e))
                        print(f"[FORWARDING] Permanent fail {group['title']}: {type(e).__name__}")

                        # Auto-leave the group if sending fails (only if enabled)
                        if _is_auto_leave_enabled(user_id):
                            try:
                                if current_entity is not None:
                                    left_ok = await safe_leave_chat(client, current_entity)
                                    if left_ok:
                                        remove_group_from_db(acc_id_str, group.get('type'), group_key, group)
                                        await notify_auto_left(account_id, acc.get('phone'), group.get('title'), group_key, reason=type(e).__name__)
                                    await add_user_log(user_id, f"Auto-left {group['title'][:20]} after failure")
                            except Exception as le:
                                print(f"[FORWARDING] Leave failed: {str(le)[:80]}")
                        else:
                            await add_user_log(user_id, f"Auto-leave disabled; kept {group['title'][:20]}")
                        
                    except SlowModeWaitError as e:
                        failed += 1
                        wait_time = int(getattr(e, 'seconds', 60) or 60)
                        await db_call(set_flood_wait, account_id, group_key, group['title'], wait_time)
                        print(f"[FORWARDING] SlowMode {wait_time}s for {group['title']}")
                        await add_user_log(user_id, f"SlowMode {wait_time}s in {group['title'][:20]}")

                    except PeerFloodError:
                        # Account-wide spam limit (Telegram flagged too many messages to
                        # new peers). Stop this round immediately and cool down hard rather
                        # than risk an account ban.
                        failed += 1
                        peerflood_hit = True
                        _health_bump('peerfloods')
                        print(f"[FORWARDING] PeerFlood for account {account_id} - cooling down")
                        await add_user_log(user_id, "PeerFlood detected - pausing this account to stay safe")
                        try:
                            await send_log(
                                account_id,
                                "<b>⚠ PeerFlood detected</b>\n\n"
                                "<i>Telegram flagged this account for sending too fast. "
                                "Pausing it for a while to avoid a ban.</i>"
                            )
                        except Exception:
                            pass
                        break

                    except Exception as e:
                        failed += 1
                        entity_cache.pop(group_key, None)
                        error_str = str(e)
                        wait_match = re.search(r'wait of (\d+) seconds', error_str, re.IGNORECASE)
                        if wait_match:
                            wait_time = int(wait_match.group(1))
                            await db_call(set_flood_wait, account_id, group_key, group['title'], wait_time)
                        else:
                            print(f"[FORWARDING] Error {group['title']}: {error_str[:50]}")

                            # Auto-leave on any non-flood send failure (only if enabled)
                            if _is_auto_leave_enabled(user_id):
                                try:
                                    if current_entity is not None:
                                        left_ok = await safe_leave_chat(client, current_entity)
                                        if left_ok:
                                            remove_group_from_db(acc_id_str, group.get('type'), group_key, group)
                                            await notify_auto_left(account_id, acc.get('phone'), group.get('title'), group_key, reason=error_str[:120])
                                        await add_user_log(user_id, f"Auto-left {group['title'][:20]} after failure")
                                except Exception as le:
                                    print(f"[FORWARDING] Leave failed: {str(le)[:80]}")
                            else:
                                await add_user_log(user_id, f"Auto-leave disabled; kept {group['title'][:20]}")

                        # Counted here; stats are flushed once per round below.
                        stats_failed += 1
                    
                    # Small random jitter so many accounts don't hit Telegram in
                    # lockstep (reduces synchronized bursts -> fewer flood errors).
                    await asyncio.sleep(msg_delay + random.uniform(0, max(1.0, msg_delay * 0.25)))
                
                # Flush accumulated stats once per round (1 write instead of N).
                if sent or stats_failed:
                    await db_call(update_account_stats, str(account_id), sent=sent, failed=stats_failed)
                _health_bump('sends', sent)
                _health_bump('fails', stats_failed)
                
                print(f"[FORWARDING] Round complete. Sent: {sent}, Failed: {failed}, Skipped: {skipped}")
                try:
                    await send_log(account_id, f"<b>✅ Round {round_num} Completed</b>\n\n📤 <b>Sent:</b> <code>{sent}</code> | ❌ <b>Failed:</b> <code>{failed}</code> | ⏭ <b>Skipped:</b> <code>{skipped}</code>\n\n⏰ <b>Next Round:</b> <code>{round_delay}s</code>{freq_note}")
                except Exception:
                    pass
                await add_user_log(user_id, f"Round: {sent} sent, {failed} failed, {skipped} skipped")
                
                # Check if still forwarding before waiting for next round
                if not acc.get('is_forwarding', False):
                    print(f"[{account_id}] Stopped before round delay")
                    break
                
                # If we hit a PeerFlood this round, wait much longer than the normal
                # round delay to let Telegram's spam flag cool off.
                effective_delay = round_delay
                if peerflood_hit:
                    effective_delay = max(round_delay, int(os.getenv('PEERFLOOD_COOLDOWN', '3600')))
                    print(f"[FORWARDING] PeerFlood cooldown: waiting {effective_delay}s")

                print(f"[FORWARDING] Waiting {effective_delay}s for next round...")
                still_active = await _interruptible_round_sleep(effective_delay, account_id, check_interval=15)
                if not still_active:
                    print(f"[{account_id}] Stopped during round delay")
                    break
                
            except asyncio.CancelledError:
                print(f"[FORWARDING] Task cancelled for account {account_id}")
                break
            except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                # Transient network / Telegram connection timeout: keep the client,
                # back off briefly and retry next round (auto_reconnect + the round-start
                # check recover the link) instead of tearing the whole loop down.
                _health_bump('timeouts')
                print(f"[FORWARDING] Connection issue for {account_id}: {type(e).__name__}: {str(e)[:80]} - retrying")
                await asyncio.sleep(15)
                continue
            except Exception as e:
                print(f"[FORWARDING] Round error for {account_id}: {type(e).__name__}: {str(e)[:120]} - retrying")
                await asyncio.sleep(15)
                continue
        
    except asyncio.CancelledError:
        print(f"[FORWARDING] Task cancelled for account {account_id}")
    except Exception as e:
        print(f"[FORWARDING] Error in loop: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if client:
            try:
                await client.disconnect()
                print(f"[FORWARDING] Client disconnected for account {account_id}")
            except:
                pass
        # NOTE: task registration in forwarding_tasks is owned by supervise_forwarding,
        # which calls this loop. Do not delete it here or the supervisor loses track.

async def supervise_forwarding(user_id, account_id):
    """Keep an account's forwarding loop alive in production.

    - Restarts run_forwarding_loop with exponential backoff if it crashes
      unexpectedly (network/Telegram errors), so an account never silently stops.
    - Stops cleanly when forwarding is turned off, the task is cancelled, or the
      session becomes unauthorized (run_forwarding_loop clears is_forwarding).
    This is the task registered in forwarding_tasks; cancelling it cancels the
    whole chain instantly."""
    backoff = 5
    try:
        while True:
            acc = await db_call(get_account_by_id, account_id)
            if not acc or not acc.get('is_forwarding', False):
                break
            started_at = time.monotonic()
            try:
                await run_forwarding_loop(user_id, account_id)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[SUPERVISOR] Account {account_id} crashed: {e}")
                import traceback
                traceback.print_exc()
            # If it ran for a healthy while, reset the backoff.
            if time.monotonic() - started_at > 120:
                backoff = 5
            # Decide whether to restart.
            acc = await db_call(get_account_by_id, account_id)
            if not acc or not acc.get('is_forwarding', False):
                break
            print(f"[SUPERVISOR] Restarting account {account_id} in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 300)
    except asyncio.CancelledError:
        print(f"[SUPERVISOR] Cancelled for account {account_id}")
    finally:
        if account_id in forwarding_tasks:
            del forwarding_tasks[account_id]


async def resume_active_forwarding():
    """On startup, resume any accounts that were forwarding before the restart.
    Staggered so we don't reconnect hundreds of sessions all at once."""
    try:
        active = await db_call(lambda: list(accounts_col.find({'is_forwarding': True})))
    except Exception as e:
        print(f"[RESUME] Could not query active accounts: {e}")
        return
    if not active:
        print("[RESUME] No active accounts to resume")
        return
    print(f"[RESUME] Resuming {len(active)} account(s)...")
    stagger = float(os.getenv('RESUME_STAGGER_SECONDS', '2'))
    resumed = 0
    for acc in active:
        acc_id = acc['_id']
        owner_id = acc.get('owner_id')
        if owner_id is None:
            continue
        existing = forwarding_tasks.get(acc_id)
        if existing and not existing.done():
            continue
        task = asyncio.create_task(supervise_forwarding(owner_id, acc_id))
        forwarding_tasks[acc_id] = task
        resumed += 1
        await asyncio.sleep(stagger)
    print(f"[RESUME] Started {resumed} forwarding supervisor(s)")


# ===================== Desired-state reconciliation engine =====================
# Forwarding is driven by the `is_forwarding` flag in MongoDB (the "desired
# state"). Each process runs a reconciler that ensures the accounts IT owns
# (by shard) and that should be forwarding have a running supervisor, and that
# anything it shouldn't run is stopped. This is what makes the bot horizontally
# scalable: spin up more worker processes (each with its own WORKER_ID) and they
# automatically split the load with no extra coordination code.

def ensure_account_running(owner_id, account_id):
    """Start a supervisor for this account locally if we own it and it isn't
    already running. Returns True if a task is running/was started here."""
    if owner_id is None or not _owns_account(account_id):
        return False
    existing = forwarding_tasks.get(account_id)
    if existing and not existing.done():
        return True
    forwarding_tasks[account_id] = asyncio.create_task(supervise_forwarding(owner_id, account_id))
    return True


def ensure_account_stopped(account_id):
    """Cancel the local supervisor for this account if present."""
    task = forwarding_tasks.get(account_id)
    if task and not task.done():
        task.cancel()
    forwarding_tasks.pop(account_id, None)


async def _reconcile_once():
    """One pass: align locally-running supervisors with desired DB state."""
    try:
        desired = await db_call(lambda: list(
            accounts_col.find({'is_forwarding': True}, {'_id': 1, 'owner_id': 1})
        ))
    except Exception as e:
        print(f"[RECONCILE] Query failed: {e}")
        return
    desired_owned = {}
    for acc in desired:
        acc_id = acc['_id']
        if _owns_account(acc_id) and acc.get('owner_id') is not None:
            desired_owned[acc_id] = acc['owner_id']

    # Enforce the soft per-worker cap. If this worker's shard is assigned more
    # accounts than it can safely run, deterministically run only the first
    # MAX_ACCOUNTS_PER_WORKER (sorted by id) and warn so the manager scales up.
    if MAX_ACCOUNTS_PER_WORKER and len(desired_owned) > MAX_ACCOUNTS_PER_WORKER:
        over = len(desired_owned) - MAX_ACCOUNTS_PER_WORKER
        keep = sorted(desired_owned.keys(), key=lambda x: str(x))[:MAX_ACCOUNTS_PER_WORKER]
        keep_set = set(keep)
        desired_owned = {k: v for k, v in desired_owned.items() if k in keep_set}
        print(f"[RECONCILE] OVER CAPACITY: worker {WORKER_ID} assigned "
              f"{len(keep) + over} accounts but cap is {MAX_ACCOUNTS_PER_WORKER}; "
              f"{over} not started. Add more workers/proxies.")

    # Stop anything running locally that should no longer run (or got capped out).
    for acc_id in list(forwarding_tasks.keys()):
        if acc_id not in desired_owned:
            ensure_account_stopped(acc_id)

    # Start missing ones, capped per cycle so a big fleet ramps up smoothly.
    started = 0
    for acc_id, owner_id in desired_owned.items():
        existing = forwarding_tasks.get(acc_id)
        if existing and not existing.done():
            continue
        ensure_account_running(owner_id, acc_id)
        started += 1
        if started >= START_BATCH:
            break
        await asyncio.sleep(0.2)
    if started:
        print(f"[RECONCILE] Started {started} account(s) this cycle "
              f"(worker {WORKER_ID}/{WORKER_COUNT}, role={BOT_ROLE})")


async def forwarding_reconciler():
    """Background loop that keeps local forwarding aligned with desired state."""
    print(f"[RECONCILE] Reconciler started (worker {WORKER_ID}/{WORKER_COUNT}, "
          f"role={BOT_ROLE}, interval={RECONCILE_INTERVAL}s)")
    while True:
        try:
            await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[RECONCILE] Error: {e}")
        await asyncio.sleep(RECONCILE_INTERVAL)


BOT_START_TIME = time.time()


async def health_logger():
    """Periodically print a one-line health summary for ops/monitoring."""
    interval = int(os.getenv('HEALTH_INTERVAL', '300'))
    while True:
        try:
            await asyncio.sleep(interval)
            active_local = sum(1 for t in forwarding_tasks.values() if not t.done())
            print(
                f"[HEALTH] role={BOT_ROLE} worker={WORKER_ID}/{WORKER_COUNT} "
                f"local_active_accounts={active_local} proxies={len(RUNTIME_PROXIES)} "
                f"uptime={_format_duration(int(time.time() - BOT_START_TIME))}"
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[HEALTH] {e}")


# Per-process health counters (reset each reporting window). Used by the manager
# to adapt accounts/worker when timeouts/flood spike.
_HEALTH = {'sends': 0, 'fails': 0, 'floods': 0, 'peerfloods': 0, 'timeouts': 0}


def _health_bump(key, n=1):
    try:
        _HEALTH[key] = _HEALTH.get(key, 0) + n
    except Exception:
        pass


async def health_reporter():
    """Write a per-worker health snapshot to Mongo so the manager can adapt the
    per-worker cap (e.g. shrink it when connection timeouts spike, alert on flood)."""
    interval = int(os.getenv('HEALTH_REPORT_INTERVAL', '30'))
    while True:
        try:
            await asyncio.sleep(interval)
            window = dict(_HEALTH)
            for k in _HEALTH:
                _HEALTH[k] = 0
            active_local = sum(1 for t in forwarding_tasks.values() if not t.done())
            doc = {
                'worker_id': WORKER_ID,
                'role': BOT_ROLE,
                'updated_at': datetime.now(),
                'active_accounts': active_local,
                'window_seconds': interval,
                'sends': window.get('sends', 0),
                'fails': window.get('fails', 0),
                'floods': window.get('floods', 0),
                'peerfloods': window.get('peerfloods', 0),
                'timeouts': window.get('timeouts', 0),
            }
            await db_call(lambda: worker_health_col.update_one(
                {'worker_id': WORKER_ID}, {'$set': doc}, upsert=True))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[HEALTH] reporter error: {e}")


async def forward_message(client, to_entity, msg_id, from_peer, topic_id=None):
    random_id = random.randint(1, 2147483647)
    result = await client(ForwardMessagesRequest(
        from_peer=from_peer,
        id=[msg_id],
        random_id=[random_id],
        to_peer=to_entity,
        top_msg_id=topic_id
    ))
    if result.updates:
        for update in result.updates:
            if hasattr(update, 'message') and hasattr(update.message, 'id'):
                return update.message.id
    return None

def build_message_link(entity, msg_id, topic_id=None):
    username = getattr(entity, 'username', None)
    if username:
        base = f"https://t.me/{username}"
    else:
        chat_id = getattr(entity, 'id', None)
        if chat_id:
            base = f"https://t.me/c/{chat_id}"
        else:
            return None
    
    if topic_id:
        return f"{base}/{topic_id}/{msg_id}" if msg_id else f"{base}/{topic_id}"
    return f"{base}/{msg_id}" if msg_id else base

async def refresh_account_groups(client, account_id):
    """Refresh groups for an account and return count of groups found."""
    try:
        dialogs = await client.get_dialogs(limit=None)
        groups = []
        for d in dialogs:
            e = d.entity
            if isinstance(e, User):
                continue
            if not isinstance(e, (Channel, Chat)):
                continue
            if isinstance(e, Channel) and e.broadcast:
                continue
            title = getattr(e, 'title', 'Unknown')
            if title and title != 'Unknown':
                group_id = e.id
                access_hash = getattr(e, 'access_hash', None)
                username = getattr(e, 'username', None)
                is_channel = isinstance(e, Channel)
                
                if access_hash is None and is_channel:
                    try:
                        full_entity = await client.get_entity(e)
                        access_hash = getattr(full_entity, 'access_hash', None)
                    except:
                        pass
                
                groups.append({
                    'account_id': str(account_id),
                    'group_id': group_id,
                    'title': title,
                    'access_hash': access_hash,
                    'username': username,
                    'is_channel': is_channel
                })
        
        # Save to database (update or insert)
        for g in groups:
            account_auto_groups_col.update_one(
                {'account_id': str(account_id), 'group_id': g['group_id']},
                {'$set': g},
                upsert=True
            )
        
        return len(groups)
    except Exception as e:
        print(f"[refresh_account_groups] Error: {e}")
        return 0


async def fetch_groups_for_account(client, account_id):
    """Compatibility wrapper used by the dashboard refresh action."""
    return await refresh_account_groups(client, account_id)


async def fetch_groups(client, account_id, phone):
    try:
        dialogs = await client.get_dialogs(limit=None)
        groups = []
        for d in dialogs:
            e = d.entity
            if isinstance(e, User):
                continue
            if not isinstance(e, (Channel, Chat)):
                continue
            if isinstance(e, Channel) and e.broadcast:
                continue
            title = getattr(e, 'title', 'Unknown')
            if title and title != 'Unknown':
                group_id = e.id
                access_hash = getattr(e, 'access_hash', None)
                username = getattr(e, 'username', None)
                is_channel = isinstance(e, Channel)
                
                if access_hash is None and is_channel:
                    try:
                        full_entity = await client.get_entity(e)
                        access_hash = getattr(full_entity, 'access_hash', None)
                    except:
                        pass
                
                groups.append({
                    # store account_id consistently as string (ObjectId -> str)
                    'account_id': str(account_id),
                    'phone': phone,
                    'group_id': group_id,
                    'title': title,
                    'username': username,
                    'access_hash': access_hash,
                    'is_channel': is_channel,
                    'added_at': datetime.now()
                })
        if groups:
            # Remove any older variants (ObjectId vs str)
            account_auto_groups_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
            account_auto_groups_col.insert_many(groups)
        return len(groups)
    except Exception as e:
        print(f"Fetch groups error: {e}")
        return 0

# ===================== UI Helpers =====================

def _h(s: str) -> str:
    """Basic HTML escape for user-provided strings."""
    try:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
    except Exception:
        return ""


def ui_title(title: str) -> str:
    return f"<b>{_h(title)}</b>"


def ui_kv(key: str, val: str) -> str:
    return f"<b>{_h(key)}:</b> {_h(val)}"


def ui_section(title: str, lines: list[str]) -> str:
    body = "\n".join(lines).strip()
    return f"<b>{_h(title)}</b>\n{body}" if body else f"<b>{_h(title)}</b>"


def ui_divider() -> str:
    return "\n\n"


async def respond_with_welcome(event, text: str, buttons=None, parse_mode: str = 'html'):
    welcome_image = MESSAGES.get('welcome_image', '')
    if welcome_image:
        await event.respond(file=welcome_image, message=text, parse_mode=parse_mode, buttons=buttons)
    else:
        await event.respond(text, parse_mode=parse_mode, buttons=buttons)


def render_plan_select_text() -> str:
    return (
        "<b>Choose Your Plan</b>\n\n"
        "Pick what fits your scale. You can upgrade anytime.\n\n"
        "Plans: Kai • Super • Ultra"
    )


def render_welcome_text() -> str:
    return (
        "Welcome to Jiren Ads Bot\n\n"
        "Automate Telegram promotions across groups and topics with clean controls and stable delivery.\n\n"
        "Tap Launch Ads to get started."
    )


def render_dashboard_text(uid: int) -> str:
    user = get_user(uid)
    max_acc = get_user_max_accounts(uid)
    
    # Determine plan name and expiry
    if is_admin(uid):
        plan_name = "Admin"
        max_acc = 999
        expiry_text = "999d"
    elif is_premium(uid):
        # Use stored plan_name if available, otherwise derive from max_accounts
        plan_name = get_display_plan_name(user)
        if plan_name == "No Plan":
            # Backward compatibility: derive from max_accounts
            if max_acc >= 15:
                plan_name = "Ultra"
            elif max_acc >= 7:
                plan_name = "Super"
            else:
                plan_name = "Kai"
        
        # Calculate expiry countdown
        expires_at = user.get('premium_expires_at')
        if expires_at and isinstance(expires_at, datetime):
            remaining = expires_at - datetime.now()
            if remaining.total_seconds() > 0:
                days_left = remaining.days
                expiry_text = f"{days_left}d"
            else:
                expiry_text = "Expired"
        else:
            expiry_text = "∞"  # Legacy users without expiry
    else:
        plan_name = "No Plan"
        expiry_text = "—"
    
    accounts = get_user_accounts(uid)
    active = sum(1 for a in accounts if a.get('is_forwarding'))
    
    # Fix mode display to show user-friendly text
    mode_raw = user.get('forwarding_mode', 'topics')
    if mode_raw == 'topics':
        mode_display = "Topics Only"
    elif mode_raw == 'auto':
        mode_display = "Groups Only"
    elif mode_raw == 'both':
        mode_display = "Topics & Groups"
    else:
        mode_display = mode_raw.capitalize()
    
    # Get interval display name (Slow/Medium/Fast, not Safe/Balanced/Risky)
    preset = user.get('interval_preset', 'medium')
    preset_display = preset.capitalize()

    return (
        "<b>Dashboard</b>\n\n"
        "<b>━━━━━━━━━━━━━━━━━</b>\n\n"
        "<b>Status:</b>\n"
        f"├ <b>Plan:</b> <code>{plan_name} ({expiry_text})</code>\n"
        f"├ <b>Accounts:</b> <code>{len(accounts)}/{max_acc}</code> (Active: <code>{active}</code>)\n"
        f"├ <b>Mode:</b> <code>{mode_display}</code>\n"
        f"└ <b>Interval:</b> <code>{preset_display}</code>\n\n"
        "<b>━━━━━━━━━━━━━━━━━</b>"
    )


# ===================== Keyboards =====================

def new_welcome_keyboard():
    """New welcome screen with single Launch Ads button."""
    return [
        [Button.inline("Launch Ads", b"adsye_now")],
        [Button.url("Support", MESSAGES['support_link']), Button.url("Updates", MESSAGES['updates_link'])]
    ]

def plan_select_keyboard(user_id=None):
    """Plan selection: Kai, Super, Ultra (2x2 grid layout)."""
    user = get_user(user_id) if user_id else None
    user_plan = normalize_plan_key(user.get('plan') or user.get('plan_name')) if user else ""
    is_prem = is_premium(user_id) if user_id else False

    # Check if premium has expired
    if is_prem and user:
        expires_at = user.get('premium_expires_at')
        if expires_at and isinstance(expires_at, datetime):
            if expires_at < datetime.now():
                # Premium expired - reset to no plan
                is_prem = False
                user_plan = ""

    buttons = []

    # First row: Kai
    row1 = []
    grow_label = "✓ Kai (Active)" if user_plan == 'grow' and is_prem else f"Kai ({PLANS['grow']['price_display']})"
    row1.append(Button.inline(grow_label, b"plan_grow"))
    buttons.append(row1)

    # Second row: Super + Ultra
    prime_label = "✓ Super (Active)" if user_plan == 'prime' and is_prem else f"Super ({PLANS['prime']['price_display']})"
    dominion_label = "✓ Ultra (Active)" if user_plan == 'dominion' and is_prem else f"Ultra ({PLANS['dominion']['price_display']})"
    buttons.append([
        Button.inline(prime_label, b"plan_prime"),
        Button.inline(dominion_label, b"plan_dominion")
    ])

    # Dashboard button
    buttons.append([Button.inline("🏠 Dashboard", b"enter_dashboard")])

    return buttons

def tier_selection_keyboard():
    return [
        [Button.inline("View Plans", b"tier_premium")],
        [Button.inline("Back", b"back_start")]
    ]

def main_dashboard_keyboard(user_id):
    buttons = [
        [Button.inline("\U0001F4E3 Broadcast Menu", b"menu_broadcast")],
        [
            Button.inline("\U0001F4CB Accounts", b"menu_account"),
            Button.inline("\U0001F464 Profile", b"my_profile"),
        ],
        [Button.inline("\U0001F48E Plans", b"back_plans")],
    ]

    if is_admin(user_id):
        buttons.append([Button.inline("\u2699\uFE0F Admin", b"admin_panel")])

    return buttons

def broadcast_menu_keyboard(user_id):
    accounts = get_user_accounts(user_id)
    has_active = any(acc.get('is_forwarding') for acc in accounts)
    ads_btn = "\u23F9\uFE0F Stop Broadcast" if has_active else "\u25B6\uFE0F Start Broadcast"  # ⏹️ / ▶️
    ads_data = b"stop_all_ads" if has_active else b"start_all_ads"

    return [
        [Button.inline("\u2699\uFE0F Settings", b"menu_settings"), Button.inline("\U0001F504 Mode", b"menu_fwd_mode")],
        [Button.inline("\u23F1\uFE0F Intervals", b"menu_interval"), Button.inline("\U0001F4CA Insights", b"menu_analytics")],
        [Button.inline(ads_btn, ads_data)],
        [Button.inline("\u2190 Back", b"enter_dashboard")],
    ]

def account_list_keyboard(user_id, page=0):
    accounts = get_user_accounts(user_id)
    max_accounts = get_user_max_accounts(user_id)
    total = len(accounts)
    pages = max(1, (total + ACCOUNTS_PER_PAGE - 1) // ACCOUNTS_PER_PAGE)
    
    start = page * ACCOUNTS_PER_PAGE
    end = min(start + ACCOUNTS_PER_PAGE, total)
    page_accounts = accounts[start:end]
    
    buttons = []
    for i, acc in enumerate(page_accounts):
        idx = start + i + 1
        name = acc.get('name', 'Unknown')
        # Add emoji based on status - Green tick for active, Red X for inactive
        status_emoji = "✅" if acc.get('is_forwarding') else "❌"
        status_text = "Active" if acc.get('is_forwarding') else "Inactive"
        # Format: [Status Emoji] Status #Number - Full Name
        buttons.append([Button.inline(f"{status_emoji} {status_text} #{idx} - {name}", f"acc_{acc['_id']}")])
    
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ Prev", f"accpage_{page-1}"))
    if page < pages - 1:
        nav.append(Button.inline("➡️ Next", f"accpage_{page+1}"))
    if nav:
        buttons.append(nav)
    
    if total >= max_accounts:
        buttons.append([Button.inline("➕ Add Account 🔒", b"account_limit_reached"), Button.inline("🗑️ Delete Account", b"delete_account_menu")])
    else:
        buttons.append([Button.inline("➕ Add Account", b"add_account"), Button.inline("🗑️ Delete Account", b"delete_account_menu")])
    buttons.append([Button.inline("🔙 Back", b"enter_dashboard")])
    
    return buttons

def settings_menu_keyboard(uid):
    """Settings root menu with two categories."""
    return [
        [Button.inline("🧩 Automation Tools", b"menu_settings_automation")],
        [Button.inline("💬 Messaging & Logs", b"menu_settings_content")],
        [Button.inline("\u2190 Back", b"menu_broadcast")],  # ←
    ]

def settings_automation_keyboard(uid):
    buttons = []
    user_doc = get_user(uid)

    # Premium-only features (show locked for free users)
    if is_premium(uid):
        buttons.append([Button.inline("\U0001F504 Smart Rotation", b"menu_smart_rotation")])  # 🔄
        buttons.append([Button.inline("\U0001F465 Auto Group Join", b"menu_auto_group_join")])  # 👥
    else:
        buttons.append([Button.inline("\U0001F504 Smart Rotation 🔒", b"locked_smart_rotation")])
        buttons.append([Button.inline("\U0001F465 Auto Group Join 🔒", b"locked_auto_group_join")])

    quiet_label = _quiet_hours_label_from_doc(user_doc)
    buttons.append([Button.inline(f"Quiet Hours: {quiet_label}", b"menu_quiet_hours")])

    # Refresh All Groups - free for everyone
    buttons.append([Button.inline("🔄 Refresh All Groups", b"refresh_all_groups")])

    # Auto Leave Failed Groups toggle
    auto_leave_enabled = user_doc.get('auto_leave_groups', True)
    leave_status = "✅ ON" if auto_leave_enabled else "❌ OFF"
    buttons.append([Button.inline(f"Auto Leave Failed: {leave_status}", b"toggle_auto_leave")])

    buttons.append([Button.inline("\u2190 Back", b"menu_settings")])
    return buttons

def quiet_hours_menu(uid):
    user_doc = get_user(uid)
    q = user_doc.get('quiet_hours') or {}
    start = _parse_time_24h(q.get('start'))
    end = _parse_time_24h(q.get('end'))
    current = f"{start}-{end}" if q.get('enabled') and start and end else "Off"

    def mark(s, e):
        if q.get('enabled') and start == s and end == e:
            return " (Active)"
        return ""

    buttons = [
        [Button.inline(f"01:00 - 07:00{mark('01:00', '07:00')}", b"quiet_preset_0100_0700")],
        [Button.inline(f"00:00 - 06:00{mark('00:00', '06:00')}", b"quiet_preset_0000_0600")],
        [Button.inline(f"00:00 - 07:00{mark('00:00', '07:00')}", b"quiet_preset_0000_0700")],
        [Button.inline("Custom Interval", b"quiet_custom")],
        [Button.inline("Back", b"menu_settings_automation")],
    ]

    text = (
        "<b>Quiet Hours</b>\n\n"
        "Pause broadcasts during the selected hours.\n\n"
        f"<b>Current:</b> <code>{current}</code>"
    )
    return text, buttons

def settings_content_keyboard(uid):
    buttons = []

    # Auto Reply - Show locked for free users
    if is_premium(uid):
        buttons.append([Button.inline("\U0001F4AC Auto Reply", b"menu_autoreply")])  # 💬
    else:
        buttons.append([Button.inline("\U0001F4AC Auto Reply 🔒", b"locked_autoreply")])  # 💬🔒

    # Topics - Show locked for free users
    if is_premium(uid):
        buttons.append([Button.inline("\U0001F4C2 Topics", b"menu_topics")])  # 📂
    else:
        buttons.append([Button.inline("\U0001F4C2 Topics 🔒", b"locked_topics")])  # 📂🔒

    buttons.extend([
        [Button.inline("\U0001F4DD Logs", b"menu_logs")],            # 📝
        [Button.inline("\U0001F4E3 Ads Mode", b"menu_ads_mode")],     # 📣
    ])

    buttons.append([Button.inline("\u2190 Back", b"menu_settings")])
    return buttons

def _admin_user_detail_callback(target_id: int, source: str) -> str:
    if source == 'all':
        return f"admin_user_detail_all_{target_id}"
    return f"admin_user_detail_{target_id}"

def admin_settings_menu(target_id: int, source: str):
    user = get_user(target_id)
    mode = user.get('forwarding_mode', 'auto')
    ads_mode = user.get('ads_mode', 'saved')
    preset = user.get('interval_preset', 'medium')
    if preset == 'custom' and user.get('custom_interval'):
        custom = user.get('custom_interval', {})
        interval_display = f"Custom ({custom.get('msg_delay', 30)}s / {custom.get('round_delay', 600)}s)"
    else:
        preset_info = INTERVAL_PRESETS.get(preset, INTERVAL_PRESETS['medium'])
        interval_display = f"{preset.capitalize()} ({preset_info['msg_delay']}s / {preset_info['round_delay']}s)"

    text = (
        "<b>Admin: User Settings</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n"
        f"<b>Mode:</b> <code>{mode}</code>\n"
        f"<b>Intervals:</b> <code>{interval_display}</code>\n"
        f"<b>Ads Mode:</b> <code>{ads_mode}</code>\n\n"
        "<i>Choose what to update for this user.</i>"
    )
    buttons = [
        [Button.inline("Mode", f"admin_settings_mode_{target_id}_{source}")],
        [Button.inline("Intervals", f"admin_settings_interval_{target_id}_{source}")],
        [Button.inline("Ads Mode", f"admin_settings_ads_{target_id}_{source}")],
        [Button.inline("Back", _admin_user_detail_callback(target_id, source))]
    ]
    return text, buttons

def admin_automation_menu(target_id: int, source: str):
    user_doc = get_user(target_id)
    is_prem = is_premium(target_id)
    quiet_label = _quiet_hours_label_from_doc(user_doc)
    rotation_on = user_doc.get('smart_rotation', False)
    auto_leave_on = user_doc.get('auto_leave_groups', True)

    text = (
        "<b>Admin: Automation Tools</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n\n"
        "Manage rotation, group join, quiet hours, refresh, and auto leave."
    )

    buttons = []
    if is_prem:
        buttons.append([Button.inline(f"Smart Rotation: {'ON' if rotation_on else 'OFF'}", f"admin_toggle_smart_rotation_{target_id}_{source}")])
        buttons.append([Button.inline("Auto Group Join", f"admin_auto_group_join_{target_id}_{source}")])
    else:
        buttons.append([Button.inline("Smart Rotation 🔒", f"admin_locked_smart_rotation_{target_id}_{source}")])
        buttons.append([Button.inline("Auto Group Join 🔒", f"admin_locked_auto_group_join_{target_id}_{source}")])

    buttons.append([Button.inline(f"Quiet Hours: {quiet_label}", f"admin_quiet_hours_{target_id}_{source}")])
    buttons.append([Button.inline("🔄 Refresh All Groups", f"admin_refresh_all_{target_id}_{source}")])
    buttons.append([Button.inline(f"Auto Leave Failed: {'✅ ON' if auto_leave_on else '❌ OFF'}", f"admin_toggle_auto_leave_{target_id}_{source}")])
    buttons.append([Button.inline("Back", _admin_user_detail_callback(target_id, source))])
    return text, buttons

def admin_content_menu(target_id: int, source: str):
    user_doc = get_user(target_id)
    is_prem = is_premium(target_id)
    auto_reply_on = user_doc.get('autoreply_enabled', False)
    logs_enabled = bool(user_doc.get('logs_chat_id'))
    ads_mode = user_doc.get('ads_mode', 'saved')

    text = (
        "<b>Admin: Messaging & Logs</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n"
        f"<b>Auto Reply:</b> <code>{'ON' if auto_reply_on else 'OFF'}</code>\n"
        f"<b>Logs:</b> <code>{'Enabled' if logs_enabled else 'Disabled'}</code>\n"
        f"<b>Ads Mode:</b> <code>{ads_mode}</code>\n\n"
        "<i>Choose what to update for this user.</i>"
    )
    buttons = []
    if is_prem:
        buttons.append([Button.inline("Auto Reply", f"admin_menu_autoreply_{target_id}_{source}")])
        buttons.append([Button.inline("Topics", f"admin_menu_topics_{target_id}_{source}")])
    else:
        buttons.append([Button.inline("Auto Reply 🔒", f"admin_locked_autoreply_{target_id}_{source}")])
        buttons.append([Button.inline("Topics 🔒", f"admin_locked_topics_{target_id}_{source}")])
    buttons.append([Button.inline("Logs", f"admin_menu_logs_{target_id}_{source}")])
    buttons.append([Button.inline("Ads Mode", f"admin_settings_ads_{target_id}_{source}")])
    buttons.append([Button.inline("Back", _admin_user_detail_callback(target_id, source))])
    return text, buttons

def admin_autoreply_menu(target_id: int, source: str):
    user_doc = get_user(target_id)
    enabled = user_doc.get('autoreply_enabled', False)
    accounts = get_user_accounts(target_id)
    has_custom = False
    if accounts:
        _ids = [str(acc['_id']) for acc in accounts]
        has_custom = account_settings_col.count_documents(
            {'account_id': {'$in': _ids}, 'auto_reply': {'$nin': [None, '']}}
        ) > 0

    text = (
        "<b>Admin: Auto Reply</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n"
        f"<b>Status:</b> <code>{'ON' if enabled else 'OFF'}</code>\n"
        f"<b>Custom Reply:</b> <code>{'Set' if has_custom else 'Not Set'}</code>"
    )
    buttons = [
        [Button.inline("Toggle Auto Reply", f"admin_autoreply_toggle_{target_id}_{source}")],
        [Button.inline("Set Message", f"admin_autoreply_set_{target_id}_{source}")],
        [Button.inline("View Current", f"admin_autoreply_view_{target_id}_{source}")],
        [Button.inline("Back", f"admin_menu_content_{target_id}_{source}")],
    ]
    return text, buttons

def admin_logs_menu(target_id: int, source: str):
    user_doc = get_user(target_id)
    enabled = bool(user_doc.get('logs_chat_id'))
    status = "✅ Enabled" if enabled else "❌ Disabled"
    buttons = []
    if enabled:
        buttons.append([Button.inline("Disable Logs", f"admin_logs_disable_{target_id}_{source}")])
    else:
        buttons.append([Button.inline("Enable Logs", f"admin_logs_enable_{target_id}_{source}")])
    buttons.append([Button.inline("Back", f"admin_menu_content_{target_id}_{source}")])
    text = (
        "<b>Admin: Logs</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n"
        f"<b>Status:</b> <code>{status}</code>\n\n"
        "<i>Logs are sent to the user's logger bot chat.</i>"
    )
    return text, buttons

def admin_quiet_hours_menu(target_id: int, source: str):
    user_doc = get_user(target_id)
    q = user_doc.get('quiet_hours') or {}
    start = _parse_time_24h(q.get('start'))
    end = _parse_time_24h(q.get('end'))
    current = f"{start}-{end}" if q.get('enabled') and start and end else "Off"

    def mark(s, e):
        if q.get('enabled') and start == s and end == e:
            return " (Active)"
        return ""

    buttons = [
        [Button.inline(f"01:00 - 07:00{mark('01:00', '07:00')}", f"admin_quiet_preset_{target_id}_0100_0700_{source}")],
        [Button.inline(f"00:00 - 06:00{mark('00:00', '06:00')}", f"admin_quiet_preset_{target_id}_0000_0600_{source}")],
        [Button.inline(f"00:00 - 07:00{mark('00:00', '07:00')}", f"admin_quiet_preset_{target_id}_0000_0700_{source}")],
        [Button.inline("Custom Interval", f"admin_quiet_custom_{target_id}_{source}")],
        [Button.inline("Back", f"admin_settings_automation_{target_id}_{source}")],
    ]
    text = (
        "<b>Admin: Quiet Hours</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n\n"
        f"<b>Current:</b> <code>{current}</code>"
    )
    return text, buttons
def admin_mode_menu(target_id: int, source: str):
    user = get_user(target_id)
    current = user.get('forwarding_mode', 'auto')
    modes = {
        'topics': 'Forward to Topics Only',
        'auto': 'Forward to Groups Only',
        'both': 'Forward to Both (Topics first, then Groups)'
    }
    buttons = []
    for mode, label in modes.items():
        mark = " ✓" if mode == current else ""
        buttons.append([Button.inline(f"{label}{mark}", f"admin_set_mode_{target_id}_{mode}_{source}")])
    buttons.append([Button.inline("Back", f"admin_set_settings_{target_id}_{source}")])
    text = (
        "<b>Admin: Set Mode</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n\n"
        "Select how ads should be forwarded for this user."
    )
    return text, buttons

def admin_interval_menu(target_id: int, source: str):
    user = get_user(target_id)
    current = user.get('interval_preset', 'medium')

    def mark_for(key: str) -> str:
        return " ✓" if key == current else ""

    slow = Button.inline(f"{INTERVAL_PRESETS['slow']['name']}{mark_for('slow')}", f"admin_set_interval_{target_id}_slow_{source}")
    medium = Button.inline(f"{INTERVAL_PRESETS['medium']['name']}{mark_for('medium')}", f"admin_set_interval_{target_id}_medium_{source}")
    fast = Button.inline(f"{INTERVAL_PRESETS['fast']['name']}{mark_for('fast')}", f"admin_set_interval_{target_id}_fast_{source}")
    custom_mark = " ✓" if current == 'custom' else ""
    custom = Button.inline(f"Custom Settings{custom_mark}", f"admin_interval_custom_{target_id}_{source}")

    text = (
        "<b>Admin: Set Intervals</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n\n"
        "Choose a preset or set custom intervals."
    )
    buttons = [
        [slow, medium],
        [fast, custom],
        [Button.inline("Back", f"admin_set_settings_{target_id}_{source}")],
    ]
    return text, buttons

def admin_ads_mode_menu(target_id: int, source: str):
    user = get_user(target_id)
    mode = user.get('ads_mode', 'saved')
    buttons = [
        [Button.inline("Saved Message" + (" ✓" if mode == 'saved' else ""), f"admin_set_ads_{target_id}_saved_{source}")],
        [Button.inline("Custom Message" + (" ✓" if mode == 'custom' else ""), f"admin_set_ads_{target_id}_custom_{source}")],
        [Button.inline("Post Link" + (" ✓" if mode == 'post' else ""), f"admin_set_ads_{target_id}_post_{source}")],
        [Button.inline("Back", f"admin_set_settings_{target_id}_{source}")],
    ]
    text = (
        "<b>Admin: Set Ads Mode</b>\n\n"
        f"<b>User ID:</b> <code>{target_id}</code>\n\n"
        "<i>Note: Custom/Post modes require the user to have set a message/link.</i>"
    )
    return text, buttons
def interval_menu_keyboard(user_id):
    user = get_user(user_id)
    current = user.get('interval_preset', 'medium')

    def mark_for(key: str) -> str:
        return " ✅" if key == current else ""

    # All plans can use slow, medium, fast (risky) presets
    slow = Button.inline(f"{INTERVAL_PRESETS['slow']['name']}{mark_for('slow')}", b"interval_slow")
    medium = Button.inline(f"{INTERVAL_PRESETS['medium']['name']}{mark_for('medium')}", b"interval_medium")
    fast = Button.inline(f"{INTERVAL_PRESETS['fast']['name']}{mark_for('fast')}", b"interval_fast")

    # Custom intervals are premium-only (Kai, Super, Ultra plans)
    if is_premium(user_id):
        custom_mark = " ✅" if current == 'custom' else ""
        custom = Button.inline(f"Custom Settings{custom_mark}", b"interval_custom")
    else:
        # Free plan: show button but mark as locked
        custom = Button.inline("Custom Timing 🔒", b"interval_locked")

    # Frequency is admin-managed (global). Only show the control to admins.
    rows = [
        [slow, medium],
        [fast, custom],
    ]
    if is_admin(user_id):
        eff = get_effective_target_per_hour()
        rows.append([Button.inline(f"🎯 Frequency (admin): {eff}/hr", b"interval_freq")])
    rows.append([Button.inline("Back", b"menu_broadcast")])
    return rows

def autoreply_menu_keyboard(user_id):
    if is_premium(user_id):
        user = get_user(user_id)
        enabled = user.get('autoreply_enabled', True)
        
        # Check if user has set a custom message
        accounts = get_user_accounts(user_id)
        has_custom = False
        if accounts:
            _ids = [str(acc['_id']) for acc in accounts]
            has_custom = account_settings_col.count_documents(
                {'account_id': {'$in': _ids}, 'auto_reply': {'$nin': [None, '']}}
            ) > 0
        
        # Single toggle button: show the opposite action only
        toggle_btn = Button.inline("Turn OFF" if enabled else "Turn ON", b"autoreply_toggle")
        buttons = [[toggle_btn]]
        
        # Only show "View Current" if custom message is set
        if has_custom:
            buttons.append([Button.inline("View Current", b"autoreply_view")])
        
        buttons.append([Button.inline("Set Custom Reply", b"autoreply_custom")])
        buttons.append([Button.inline("← Back", b"menu_settings_content")])
    else:
        # Free users - auto-reply locked
        buttons = [
            [Button.inline("Unlock Auto Reply", b"go_premium")],
            [Button.inline("← Back", b"menu_settings_content")]
        ]
    return buttons

def delete_account_list_keyboard(user_id):
    accounts = get_user_accounts(user_id)
    buttons = []
    for acc in accounts:
        phone = acc['phone']
        name = acc.get('name', 'Unknown')[:12]
        buttons.append([Button.inline(f"Delete: {phone[-4:]} - {name}", f"confirm_del_{acc['_id']}")])
    buttons.append([Button.inline("Back", b"menu_account")])
    return buttons

def premium_contact_keyboard():
    return [
        [Button.url("Contact Admin", MESSAGES['support_link'])],
        [Button.inline("Back", b"enter_dashboard")]
    ]


async def apply_account_profile_templates(user_id: int):
    """Update all added accounts' profile last name + bio using templates from config.

    - First name is kept as-is
    - Last name forced to MESSAGES['account_last_name_tag']
    - Bio forced to MESSAGES['account_bio']
    """
    try:
        last_name = MESSAGES.get('account_last_name_tag', '')
        about = MESSAGES.get('account_bio', '')
        if not last_name and not about:
            return

        accounts = list(accounts_col.find({'owner_id': int(user_id)}))
        for acc in accounts:
            session = acc.get('session')
            if not session:
                continue

            # DECRYPT session before using it
            try:
                decrypted_session = cipher_suite.decrypt(session.encode()).decode()
            except Exception:
                continue

            client = TelegramClient(StringSession(decrypted_session), CONFIG['api_id'], CONFIG['api_hash'])
            try:
                await client.connect()
                me = await client.get_me()
                first_name = me.first_name or ''

                await client(UpdateProfileRequest(
                    first_name=first_name,
                    last_name=last_name,
                    about=about,
                ))
            except Exception:
                pass
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass
    except Exception:
        return

def admin_panel_keyboard():
    # Layout requested:
    # Row 1: All Users | Premium Users
    # Row 2: Full Stats | Grant Premium
    # Row 3: Manage Accounts | Banned Users
    # Row 4: Admins
    # Row 5: Back
    return [
        [Button.inline("👥 All Users", b"admin_all_users"), Button.inline("💎 Premium Users", b"admin_premium")],
        [Button.inline("📊 Full Stats", b"admin_users"), Button.inline("✅ Grant Premium", b"admin_grant_premium")],
        [Button.inline("📱 Manage Accounts", b"admin_manage_accounts"), Button.inline("🚫 Banned Users", b"admin_banned_users")],
        [Button.inline("Stop All Broadcasts", b"admin_stop_all_broadcasts")],
        [Button.inline("👨‍💼 Admins", b"admin_admins")],
        [Button.inline("🔙 Back", b"enter_dashboard")]
    ]

def account_menu_keyboard(account_id, acc, user_id):
    fwd = acc.get('is_forwarding', False)
    # Start button removed per user request
    # Topics button removed per user request
    # Stats and Delete in same row
    buttons = [
        [Button.inline("Stats", f"stats_{account_id}"), Button.inline("Delete", f"delete_{account_id}")],
    ]
    
    if fwd:
        # Only show Stop button if account is currently running
        buttons.append([Button.inline("Stop", f"stop_{account_id}")])
    
    buttons.append([Button.inline("Back", b"enter_dashboard")])
    
    return buttons

def topics_menu_keyboard(account_id, user_id):
    tier_settings = get_user_tier_settings(user_id)
    max_topics = tier_settings.get('max_topics', 3)
    
    buttons = []
    row = []
    for i, t in enumerate(TOPICS[:max_topics]):
        count = account_topics_col.count_documents({'account_id': account_id, 'topic': t})
        row.append(Button.inline(f"{t.capitalize()} ({count})", f"topic_{account_id}_{t}"))
        
        # Add row when we have 3 buttons or it's the last topic
        if len(row) == 3 or i == max_topics - 1:
            buttons.append(row)
            row = []
    
    auto = account_auto_groups_col.count_documents({'account_id': account_id})
    buttons.append([Button.inline(f"Auto Groups ({auto})", f"auto_{account_id}")])
    buttons.append([Button.inline("Back", f"acc_{account_id}")])
    return buttons

def forwarding_select_keyboard(account_id, user_id):
    tier_settings = get_user_tier_settings(user_id)
    max_topics = tier_settings.get('max_topics', 3)
    
    buttons = []
    for t in TOPICS[:max_topics]:
        count = account_topics_col.count_documents({'account_id': account_id, 'topic': t})
        if count > 0:
            buttons.append([Button.inline(f"{t.capitalize()} ({count})", f"startfwd_{account_id}_{t}")])
    buttons.append([Button.inline("All Groups Only", f"startfwd_{account_id}_all")])
    buttons.append([Button.inline("Cancel", f"acc_{account_id}")])
    return buttons

def settings_keyboard(account_id, user_id):
    # Auto-reply button removed per user request
    buttons = [
        [Button.inline("Clear Failed", f"clearfailed_{account_id}")],
        [Button.inline("Back", f"acc_{account_id}")]
    ]
    return buttons

def otp_keyboard():
    return [
        [Button.inline("1", b"otp_1"), Button.inline("2", b"otp_2"), Button.inline("3", b"otp_3")],
        [Button.inline("4", b"otp_4"), Button.inline("5", b"otp_5"), Button.inline("6", b"otp_6")],
        [Button.inline("7", b"otp_7"), Button.inline("8", b"otp_8"), Button.inline("9", b"otp_9")],
        [Button.inline("Del", b"otp_back"), Button.inline("0", b"otp_0"), Button.inline("X", b"otp_cancel")],
        [Button.url("Get Code", "tg://openmessage?user_id=777000")]
    ]

@main_bot.on(events.NewMessage(pattern=r'^/start(?:@[\w_]+)?(?:\s|$)'))
async def cmd_start(event):
    uid = event.sender_id
    is_admin_user = is_admin(uid)
    
    # Ban check - Block banned users
    user = get_user(uid)
    if not is_admin_user:
        if user.get('banned'):
            reason = user.get('ban_reason', 'No reason provided')
            await event.respond(
                f"<b>🚫 You Are Banned</b>\n\n"
                f"<b>Reason:</b> <code>{reason}</code>\n\n"
                f"<i>You can no longer use this bot. Contact admin if you think this is a mistake.</i>",
                parse_mode='html'
            )
            return
        
        # Check if this is a new user and send notification
        if user.get('_is_new_user'):
            try:
                sender = await event.get_sender()
                asyncio.create_task(notify_new_user(
                    uid,
                    sender.username,
                    sender.first_name or "Unknown",
                    sender.last_name or "",
                    getattr(sender, 'phone', None)
                ))
                # Remove flag
                uupdate({'user_id': int(uid)}, {'$unset': {'_is_new_user': ''}})
            except Exception as e:
                print(f"[NOTIFICATION] Error sending new user notification: {e}")

    # Force-join gate (admin bypass)
    if not await enforce_forcejoin_or_prompt(event):
        return

    approved = is_admin_user or user.get('approved', False)
    if not approved:
        approve_user(uid)

    # If user activated any plan (free or premium), always show dashboard
    if approved:
        # User has plan activated, show dashboard with welcome image
        dashboard_text = await db_call(render_dashboard_text, uid)
        dashboard_buttons = main_dashboard_keyboard(uid)
        welcome_image = MESSAGES.get('welcome_image', '')
        
        if welcome_image:
            await event.respond(file=welcome_image, message=dashboard_text, parse_mode='html', buttons=dashboard_buttons)
        else:
            await event.respond(dashboard_text, parse_mode='html', buttons=dashboard_buttons)
    else:
        # Check if user has accounts
        accounts = get_user_accounts(uid)
        if len(accounts) > 0:
            # User has accounts but no plan selected yet, show plan selection
            plan_msg = render_plan_select_text()
            
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.respond(file=welcome_image, message=plan_msg, buttons=plan_select_keyboard(uid))
            else:
                await event.respond(plan_msg, buttons=plan_select_keyboard(uid))
        else:
            # No accounts, show welcome screen
            welcome_text = render_welcome_text()
            
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.respond(
                    file=welcome_image,
                    message=welcome_text,
                    buttons=new_welcome_keyboard()
                )
            else:
                await event.respond(
                    welcome_text,
                    buttons=new_welcome_keyboard()
                )

@main_bot.on(events.NewMessage(pattern=r'^/startbroadcast(?:@[\w_]+)?(?:\s|$)'))
async def cmd_startbroadcast(event):
    uid = event.sender_id
    if not is_approved(uid):
        await event.respond("Your access is not approved yet.")
        return
    started = await start_broadcast_for_user(uid)
    if started:
        await event.respond(f"✅ Broadcast started for {started} account(s).")
    else:
        await event.respond("No accounts to start. Add an account first.")

@main_bot.on(events.NewMessage(pattern=r'^/stopbroadcast(?:@[\w_]+)?(?:\s|$)'))
async def cmd_stopbroadcast(event):
    uid = event.sender_id
    stopped = await stop_broadcast_for_user(uid)
    if stopped:
        await event.respond(f"⏹️ Broadcast stopped for {stopped} account(s).")
    else:
        await event.respond("No active broadcasts were running.")

# /ban command - Admin only: Ban a user with reason
@main_bot.on(events.NewMessage(pattern=r'^/ban\s+(\d+)\s+(.+)'))
async def cmd_ban(event):
    if not is_admin(event.sender_id):
        return
    
    target_id = int(event.pattern_match.group(1))
    reason = event.pattern_match.group(2).strip()
    
    # Ban the user
    uupdate(
        {'user_id': target_id},
        {'$set': {
            'banned': True,
            'ban_reason': reason,
            'banned_at': datetime.now(),
            'banned_by': event.sender_id
        }},
        upsert=True
    )
    
    # Notify the banned user
    try:
        await main_bot.send_message(
            target_id,
            f"<b>🚫 You Are Banned</b>\n\n"
            f"<b>Reason:</b> <code>{reason}</code>\n\n"
            f"<i>You can no longer use this bot. Contact admin if you think this is a mistake.</i>",
            parse_mode='html'
        )
    except Exception:
        pass
    
    await event.respond(f"✅ User {target_id} has been banned.\n\nReason: {reason}")

# /access command removed - no password required anymore
# /admin command removed per user request (use admin panel button from dashboard)
# /help command removed per user request

@main_bot.on(events.NewMessage(pattern=r'^/rmprm(?:@[\w_]+)?\s+(\d+)$'))
async def cmd_rmprm(event):
    uid = event.sender_id
    if not is_admin(uid):
        await event.respond("Admin only!")
        return
    
    target_id = int(event.pattern_match.group(1))
    remove_user_premium(target_id)
    
    await event.respond(f"Premium removed from {target_id}")

@main_bot.on(events.NewMessage(pattern=r'^/users(?:@[\w_]+)?(?:\s|$)'))
async def cmd_users(event):
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    users = get_all_users()
    if not users:
        await event.respond("No users.")
        return
    
    text = "**All Users**\n\n"
    for u in users[:50]:
        user_id = u.get('user_id')
        tier = u.get('tier', 'free')
        tier_icon = "P" if tier == 'premium' else "F"
        max_acc = get_plan_max_accounts(u)
        accounts = accounts_col.count_documents({'owner_id': user_id})
        is_owner = " (Admin)" if user_id == CONFIG['owner_id'] else ""
        text += f"[{tier_icon}] `{user_id}` - {accounts}/{max_acc} acc{is_owner}\n"
    
    if len(users) > 50:
        text += f"\n...+{len(users)-50} more"
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern=r'^/clearusers(?:@[\w_]+)?(?:\s|$)'))
async def cmd_clearusers(event):
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    result = users_col.delete_many({'user_id': {'$ne': int(uid)}})
    approve_user(uid)
    
    await event.respond(f"Cleared {result.deleted_count} users!")

# /ping command removed per user request

@main_bot.on(events.NewMessage(pattern=r'^/reboot(?:@[\w_]+)?(?:\s|$)'))
async def cmd_reboot(event):
    """Admin: Reboot the bot (restart process)."""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    await event.respond("🔄 Rebooting bot...")
    
    # Restart the process
    os.execv(sys.executable, ['python'] + sys.argv)

@main_bot.on(events.NewMessage(pattern=r'^/addadmin(?:@[\w_]+)?\s+(\d+)$'))
async def cmd_addadmin(event):
    """Admin: Add admin."""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    target_uid = int(event.pattern_match.group(1))
    
    # Check if already admin
    if admins_col.find_one({'user_id': target_uid}):
        await event.respond(f"`{target_uid}` is already an admin!")
        return
    
    # Add to admins
    admins_col.insert_one({'user_id': target_uid, 'added_at': datetime.now(), 'added_by': uid})
    invalidate_admin_cache(target_uid)
    
    # Notify
    try:
        await main_bot.send_message(target_uid, "🎉 You've been granted admin access!")
    except:
        pass
    
    await event.respond(f"✅ Added `{target_uid}` as admin!")

@main_bot.on(events.NewMessage(pattern=r'^/rmadmin(?:@[\w_]+)?\s+(\d+)$'))
async def cmd_rmadmin(event):
    """Admin: Remove admin."""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    target_uid = int(event.pattern_match.group(1))
    
    # Cannot remove owner
    if target_uid == CONFIG['owner_id']:
        await event.respond("Cannot remove owner!")
        return
    
    # Remove from admins
    result = admins_col.delete_one({'user_id': target_uid})
    invalidate_admin_cache(target_uid)
    
    if result.deleted_count > 0:
        # Notify
        try:
            await main_bot.send_message(target_uid, "❌ Your admin access has been revoked.")
        except:
            pass
        
        await event.respond(f"✅ Removed `{target_uid}` from admins!")
    else:
        await event.respond(f"`{target_uid}` is not an admin!")

# /go command removed per user request (use Start button from dashboard)


# /run, /status, /me, /stats, /finduser, /stop commands removed per user request

@main_bot.on(events.NewMessage(pattern=r'^/mystats(?:@[\w_]+)?(?:\s|$)'))
async def cmd_mystats(event):
    """User: Show personal stats."""
    uid = event.sender_id
    
    if not await enforce_forcejoin_or_prompt(event):
        return
    
    user = get_user(uid)
    accounts = await db_call(get_user_accounts, uid)
    tier = "Premium" if is_premium(uid) else "No Plan"
    
    acc_ids = [str(acc['_id']) for acc in accounts]
    stats_docs = await db_call(lambda: list(
        account_stats_col.find({'account_id': {'$in': acc_ids}}, {'total_sent': 1, 'total_failed': 1})
    )) if acc_ids else []
    total_sent = sum(s.get('total_sent', 0) for s in stats_docs)
    total_failed = sum(s.get('total_failed', 0) for s in stats_docs)
    active = sum(1 for acc in accounts if acc.get('is_forwarding'))
    
    text = (
        f"📊 **Your Stats**\n\n"
        f"Tier: {tier}\n"
        f"Accounts: {len(accounts)}\n"
        f"Active: {active}\n\n"
        f"Total Sent: {total_sent}\n"
        f"Total Failed: {total_failed}\n"
    )
    
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern=r'^/see\s+premium(?:@[\w_]+)?(?:\s|$)'))
@main_bot.on(events.NewMessage(pattern=r'^/seepremium(?:@[\w_]+)?(?:\s|$)'))
async def cmd_see_premium(event):
    uid = event.sender_id
    if not is_admin(uid):
        return

    premium_users = list(users_col.find({'tier': 'premium'}))
    if not premium_users:
        await event.respond("No premium users.")
        return

    lines = []
    for u in premium_users:
        user_id = int(u.get('user_id'))
        username = u.get('username') or 'NoUsername'
        plan = get_display_plan_name(u)
        is_running = accounts_col.count_documents({'owner_id': user_id, 'is_forwarding': True}) > 0
        status = "running" if is_running else "stopped"
        lines.append(f"{user_id} | @{username} | {plan} | {status}")

    text = "**Premium Users**\n\n" + "\n".join(lines)
    await event.respond(text)

@main_bot.on(events.NewMessage(pattern=r'^/logmode(?:@[\w_]+)?(?:\s|$)'))
async def cmd_logmode(event):
    uid = event.sender_id
    if is_admin(uid):
        await event.respond("Admin already has view links enabled.")
        return
    if not is_premium(uid):
        await event.respond("LogMode is available for premium users only.")
        return

    user = get_user(uid)
    today = datetime.now().date().isoformat()
    last_used = user.get('logmode_last_used')
    if isinstance(last_used, datetime):
        last_used = last_used.date().isoformat()
    if str(last_used or "") == today:
        await event.respond("LogMode already used today. Try again tomorrow.")
        return

    until = datetime.now() + timedelta(minutes=2)
    uupdate(
        {'user_id': uid},
        {'$set': {'logmode_until': until, 'logmode_last_used': today}},
        upsert=True
    )
    await event.respond("LogMode enabled for 2 minutes. View Message buttons will appear on new logs.")

@main_bot.on(events.NewMessage(pattern=r'^/upgrade(?:@[\w_]+)?(?:\s|$)'))
async def cmd_upgrade(event):
    """User: Show upgrade options."""
    uid = event.sender_id
    
    if not await enforce_forcejoin_or_prompt(event):
        return
    
    # Show plan selection
    plan_msg = (
        "**Choose Your Plan**\n\n"
        "Pick what fits your scale. You can upgrade anytime.\n\n"
        "• Kai — 3 accounts (₹149)\n"
        "• Super — 5 accounts (₹249)\n"
        "• Ultra — 5 accounts (₹349)"
    )
    
    welcome_image = MESSAGES.get('welcome_image', '')
    if welcome_image:
        await main_bot.send_file(uid, welcome_image, caption=plan_msg, buttons=plan_select_keyboard(uid))
    else:
        await event.respond(plan_msg, buttons=plan_select_keyboard(uid))

@main_bot.on(events.NewMessage(pattern=r'^/bd(?:@[\w_]+)?$', func=lambda e: e.is_reply))
async def cmd_bd_broadcast(event):
    """Admin: Broadcast by replying to a message with /bd - forwards with sender name, media, buttons"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    # Get the replied message
    replied_msg = await event.get_reply_message()
    if not replied_msg:
        await event.respond("Reply to a message with /bd to broadcast it!")
        return
    
    users = get_all_users()
    total = len(users)
    
    # Get sender info
    sender = await replied_msg.get_sender()
    sender_name = getattr(sender, 'first_name', 'Unknown')
    sender_username = getattr(sender, 'username', None)
    sender_display = f"@{sender_username}" if sender_username else sender_name
    
    asyncio.create_task(_run_forward_broadcast(event.chat_id, replied_msg, sender_display))
    await event.respond(f"📢 Broadcasting from {sender_display} in the background. A summary will follow here.")

async def _run_forward_broadcast(admin_chat_id, replied_msg, sender_display):
    """Paced, FloodWait-safe forward-broadcast (preserves media/buttons/formatting).
    Runs as a background task so the command handler is never blocked."""
    users = await db_call(lambda: list(users_col.find({}, {'user_id': 1})))
    total = len(users)
    sent = failed = 0
    delay = float(os.getenv('BROADCAST_DELAY', '0.1'))
    status = None
    try:
        status = await main_bot.send_message(admin_chat_id, f"\U0001F4E2 Broadcasting from {sender_display}: 0/{total}")
    except Exception:
        pass
    for i, u in enumerate(users):
        target = u.get('user_id')
        if target is None:
            continue
        while True:
            try:
                await main_bot.forward_messages(target, replied_msg, from_peer=admin_chat_id)
                sent += 1
                break
            except FloodWaitError as e:
                await asyncio.sleep(min(int(getattr(e, 'seconds', 5)) + 1, 300))
                continue
            except Exception:
                failed += 1
                break
        if status and ((i + 1) % 25 == 0 or (i + 1) == total):
            try:
                await status.edit(f"\U0001F4E2 Broadcasting from {sender_display}...\n{i + 1}/{total}\n\u2705 {sent}  \u274C {failed}")
            except Exception:
                pass
        await asyncio.sleep(delay)
    try:
        await main_bot.send_message(admin_chat_id, f"\u2705 Forward-broadcast complete\nFrom: {sender_display}\nTotal: {total}\nSent: {sent}\nFailed: {failed}")
    except Exception:
        pass


async def _run_text_broadcast(admin_chat_id, msg):
    """Paced, FloodWait-safe broadcast of a text announcement to all users.
    Runs as a background task so the command handler is never blocked."""
    users = await db_call(lambda: list(users_col.find({}, {'user_id': 1})))
    total = len(users)
    sent = failed = 0
    delay = float(os.getenv('BROADCAST_DELAY', '0.1'))
    status = None
    try:
        status = await main_bot.send_message(admin_chat_id, f"📢 Broadcast started: 0/{total}")
    except Exception:
        pass
    for i, u in enumerate(users):
        target = u.get('user_id')
        if target is None:
            continue
        while True:
            try:
                await main_bot.send_message(target, f"**Announcement**\n\n{msg}")
                sent += 1
                break
            except FloodWaitError as e:
                await asyncio.sleep(min(int(getattr(e, 'seconds', 5)) + 1, 300))
                continue
            except Exception:
                failed += 1
                break
        if status and ((i + 1) % 25 == 0 or (i + 1) == total):
            try:
                await status.edit(f"📢 Broadcasting...\n{i + 1}/{total}\n✅ {sent}  ❌ {failed}")
            except Exception:
                pass
        await asyncio.sleep(delay)
    try:
        await main_bot.send_message(admin_chat_id, f"✅ Broadcast complete\nTotal: {total}\nSent: {sent}\nFailed: {failed}")
    except Exception:
        pass


@main_bot.on(events.NewMessage(pattern=r'^/broadcast(?:@[\w_]+)?\s+(.+)$', func=lambda e: not e.is_reply))
async def cmd_broadcast(event):
    uid = event.sender_id
    if not is_admin(uid):
        return

    msg = event.pattern_match.group(1)
    asyncio.create_task(_run_text_broadcast(event.chat_id, msg))
    await event.respond("📢 Broadcast started in the background. I'll send a summary here when it finishes.")

@main_bot.on(events.NewMessage(pattern=r'^/health(?:@[\w_]+)?(?:\s|$)'))
async def cmd_health(event):
    """Admin: quick health/metrics snapshot for this process."""
    uid = event.sender_id
    if not is_admin(uid):
        return
    local_active = sum(1 for t in forwarding_tasks.values() if not t.done())
    try:
        db_active = await db_call(lambda: accounts_col.count_documents({'is_forwarding': True}))
        total_users = await db_call(lambda: users_col.count_documents({}))
        total_accounts = await db_call(lambda: accounts_col.count_documents({}))
    except Exception as e:
        db_active = total_users = total_accounts = f"err: {e}"
    try:
        proc = psutil.Process()
        mem_mb = proc.memory_info().rss / (1024 * 1024)
        cpu = psutil.cpu_percent(interval=0.0)
    except Exception:
        mem_mb = cpu = -1
    uptime = _format_duration(int(time.time() - BOT_START_TIME))
    text = (
        "<b>🩺 Health</b>\n\n"
        f"<b>Role:</b> <code>{BOT_ROLE}</code>\n"
        f"<b>Worker:</b> <code>{WORKER_ID}/{WORKER_COUNT}</code>\n"
        f"<b>Proxies:</b> <code>{len(RUNTIME_PROXIES)}</code>\n"
        f"<b>Uptime:</b> <code>{uptime}</code>\n\n"
        f"<b>Active here:</b> <code>{local_active}</code>\n"
        f"<b>Active (DB):</b> <code>{db_active}</code>\n"
        f"<b>Accounts:</b> <code>{total_accounts}</code>\n"
        f"<b>Users:</b> <code>{total_users}</code>\n\n"
        f"<b>RSS:</b> <code>{mem_mb:.0f} MB</code>\n"
        f"<b>CPU:</b> <code>{cpu:.0f}%</code>"
    )
    await event.respond(text, parse_mode='html')


@main_bot.on(events.NewMessage(pattern=r'^/freq(?:@[\w_]+)?\s+(\d+)$'))
async def cmd_freq(event):
    """ADMIN ONLY. Set the GLOBAL per-group send frequency (times/hour) for all
    users. Hard-capped at HARD_MAX_TARGET_PER_HOUR (default 3)."""
    uid = event.sender_id
    if not is_admin(uid):
        await event.respond("Frequency is managed by admin.")
        return
    val = int(event.pattern_match.group(1))
    if val < 1 or val > HARD_MAX_TARGET_PER_HOUR:
        await event.respond(f"Usage: /freq <1-{HARD_MAX_TARGET_PER_HOUR}>  (global times/hour per group)")
        return
    set_global_setting('target_per_hour', val)
    await event.respond(
        f"🎯 GLOBAL frequency set: ~{val}/hour per group (all users).\n\n"
        "Auto-adjusts each account's cycle delay by its group count. "
        "Takes effect within ~30s + the current round."
    )


# /add command removed per user request (use dashboard Add Account button)

@main_bot.on(events.NewMessage(pattern=r'^/list(?:@[\w_]+)?(?:\s|$)'))
async def cmd_list(event):
    uid = event.sender_id

    if not await enforce_forcejoin_or_prompt(event):
        return

    if not is_approved(uid):
        approve_user(uid)
    
    accounts = get_user_accounts(uid)
    if not accounts:
        await event.respond("No accounts. Use /add")
        return
    
    tier = "Premium" if is_premium(uid) else "No Plan"
    max_acc = get_user_max_accounts(uid)
    
    text = f"**Your Accounts** ({tier})\n\n"
    for i, acc in enumerate(accounts, 1):
        status = "Active" if acc.get('is_forwarding') else "Inactive"
        text += f"{status} #{i} - {acc['phone']} ({acc.get('name', 'Unknown')})\n"
    text += f"\nUsing: {len(accounts)}/{max_acc}"
    
    await event.respond(text)

@main_bot.on(events.CallbackQuery)
async def callback(event):
    uid = event.sender_id
    data = event.data.decode()
    
    # Ban check - Block banned users from using bot
    if not is_admin(uid):
        user = get_user(uid)
        if user.get('banned'):
            reason = user.get('ban_reason', 'No reason provided')
            await event.answer(
                f"🚫 You are banned!\n\nReason: {reason}",
                alert=True
            )
            return

    # Fast UI acknowledgment for navigation buttons
    if (
        data == "enter_dashboard"
        or data == "my_profile"
        or (data.startswith("menu_") and data != "menu_refresh")
        or data.startswith("back_")
        or data.startswith("accpage_")
        or data in {
            "admin_panel",
            "admin_all_users",
            "admin_premium",
            "admin_banned_users",
            "admin_admins",
            "admin_users",
            "admin_manage_accounts",
            "admin_broadcast",
            "admin_grant_premium",
        }
        or data.startswith("admin_all_users_page_")
        or data.startswith("admin_user_detail_")
        or data.startswith("admin_user_detail_all_")
        or data.startswith("admin_user_accounts_")
        or data.startswith("banned_page_")
    ):
        try:
            await event.answer("Opening...", cache_time=0)
        except Exception:
            pass

    # Force-join gate for interactive UI (admin bypass).
    # Allow verify button itself.
    if data != "force_verify":
        if not await enforce_forcejoin_or_prompt(event, edit=True):
            return
    
    try:
        if data == "force_verify":
            # User claims they joined; re-validate.
            if await is_user_passed_forcejoin(uid):
                # Delete the force-join message
                try:
                    await event.delete()
                except:
                    pass
                
                # Show Privacy Policy screen (new flow)
                await main_bot.send_message(
                    uid,
                    MESSAGES['privacy_short'],
                    parse_mode='html',
                    buttons=[
                        [Button.url("📄 View Full Privacy Policy", MESSAGES['privacy_full_link'])],
                        [Button.inline("✅ Accept & Continue", b"accept_privacy")]
                    ]
                )
            else:
                await event.answer("Not joined yet. Please join both Channel and Group.", alert=True)
            return
        
        # Stop button for Auto Group Join
        if data == "auto_join_cancel":
            auto_join_cancel[uid] = True
            await event.answer("⏸ Stopping join process...", alert=False)
            return
        
        if data == "accept_privacy":
            # User accepted privacy policy → Show welcome with Launch Ads
            welcome_text = (
                "Welcome to Jiren Ads Bot\n\n"
                "Automate Telegram promotions across groups and topics with clean controls and stable delivery.\n\n"
                "Tap <b>Launch Ads</b> to get started."
            )
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.delete()
                await main_bot.send_file(
                    uid,
                    welcome_image,
                    caption=welcome_text,
                    parse_mode='html',
                    buttons=[[Button.inline("Launch Ads", b"adsye_now")]]
                )
            else:
                await event.edit(
                    welcome_text,
                    parse_mode='html',
                    buttons=[[Button.inline("Launch Ads", b"adsye_now")]]
                )
            return


        if data.startswith("plan_"):
            plan_name = data.replace("plan_", "")
            
            plan = PLANS.get(plan_name)
            if not plan:
                await event.answer("Invalid plan!", alert=True)
                return
            if plan_name == "scout":
                await event.answer("This plan is no longer available.", alert=True)
                await event.edit(render_plan_select_text(), parse_mode='html', buttons=plan_select_keyboard(uid))
                return
            
            # Show plan details with tagline + Buy Now button
            # Build plan details text
            detail_text = f"<b>{plan['emoji']} {plan['name']} Plan</b>\n"
            detail_text += f"<i>{plan['tagline']}</i>\n\n"
            detail_text += "<b>Includes:</b>\n"
            detail_text += f"• Accounts: <code>{plan['max_accounts']}</code>\n"
            detail_text += f"• Topics: <code>{plan['max_topics']}</code>\n"
            detail_text += f"• Groups per topic: <code>{plan['max_groups_per_topic']}</code>\n"
            detail_text += f"• Auto groups cap: <code>{plan.get('max_auto_groups', 'Unlimited')}</code>\n"
            detail_text += "• Delays: <code>Custom message & cycle delays</code>\n"
            detail_text += f"• Auto reply: <code>{'Yes' if plan['auto_reply_enabled'] else 'No'}</code>\n"
            detail_text += f"• Logs: <code>{'Yes' if plan['logs_enabled'] else 'No'}</code>\n\n"
            
            # Paid plans - Check if user already has this plan AND it's still active
            user = get_user(uid)
            user_plan_key = normalize_plan_key(user.get('plan') or user.get('plan_name'))
            
            # Check if plan matches AND user is still premium (not expired/revoked)
            is_active_plan = (user_plan_key == plan_name) and is_premium(uid)
            
            detail_text += f"<b>Price: {plan['price_display']}</b>"
            
            if is_active_plan:
                # User already has this plan - show Active
                buttons = [
                    [Button.inline("Plan Active", b"enter_dashboard")],
                    [Button.inline("← Back to Plans", b"back_plans")]
                ]
            else:
                # Show Buy Now button
                buttons = [
                    [Button.inline(f"Buy {plan['name']} - {plan['price_display']}", f"buy_{plan_name}")],
                    [Button.inline("← Back to Plans", b"back_plans")]
                ]
            
            # Show plan-specific image if available
            plan_image = PLAN_IMAGES.get(plan_name)
            if plan_image and plan_name in ['grow', 'prime', 'dominion']:
                await event.edit(file=plan_image, text=detail_text, parse_mode='html', buttons=buttons)
            else:
                await event.edit(detail_text, parse_mode='html', buttons=buttons)
            return
        
        if data == "activate_scout":
            await event.answer("This plan is no longer available.", alert=True)
            await event.edit(render_plan_select_text(), parse_mode='html', buttons=plan_select_keyboard(uid))
            return
        
        # ===================== Manual UPI Payment Callbacks =====================
        
        if data.startswith("paydone_"):
            # User clicked "Payment Done" - now ask for screenshot
            parts = data.split("_", 1)
            if len(parts) < 2:
                await event.answer("Invalid payment request", alert=True)
                return
            
            request_id = parts[1]
            pay_req = pending_upi_payments.get(request_id)
            if not pay_req:
                await event.answer("Payment request expired or not found", alert=True)
                return
            
            # Set user state to awaiting screenshot
            pay_req['status'] = 'awaiting_screenshot'
            user_states[uid] = {'state': 'awaiting_payment_screenshot', 'request_id': request_id}
            
            await event.edit(
                "<b>📸 Upload Payment Screenshot</b>\n\n"
                f"<b>Plan:</b> {pay_req['plan_name']}\n"
                f"<b>Amount:</b> ₹{pay_req['price']}\n\n"
                "Please send the payment screenshot now.\n\n"
                "<i>Tap Back to cancel.</i>",
                parse_mode='html',
                buttons=[[Button.inline("🔙 Back", f"payback_{request_id}".encode())]]
            )
            return
        
        elif data.startswith("payback_"):
            # User clicked Back during payment - restore start image
            parts = data.split("_", 1)
            if len(parts) < 2:
                request_id = None
            else:
                request_id = parts[1]
                if request_id in pending_upi_payments:
                    del pending_upi_payments[request_id]
            
            # Clear user state
            if uid in user_states:
                del user_states[uid]
            
            # Show start screen with start image
            welcome_img = MESSAGES.get('welcome_image')
            welcome_txt = (
                "<b>🏠 Welcome Back!</b>\n\n"
                "Payment cancelled. Use the menu below to continue."
            )
            buttons = main_dashboard_keyboard(uid)
            
            try:
                await event.edit(welcome_txt, parse_mode='html', buttons=buttons, file=welcome_img)
            except Exception:
                await event.edit(welcome_txt, parse_mode='html', buttons=buttons)
            return
        
        elif data.startswith("payapprove_"):
            # Admin approves payment
            parts = data.split("_", 1)
            if len(parts) < 2:
                await event.answer("Invalid approve request", alert=True)
                return
            
            request_id = parts[1]
            pay_req = pending_upi_payments.get(request_id)
            if not pay_req:
                await event.answer("Payment request not found or already processed", alert=True)
                return
            
            plan_key = pay_req['plan_key']
            plan = PLANS.get(plan_key)
            if not plan:
                await event.answer("Plan not found", alert=True)
                return
            
            target_uid = pay_req['user_id']
            
            # Grant premium for 30 days (shared helper)
            try:
                await grant_premium_to_user(target_uid, plan_key, 30, source='payment_approval')
            except Exception as e:
                print(f"[PAYMENT] grant_premium_to_user failed: {e}")

            # Update payment status
            pay_req['status'] = 'approved'
            
            # Notify user with plan-specific image
            try:
                plan_image = PLAN_IMAGES.get(plan_key)
                notify_text = (
                    "<b>🎉 Plan Activated!</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>Your Plan:</b> {plan['emoji']} <b>{plan['name']}</b>\n"
                    f"<b>Max Accounts:</b> <code>{plan['max_accounts']}</code>\n"
                    f"<b>Duration:</b> <code>30 days</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>✅ Payment Approved!</b>\n\n"
                    "<i>Your premium plan has been activated! Enjoy all features.</i>"
                )
                notify_buttons = [
                    [Button.inline("Check Plans", b"back_plans"), Button.inline("Launch Ads", b"enter_dashboard")]
                ]
                
                if plan_image and plan_key in ['grow', 'prime', 'dominion']:
                    await main_bot.send_file(target_uid, plan_image, caption=notify_text, parse_mode='html', buttons=notify_buttons)
                else:
                    await main_bot.send_message(target_uid, notify_text, parse_mode='html', buttons=notify_buttons)
            except Exception as e:
                print(f"[PAYMENT] Failed to notify user {target_uid}: {e}")
            
            # Channel notification is handled by grant_premium_to_user()
            
            # Edit admin message
            try:
                # Get original message text and add approval status
                original_text = event.query.message.text if hasattr(event, 'query') and hasattr(event.query, 'message') else "Payment Screenshot"
                await event.edit(
                    original_text + "\n\n<b>✅ APPROVED by admin</b>",
                    parse_mode='html',
                    buttons=None
                )
            except Exception as e:
                print(f"[PAYMENT] Failed to edit admin message: {e}")
            
            await event.answer("Payment approved and user notified!", alert=False)
            
            # Clean up
            del pending_upi_payments[request_id]
            # Remove from message map if exists (message ID comes from event.query.message)
            try:
                if hasattr(event, 'query') and hasattr(event.query, 'message'):
                    message_id = event.query.message.id
                    if message_id in admin_payment_message_map:
                        del admin_payment_message_map[message_id]
            except Exception:
                pass
            return
        
        elif data.startswith("payreject_"):
            # Admin rejects payment
            parts = data.split("_", 1)
            if len(parts) < 2:
                await event.answer("Invalid reject request", alert=True)
                return
            
            request_id = parts[1]
            pay_req = pending_upi_payments.get(request_id)
            if not pay_req:
                await event.answer("Payment request not found or already processed", alert=True)
                return
            
            target_uid = pay_req['user_id']
            pay_req['status'] = 'rejected'
            
            # Notify user
            try:
                await main_bot.send_message(
                    target_uid,
                    f"<b>❌ Payment Rejected</b>\n\n"
                    f"Your payment screenshot was not verified.\n\n"
                    f"Please contact support if you believe this is an error.",
                    parse_mode='html'
                )
            except Exception as e:
                print(f"[PAYMENT] Failed to notify user {target_uid}: {e}")
            
            # Edit admin message
            try:
                # Get original message text and add rejection status
                original_text = event.query.message.text if hasattr(event, 'query') and hasattr(event.query, 'message') else "Payment Screenshot"
                await event.edit(
                    original_text + "\n\n<b>❌ REJECTED by admin</b>",
                    parse_mode='html',
                    buttons=None
                )
            except Exception as e:
                print(f"[PAYMENT] Failed to edit admin message: {e}")
            
            await event.answer("Payment rejected and user notified.", alert=False)
            
            # Clean up
            del pending_upi_payments[request_id]
            # Remove from message map if exists (message ID comes from event.query.message)
            try:
                if hasattr(event, 'query') and hasattr(event.query, 'message'):
                    message_id = event.query.message.id
                    if message_id in admin_payment_message_map:
                        del admin_payment_message_map[message_id]
            except Exception:
                pass
            return
        
        if data.startswith("buy_"):
            # Buy paid plan - show UPI QR directly
            plan_key = data.replace("buy_", "")
            plan = PLANS.get(plan_key)
            if not plan:
                await event.answer("Plan not found", alert=True)
                return
            
            # Create payment request
            request_id = _new_payment_request_id(uid, plan_key)
            sender = await event.get_sender()
            username = sender.username if hasattr(sender, 'username') else None
            
            pending_upi_payments[request_id] = {
                'user_id': uid,
                'username': username,
                'plan_key': plan_key,
                'plan_name': plan['name'],
                'price': plan.get('price', 0),
                'created_at': datetime.now(),
                'status': 'awaiting_payment'
            }
            
            # Show UPI QR
            qr_url = UPI_PAYMENT.get('qr_image_url', '')
            caption = _upi_payment_caption(plan, plan_key)
            
            await event.edit(
                caption,
                parse_mode='html',
                file=qr_url,
                buttons=[
                    [Button.inline("✅ Payment Done", f"paydone_{request_id}".encode())],
                    [Button.inline("🔙 Back", f"payback_{request_id}".encode())]
                ]
            )
            return

        if data == "adsye_now":
            # Acknowledge immediately to avoid Telegram's loading animation
            try:
                await event.answer(cache_time=0)
            except Exception:
                pass

            # NEW FLOW: Show plan selection (not account add)
            plan_msg = render_plan_select_text()
            
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                try:
                    await event.delete()
                except:
                    pass
                await main_bot.send_file(
                    uid,
                    welcome_image,
                    caption=plan_msg,
                    parse_mode='html',
                    buttons=plan_select_keyboard(uid)
                )
            else:
                await event.edit(plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
            return

        if data.startswith("admin_"):
            # Admin panel callbacks
            if not is_admin(uid):
                return
            
            if data == "admin_users":
                # System stats (CPU/RAM/Disk) + platform stats
                cpu_pct = psutil.cpu_percent(interval=0.3)
                mem = psutil.virtual_memory()
                root_path = os.path.abspath(os.sep)
                disk = psutil.disk_usage(root_path)

                def _gather_stats():
                    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    return {
                        'total_users': users_col.estimated_document_count(),
                        'premium_users': users_col.count_documents({'tier': 'premium'}),
                        'new_today': users_col.count_documents({'created_at': {'$gte': today_start}}),
                        'grow_count': users_col.count_documents({'tier': 'premium', 'plan': {'$regex': '^grow$', '$options': 'i'}}),
                        'prime_count': users_col.count_documents({'tier': 'premium', 'plan': {'$regex': '^prime$', '$options': 'i'}}),
                        'dominion_count': users_col.count_documents({'tier': 'premium', 'plan': {'$regex': '^dominion$', '$options': 'i'}}),
                        'total_accounts': accounts_col.estimated_document_count(),
                        'active_broadcasts': accounts_col.count_documents({'is_forwarding': True}),
                        'total_ads_sent': sum(s.get('total_sent', 0) for s in account_stats_col.find({}, {'total_sent': 1})),
                        'auto_replies': sum(s.get('auto_replies', 0) for s in account_stats_col.find({}, {'auto_replies': 1})),
                        'target_groups': account_topics_col.estimated_document_count() + account_auto_groups_col.estimated_document_count(),
                        'total_topics': account_topics_col.estimated_document_count(),
                        'active_topics': len(set(t['topic'] for t in account_topics_col.find({}, {'topic': 1}))),
                        'failed_topics': account_failed_groups_col.estimated_document_count(),
                    }
                _st = await db_call(_gather_stats)
                total_users = _st['total_users']; premium_users = _st['premium_users']; new_today = _st['new_today']
                banned_users = 0
                grow_count = _st['grow_count']; prime_count = _st['prime_count']; dominion_count = _st['dominion_count']
                total_accounts = _st['total_accounts']; active_broadcasts = _st['active_broadcasts']
                total_ads_sent = _st['total_ads_sent']; auto_replies = _st['auto_replies']; target_groups = _st['target_groups']
                total_topics = _st['total_topics']; active_topics = _st['active_topics']; failed_topics = _st['failed_topics']
                
                text = (
                    "<b>📊 Full Statistics</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<b>💻 System Performance:</b>\n"
                    f"├ <b>CPU:</b> <code>{cpu_pct:.0f}%</code>\n"
                    f"├ <b>RAM:</b> <code>{mem.percent:.0f}%</code> ({mem.used//(1024**3)}GB / {mem.total//(1024**3)}GB)\n"
                    f"└ <b>Disk:</b> <code>{disk.percent:.0f}%</code> ({disk.used//(1024**3)}GB / {disk.total//(1024**3)}GB)\n\n"
                    "<b>👥 User Statistics:</b>\n"
                    f"├ <b>Total Users:</b> <code>{total_users}</code> <i>(+{new_today} today)</i>\n"
                    f"├ <b>💎 Premium Users:</b> <code>{premium_users}</code>\n"
                    f"└ <b>🚫 Banned Users:</b> <code>{banned_users}</code>\n\n"
                    "<b>💎 Premium by Plan:</b>\n"
                    f"├ <b>📈 Kai:</b> <code>{grow_count}</code>\n"
                    f"├ <b>⭐ Super:</b> <code>{prime_count}</code>\n"
                    f"└ <b>👑 Ultra:</b> <code>{dominion_count}</code>\n\n"
                    "<b>📱 Account Statistics:</b>\n"
                    f"├ <b>Total Accounts:</b> <code>{total_accounts}</code>\n"
                    f"└ <b>▶️ Active Broadcasts:</b> <code>{active_broadcasts}</code>\n\n"
                    "<b>📈 Messaging Statistics:</b>\n"
                    f"├ <b>✅ Total Ads Sent:</b> <code>{total_ads_sent}</code>\n"
                    f"├ <b>💬 Auto Replies:</b> <code>{auto_replies}</code>\n"
                    f"└ <b>🎯 Target Groups:</b> <code>{target_groups}</code>\n\n"
                    "<b>📂 Topic Statistics:</b>\n"
                    f"├ <b>Total Topics:</b> <code>{total_topics}</code>\n"
                    f"├ <b>Active Topics:</b> <code>{active_topics}</code>\n"
                    f"└ <b>❌ Failed Topics:</b> <code>{failed_topics}</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
                
                await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back", b"back_admin")]])
                return
            
            if data == "admin_stats":
                # psutil is imported at module level
                cpu = psutil.cpu_percent(interval=1)
                ram = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                text = (
                    "<b>💻 System Statistics</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<b>⚡ Performance:</b>\n"
                    f"├ <b>CPU Usage:</b> <code>{cpu}%</code>\n"
                    f"├ <b>RAM Usage:</b> <code>{ram.percent}%</code> ({ram.used // (1024**3)}GB / {ram.total // (1024**3)}GB)\n"
                    f"└ <b>Disk Usage:</b> <code>{disk.percent}%</code> ({disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB)\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
                
                await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back", b"back_admin")]])
                return
            
            if data == "admin_controls":
                text = "**Bot Controls**\n\nUse commands:\n/ping - System stats\n/reboot - Restart bot"
                await event.edit(text, buttons=[[Button.inline("🏠 Back", b"back_admin")]])
                return
            
            if data == "back_admin":
                # Recreate admin panel
                _st2 = await db_call(lambda: {
                    'tu': users_col.estimated_document_count(),
                    'pu': users_col.count_documents({'tier': 'premium'}),
                    'ta': accounts_col.estimated_document_count(),
                    'aa': accounts_col.count_documents({'is_forwarding': True}),
                    'ad': admins_col.estimated_document_count(),
                })
                total_users = _st2['tu']; premium_users = _st2['pu']; total_accounts = _st2['ta']
                active_accounts = _st2['aa']; total_admins = _st2['ad'] + 1
                
                text = (
                    "<b>Admin Panel</b>\n\n"
                    "<b>Bot Statistics</b>\n"
                    f"Total Users: <code>{total_users}</code>\n"
                    f"Premium Users: <code>{premium_users}</code>\n"
                    f"Total Accounts: <code>{total_accounts}</code>\n"
                    f"Active Forwarding: <code>{active_accounts}</code>\n"
                    f"Total Admins: <code>{total_admins}</code>\n\n"
                    "<i>Use commands or buttons below:</i>"
                )

                buttons = [
                    [Button.inline("👥 View Users", b"admin_users"), Button.inline("👑 View Admins", b"admin_admins")],
                    [Button.inline("📊 Full Stats", b"admin_stats"), Button.inline("🔧 Bot Controls", b"admin_controls")],
                    [Button.inline("🏠 Back", b"back_start")]
                ]

                await event.edit(text, parse_mode='html', buttons=buttons)
                return

        if data.startswith("addprm_"):
            # Admin granting premium plan
            if not is_admin(uid):
                return
            
            state = user_states.get(uid, {})
            target_uid = state.get('target_uid')
            
            if not target_uid:
                await event.answer("Session expired!", alert=True)
                return
            
            if data == "addprm_cancel":
                del user_states[uid]
                await event.edit("Cancelled.")
                return
            
            # Extract plan name
            plan_name = data.replace("addprm_", "")
            plan = PLANS.get(plan_name)
            
            if not plan:
                await event.answer("Invalid plan!", alert=True)
                return
            
            # Grant premium with plan name
            set_user_premium(target_uid, plan['max_accounts'], plan_name)
            
            # Notify target user with plan-specific image
            try:
                notification_text = (
                    f"<b>🎉 Plan Activated!</b>\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>Your Plan:</b> {plan['emoji']} <b>{plan['name']}</b>\n"
                    f"<b>Max Accounts:</b> <code>{plan['max_accounts']}</code>\n"
                    f"<b>Duration:</b> <code>30 days</code>\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<i>Your premium plan has been activated by admin! Enjoy all features.</i>"
                )
                
                # Get plan-specific image from PLAN_IMAGES
                plan_image = PLAN_IMAGES.get(plan_name)
                if plan_image and plan_name in ['grow', 'prime', 'dominion']:
                    await main_bot.send_file(target_uid, plan_image, caption=notification_text, parse_mode='html')
                else:
                    await main_bot.send_message(target_uid, notification_text, parse_mode='html')
            except:
                pass
            
            # Confirm to admin
            del user_states[uid]
            await event.edit(
                f"**Premium Granted**\n\n"
                f"User: `{target_uid}`\n"
                f"Plan: {plan['name']}\n"
                f"Accounts: {plan['max_accounts']}\n\n"
                f"User has been notified."
            )
            return

        if data == "noop":
            await event.answer("Account limit reached!")
            return
        
        if data == "back_plans":
            # Return to plan selection screen with welcome/start image
            plan_msg = (
                "<b>Choose Your Plan</b>\n\n"
                "Pick what fits your scale. You can upgrade anytime."
            )

            # Show welcome/start image when returning to plans
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.edit(file=welcome_image, text=plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
            else:
                await event.edit(plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
            return

        if data == "back_start":
            # If force-join is enabled and user isn't joined, show lock screen
            if not await enforce_forcejoin_or_prompt(event, edit=True):
                return

            # Check if user has accounts
            accounts = get_user_accounts(uid)
            
            if len(accounts) > 0:
                # User has accounts, show plan selection
                plan_msg = (
                    "**Choose Your Plan to Continue**\n\n"
                    "• Kai — 3 accounts (₹149)\n"
                    "• Super — 5 accounts (₹249)\n"
                    "• Ultra — 5 accounts (₹349)"
                )
                
                welcome_image = MESSAGES.get('welcome_image', '')
                if welcome_image:
                    try:
                        await event.delete()
                    except:
                        pass
                    await main_bot.send_file(uid, welcome_image, caption=plan_msg, buttons=plan_select_keyboard(uid))
                else:
                    await event.edit(plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
            else:
                # No accounts, show welcome screen
                await event.edit(render_welcome_text(), parse_mode='html', buttons=new_welcome_keyboard())
            return
        
        if data == "enter_dashboard":
            # Force-join gate (extra safety)
            if not await enforce_forcejoin_or_prompt(event, edit=True):
                return

            if not is_approved(uid):
                approve_user(uid)

            text = await db_call(render_dashboard_text, uid)
            
            buttons = main_dashboard_keyboard(uid)
            # Admin button removed (already in main_dashboard_keyboard)
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            # Update account profiles after UI render to keep navigation snappy
            try:
                task = asyncio.create_task(apply_account_profile_templates(uid))
                task.add_done_callback(lambda t: t.exception())
            except Exception:
                pass
            return
        
        if data == "menu_broadcast":
            text = (
                "<b>Broadcast Menu</b>\n\n"
                "Choose an action."
            )
            await event.edit(text, parse_mode='html', buttons=broadcast_menu_keyboard(uid))
            return
        
        if data == "menu_account":
            # Clear any pending account adding state when returning to account menu
            if uid in user_states:
                del user_states[uid]
            
            accounts = get_user_accounts(uid)
            max_acc = get_user_max_accounts(uid)
            text = (
                f"<b>📱 Account Management</b>\n\n"
                f"<b>Accounts:</b> <code>{len(accounts)}/{max_acc}</code>\n\n"
                f"<i>Select an account below or add a new one.</i>"
            )
            await event.edit(text, parse_mode='html', buttons=account_list_keyboard(uid))
            return
        
        if data.startswith("accpage_"):
            page = int(data.split("_")[1])
            accounts = get_user_accounts(uid)
            max_acc = get_user_max_accounts(uid)
            text = (
                f"<b>👤 Account Management</b>\n\n"
                f"<b>Accounts:</b> <code>{len(accounts)}/{max_acc}</code>\n\n"
                f"<i>Page:</i> <code>{page+1}</code>"
            )
            await event.edit(text, parse_mode='html', buttons=account_list_keyboard(uid, page))
            return
        
        if data == "add_account":
            accounts = get_user_accounts(uid)
            max_accounts = get_user_max_accounts(uid)
            if max_accounts <= 0:
                plan_msg = render_plan_select_text()
                await event.edit(plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
                return
            if len(accounts) >= max_accounts:
                await event.answer(f"Account limit reached ({max_accounts})!", alert=True)
                return
            user_states[uid] = {'action': 'phone', 'owner_id': uid}
            await event.edit("**Add Account**\n\nSend phone number with country code:\n\nExample: `+919876543210`", buttons=[[Button.inline("Cancel", b"menu_account")]])
            return
        
        if data == "delete_account_menu":
            accounts = get_user_accounts(uid)
            if not accounts:
                await event.answer("No accounts to delete!", alert=True)
                return
            await event.edit("**Delete Account**\n\nSelect account to delete:", buttons=delete_account_list_keyboard(uid))
            return
        
        if data.startswith("confirm_del_"):
            acc_id = data.replace("confirm_del_", "")
            from bson.objectid import ObjectId
            try:
                acc = accounts_col.find_one({'_id': ObjectId(acc_id), 'user_id': uid})
            except:
                acc = accounts_col.find_one({'_id': acc_id, 'user_id': uid})
            if acc:
                phone = acc['phone']
                await event.edit(
                    f"**Confirm Delete**\n\nAre you sure you want to delete account:\n`{phone}`?",
                    buttons=[
                        [Button.inline("Yes, Delete", f"final_del_{acc_id}"), Button.inline("No, Cancel", b"delete_account_menu")]
                    ]
                )
            return
        
        if data.startswith("final_del_"):
            acc_id = data.replace("final_del_", "")
            from bson.objectid import ObjectId
            try:
                acc = accounts_col.find_one({'_id': ObjectId(acc_id), 'user_id': uid})
            except:
                acc = accounts_col.find_one({'_id': acc_id, 'user_id': uid})
            if acc:
                real_id = acc['_id']
                if real_id in forwarding_tasks:
                    forwarding_tasks[real_id].cancel()
                    del forwarding_tasks[real_id]
                accounts_col.delete_one({'_id': real_id})
                account_topics_col.delete_many({'account_id': real_id})
                account_settings_col.delete_many({'account_id': real_id})
                account_auto_groups_col.delete_many({'account_id': real_id})
                await event.answer("Account deleted!", alert=True)
            await event.edit(
                "<b>👤 Account Management</b>",
                parse_mode='html',
                buttons=account_list_keyboard(uid)
            )
            return
        
        if data == "menu_analytics":
            accounts = get_user_accounts(uid)
            total_sent = 0
            total_failed = 0
            total_groups = 0
            total_auto_replies = 0
            
            _ids = [str(acc['_id']) for acc in accounts]
            if _ids:
                _sd = await db_call(lambda: list(account_stats_col.find(
                    {'account_id': {'$in': _ids}}, {'total_sent': 1, 'total_failed': 1, 'auto_replies': 1})))
                total_sent = sum(s.get('total_sent', 0) for s in _sd)
                total_failed = sum(s.get('total_failed', 0) for s in _sd)
                total_auto_replies = sum(s.get('auto_replies', 0) for s in _sd)
                total_groups = await db_call(lambda: account_auto_groups_col.count_documents({'account_id': {'$in': _ids}}))
            
            active = sum(1 for acc in accounts if acc.get('is_forwarding'))
            
            success_rate = 0.0
            if (total_sent + total_failed) > 0:
                success_rate = (total_sent / (total_sent + total_failed)) * 100

            text = (
                "<b>📊 Analytics</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                "<b>👥 Account Statistics:</b>\n"
                f"├ <b>Total Accounts:</b> <code>{len(accounts)}</code>\n"
                f"├ <b>Active Accounts:</b> <code>{active}</code>\n"
                f"└ <b>Total Groups:</b> <code>{total_groups}</code>\n\n"
                "<b>📈 Message Statistics:</b>\n"
                f"├ <b>✅ Messages Sent:</b> <code>{total_sent}</code>\n"
                f"├ <b>❌ Messages Failed:</b> <code>{total_failed}</code>\n"
                f"├ <b>📊 Success Rate:</b> <code>{success_rate:.1f}%</code>\n"
                f"└ <b>💬 Auto Replies:</b> <code>{total_auto_replies}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )

            await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back", b"menu_broadcast")]])
            return
        
        if data == "admin_banned_users" or data.startswith("banned_page_"):
            if not is_admin(uid):
                return
            
            # Pagination for banned users
            page = 0
            if data.startswith("banned_page_"):
                page = int(data.split("_")[2])
            
            per_page = 5
            skip = page * per_page
            
            # Get banned users
            banned_users = list(users_col.find({'banned': True}).skip(skip).limit(per_page))
            total_banned = users_col.count_documents({'banned': True})
            
            if total_banned == 0:
                await event.edit(
                    "<b>🚫 Banned Users</b>\n\n"
                    "<i>No banned users found.</i>",
                    parse_mode='html',
                    buttons=[[Button.inline("← Back", b"admin_panel")]]
                )
                return
            
            pages = (total_banned + per_page - 1) // per_page
            
            text = (
                f"<b>🚫 Banned Users</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                f"<b>Total Banned:</b> <code>{total_banned}</code>\n"
                f"<b>Current Page:</b> <code>{page + 1}/{pages}</code>\n\n"
                "<b>💡 How to Ban a User:</b>\n"
                "<code>/ban [user_id] [reason]</code>\n\n"
                "<b>📌 Example:</b>\n"
                "<code>/ban 123456789 spam</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            buttons = []
            for user in banned_users:
                user_id = user['user_id']
                reason = user.get('ban_reason', 'No reason')[:20]
                buttons.append([Button.inline(f"🚫 User {user_id} - {reason}", f"banned_user_{user_id}")])
            
            # Pagination
            nav = []
            if page > 0:
                nav.append(Button.inline("⬅️ Prev", f"banned_page_{page-1}"))
            if page < pages - 1:
                nav.append(Button.inline("➡️ Next", f"banned_page_{page+1}"))
            if nav:
                buttons.append(nav)
            
            buttons.append([Button.inline("← Back", b"admin_panel")])
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("banned_user_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.split("_")[2])
            user = users_col.find_one({'user_id': target_id})
            
            if not user or not user.get('banned'):
                await event.answer("User not found or not banned!", alert=True)
                return
            
            reason = user.get('ban_reason', 'No reason provided')
            banned_at = user.get('banned_at')
            banned_date = banned_at.strftime('%d %b %Y %H:%M') if banned_at else 'Unknown'
            
            text = (
                "<b>🚫 Banned User Details</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n"
                f"<b>Ban Reason:</b> <code>{reason}</code>\n"
                f"<b>Banned On:</b> <code>{banned_date}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            buttons = [
                [Button.inline("✅ Unban User", f"unban_{target_id}")],
                [Button.inline("← Back", b"admin_banned_users")]
            ]
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("unban_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.split("_")[1])
            
            # Unban the user
            uupdate(
                {'user_id': target_id},
                {'$set': {
                    'banned': False,
                    'unbanned_at': datetime.now(),
                    'unbanned_by': uid
                }}
            )
            
            # Notify the user
            try:
                await main_bot.send_message(
                    target_id,
                    "<b>✅ You Have Been Unbanned!</b>\n\n"
                    "<i>You can now use the bot again. Welcome back!</i>",
                    parse_mode='html'
                )
            except Exception:
                pass
            
            await event.answer("User has been unbanned!", alert=True)
            
            # Return to banned users list
            await event.edit(
                "<b>🚫 Banned Users</b>\n\n"
                "<i>User has been unbanned successfully!</i>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to List", b"admin_banned_users")], [Button.inline("← Admin Panel", b"admin_panel")]]
            )
            return
        
        if data.startswith("admin_reset_user_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.split("_")[3])
            
            # Get all user's accounts
            accounts = get_user_accounts(target_id)
            accounts_deleted = 0
            tasks_stopped = 0
            
            # Stop all running tasks and delete accounts
            for acc in accounts:
                account_id = str(acc['_id'])
                
                # Stop forwarding task if running
                if forwarding_tasks.get(acc['_id']):
                    tasks_stopped += 1
                ensure_account_stopped(acc['_id'])
                
                # Stop auto reply if running
                if account_id in auto_reply_clients:
                    try:
                        await auto_reply_clients[account_id].disconnect()
                        del auto_reply_clients[account_id]
                    except Exception:
                        pass
                
                # Delete account data
                accounts_col.delete_one({'_id': acc['_id']})
                account_topics_col.delete_many({'account_id': account_id})
                account_auto_groups_col.delete_many({'account_id': account_id})
                account_failed_groups_col.delete_many({'account_id': account_id})
                account_stats_col.delete_one({'account_id': account_id})
                accounts_deleted += 1
            
            # Reset user to free plan
            uupdate(
                {'user_id': target_id},
                {'$set': {
                    'tier': 'free',
                    'plan': 'scout',
                    'plan_name': 'No Plan',
                    'max_accounts': 1,
                    'premium_expires_at': None,
                    'plan_expiry': None,
                    'approved': True
                }}
            )
            
            # Notify the user
            try:
                await main_bot.send_message(
                    target_id,
                    "<b>🔄 Account Reset</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>Accounts Deleted:</b> <code>{accounts_deleted}</code>\n"
                    f"<b>Tasks Stopped:</b> <code>{tasks_stopped}</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<i>Your account has been reset by admin. All data cleared and plan reset to No Plan.</i>",
                    parse_mode='html'
                )
            except Exception:
                pass
            
            await event.answer(f"User {target_id} has been reset!", alert=True)
            
            # Show confirmation
            await event.edit(
                "<b>✅ User Reset Complete</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n"
                f"<b>Accounts Deleted:</b> <code>{accounts_deleted}</code>\n"
                f"<b>Tasks Stopped:</b> <code>{tasks_stopped}</code>\n"
                f"<b>Plan Reset:</b> <code>No Plan</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to Users", b"admin_all_users")], [Button.inline("← Admin Panel", b"admin_panel")]]
            )
            return

        if data.startswith("admin_start_broadcast_"):
            if not is_admin(uid):
                return

            target_id = int(data.replace("admin_start_broadcast_", ""))
            try:
                await apply_account_profile_templates(target_id)
            except Exception:
                pass
            started = await start_broadcast_for_user(target_id)
            await event.answer(f"Started {started} accounts!", alert=True)
            await event.edit(
                f"<b>✅ Broadcast Started</b>\n\n<b>User:</b> <code>{target_id}</code>\n<b>Accounts Started:</b> <code>{started}</code>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to User", f"admin_user_detail_{target_id}")]]
            )
            return

        if data.startswith("admin_stop_broadcast_"):
            if not is_admin(uid):
                return

            target_id = int(data.replace("admin_stop_broadcast_", ""))
            stopped = await stop_broadcast_for_user(target_id, by_admin=True)
            await event.answer(f"Stopped {stopped} accounts!", alert=True)
            await event.edit(
                f"<b>⏹️ Broadcast Stopped</b>\n\n<b>User:</b> <code>{target_id}</code>\n<b>Accounts Stopped:</b> <code>{stopped}</code>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to User", f"admin_user_detail_{target_id}")]]
            )
            return

        if data.startswith("admin_add_account_"):
            if not is_admin(uid):
                return

            target_id = int(data.replace("admin_add_account_", ""))
            user_states[uid] = {'action': 'phone', 'owner_id': target_id}
            await event.edit(
                f"<b>➕ Add Account for User</b>\n\n<b>User:</b> <code>{target_id}</code>\n\nSend phone with country code:\n\n<code>+919876543210</code>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to User", f"admin_user_detail_{target_id}")]]
            )
            return

        if data.startswith("admin_user_accounts_"):
            if not is_admin(uid):
                return

            target_id = int(data.replace("admin_user_accounts_", ""))
            accounts = list(accounts_col.find({'owner_id': target_id}).sort('added_at', -1))
            if not accounts:
                await event.edit(
                    f"<b>User Accounts</b>\n\n<i>No accounts for {target_id}.</i>",
                    parse_mode='html',
                    buttons=[[Button.inline("← Back to User", f"admin_user_detail_{target_id}")]]
                )
                return

            text = (
                f"<b>User Accounts</b>\n\n"
                f"<b>User:</b> <code>{target_id}</code>\n"
                f"<b>Total:</b> <code>{len(accounts)}</code>\n\n"
                "<i>Select an account to remove.</i>"
            )
            buttons = []
            for acc in accounts:
                acc_id = str(acc['_id'])
                phone = acc.get('phone', 'Unknown')
                buttons.append([Button.inline(f"Remove {phone}", f"admin_remove_user_account_{target_id}_{acc_id}")])
            buttons.append([Button.inline("← Back to User", f"admin_user_detail_{target_id}")])
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_remove_user_account_"):
            if not is_admin(uid):
                return

            parts = data.split("_")
            if len(parts) < 6:
                await event.answer("Invalid request", alert=True)
                return
            target_id = int(parts[4])
            acc_id = parts[5] if len(parts) > 5 else ""
            if not acc_id:
                await event.answer("Invalid account id", alert=True)
                return

            from bson.objectid import ObjectId
            acc = None
            try:
                acc = accounts_col.find_one({'_id': ObjectId(acc_id), 'owner_id': target_id})
            except Exception:
                acc = None
            if not acc:
                await event.answer("Account not found", alert=True)
                return

            await delete_account_and_related(acc_id)
            await event.edit(
                f"<b>✅ Account Removed</b>\n\n<b>User:</b> <code>{target_id}</code>\n<b>Phone:</b> <code>{acc.get('phone','')}</code>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to User", f"admin_user_detail_{target_id}")]]
            )
            return
        
        if data == "my_profile":
            user = get_user(uid)
            
            # Check if user is admin for special display
            if is_admin(uid):
                # Admin/God Mode Display
                accounts = get_user_accounts(uid)
                active_accounts = sum(1 for acc in accounts if acc.get('is_forwarding'))
                _ids = [str(acc['_id']) for acc in accounts]
                total_groups = await db_call(lambda: account_auto_groups_col.count_documents(
                    {'account_id': {'$in': _ids}})) if _ids else 0
                _statd = await db_call(lambda: list(account_stats_col.find(
                    {'account_id': {'$in': _ids}}, {'total_sent': 1}))) if _ids else []
                total_messages = sum(s.get('total_sent', 0) for s in _statd)
                
                # Get current settings
                interval_preset = user.get('interval_preset', 'medium')
                if interval_preset == 'custom':
                    custom = user.get('custom_interval', {})
                    interval_str = f"Custom ({custom.get('msg_delay', 30)}s / {custom.get('round_delay', 600)}s)"
                else:
                    preset_info = INTERVAL_PRESETS.get(interval_preset, INTERVAL_PRESETS['medium'])
                    # Show only preset name (Slow/Medium/Fast) without Safe/Balanced/Risky
                    preset_name = interval_preset.capitalize()
                    interval_str = f"{preset_name} ({preset_info['msg_delay']}s / {preset_info['round_delay']}s)"
                
                # Get actual feature status from user settings (not hardcoded for admins)
                auto_reply = "✅ Enabled" if user.get('autoreply_enabled') else "❌ Disabled"
                smart_rotation = "✅ Enabled" if user.get('smart_rotation') else "❌ Disabled"
                # Logs are enabled if logs_chat_id is set
                logs = "✅ Enabled" if user.get('logs_chat_id') else "❌ Disabled"
                
                try:
                    username = f"@{event.sender.username}" if event.sender.username else "Not set"
                except:
                    username = "Not set"
                
                text = (
                    f"<b>👤 My Profile</b>\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>📱 User Details:</b>\n"
                    f"├ <b>User ID:</b> <code>{uid}</code>\n"
                    f"├ <b>Username:</b> <code>{username}</code>\n"
                    f"└ <b>Plan:</b> ⚡ <b>God Mode</b>\n\n"
                    f"<b>💎 Subscription:</b>\n"
                    f"├ <b>Expires On:</b> <code>∞ Never</code>\n"
                    f"└ <b>Days Left:</b> <code>∞ Unlimited</code>\n\n"
                    f"<b>📊 Usage Statistics:</b>\n"
                    f"├ <b>Total Accounts:</b> <code>{len(accounts)}/999</code>\n"
                    f"├ <b>Active Accounts:</b> <code>{active_accounts}</code>\n"
                    f"├ <b>Total Groups:</b> <code>{total_groups}</code>\n"
                    f"└ <b>Total Messages Sent:</b> <code>{total_messages}</code>\n\n"
                    f"<b>⚙️ Current Settings:</b>\n"
                    f"├ <b>Interval:</b> <code>{interval_str}</code>\n"
                    f"├ <b>Auto Reply:</b> {auto_reply}\n"
                    f"├ <b>Smart Rotation:</b> {smart_rotation}\n"
                    f"└ <b>Logs:</b> {logs}\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
            else:
                # Regular User Display
                # Check if user still has active premium
                user_is_premium = is_premium(uid)
                
                if user_is_premium:
                    # User has active premium - show their plan
                    plan_key = normalize_plan_key(user.get('plan') or user.get('plan_name')) or 'grow'
                    plan = PLANS.get(plan_key, PLANS['grow'])
                    plan_display = f"{plan['emoji']} <b>{plan['name']}</b>"
                else:
                    # Premium expired or revoked - reset to no plan
                    plan = PLAN_SCOUT
                    plan_display = "<b>No Plan</b>"
                
                # Calculate expiry and days remaining
                expiry_date = user.get('plan_expiry')
                if expiry_date and user_is_premium:
                    days_remaining = (expiry_date - datetime.now()).days
                    expiry_str = expiry_date.strftime('%d %b %Y')
                    days_str = f"{days_remaining} days" if days_remaining > 0 else "Expired"
                else:
                    expiry_str = "—"
                    days_str = "—"
                
                # Get usage statistics
                accounts = get_user_accounts(uid)
                active_accounts = sum(1 for acc in accounts if acc.get('is_forwarding'))
                total_groups = 0
                total_messages = 0
                
                _ids = [str(acc['_id']) for acc in accounts]
                if _ids:
                    total_groups = await db_call(lambda: account_auto_groups_col.count_documents({'account_id': {'$in': _ids}}))
                    total_messages = await db_call(bulk_total_sent, accounts)
                
                # Get current settings
                interval_preset = user.get('interval_preset', 'medium')
                if interval_preset == 'custom':
                    custom = user.get('custom_interval', {})
                    interval_str = f"Custom ({custom.get('msg_delay', 30)}s / {custom.get('round_delay', 600)}s)"
                else:
                    preset_info = INTERVAL_PRESETS.get(interval_preset, INTERVAL_PRESETS['medium'])
                    # Show only preset name (Slow/Medium/Fast) without Safe/Balanced/Risky
                    preset_name = interval_preset.capitalize()
                    interval_str = f"{preset_name} ({preset_info['msg_delay']}s / {preset_info['round_delay']}s)"
                
                # Fix: Check actual user settings, not just premium status
                auto_reply = "✅ Enabled" if user.get('autoreply_enabled') else "❌ Disabled"
                smart_rotation = "✅ Enabled" if user.get('smart_rotation') else "❌ Disabled"
                # Logs are enabled if logs_chat_id is set
                logs = "✅ Enabled" if user.get('logs_chat_id') else "❌ Disabled"
                
                # Get username
                try:
                    username = f"@{event.sender.username}" if event.sender.username else "Not set"
                except:
                    username = "Not set"
                
                text = (
                    f"<b>👤 My Profile</b>\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>📱 User Details:</b>\n"
                    f"├ <b>User ID:</b> <code>{uid}</code>\n"
                    f"├ <b>Username:</b> <code>{username}</code>\n"
                    f"└ <b>Plan:</b> {plan_display}\n\n"
                    f"<b>💎 Subscription:</b>\n"
                    f"├ <b>Expires On:</b> <code>{expiry_str}</code>\n"
                    f"└ <b>Days Left:</b> <code>{days_str}</code>\n\n"
                    f"<b>📊 Usage Statistics:</b>\n"
                    f"├ <b>Total Accounts:</b> <code>{len(accounts)}/{plan['max_accounts']}</code>\n"
                    f"├ <b>Active Accounts:</b> <code>{active_accounts}</code>\n"
                    f"├ <b>Total Groups:</b> <code>{total_groups}</code>\n"
                    f"└ <b>Total Messages Sent:</b> <code>{total_messages}</code>\n\n"
                    f"<b>⚙️ Current Settings:</b>\n"
                    f"├ <b>Interval:</b> <code>{interval_str}</code>\n"
                    f"├ <b>Auto Reply:</b> {auto_reply}\n"
                    f"├ <b>Smart Rotation:</b> {smart_rotation}\n"
                    f"└ <b>Logs:</b> {logs}\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
            
            await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back to Dashboard", b"enter_dashboard")]])
            return
        
        if data == "menu_interval":
            user = get_user(uid)
            current = user.get('interval_preset', 'medium')
            
            if current == 'custom' and user.get('custom_interval'):
                custom = user['custom_interval']
                text = (
                    "<b>⏱️ Interval Settings</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<b>📋 Current Configuration:</b>\n"
                    "├ <b>Mode:</b> <code>Custom</code>\n"
                    f"├ <b>⏰ Message Delay:</b> <code>{custom['msg_delay']}s</code>\n"
                    f"└ <b>🔄 Cycle Delay:</b> <code>{custom['round_delay']}s</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
            else:
                preset = INTERVAL_PRESETS.get(current, INTERVAL_PRESETS['medium'])
                preset_name = current.capitalize()
                text = (
                    "<b>⏱️ Interval Settings</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<b>📋 Current Configuration:</b>\n"
                    f"├ <b>Mode:</b> <code>{preset_name}</code>\n"
                    f"├ <b>⏰ Message Delay:</b> <code>{preset['msg_delay']}s</code>\n"
                    f"└ <b>🔄 Cycle Delay:</b> <code>{preset['round_delay']}s</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
            
            tph = get_effective_target_per_hour()
            text += f"\n\n🎯 <b>Frequency:</b> <code>~{tph}/hour per group</code> (managed by admin)"
            await event.edit(text, parse_mode='html', buttons=interval_menu_keyboard(uid))
            return
        
        if data.startswith("interval_") and data not in ("interval_locked", "interval_custom"):
            # Handle preset intervals (slow, medium, fast) - available to all plans
            preset_key = data.replace("interval_", "")
            if preset_key in INTERVAL_PRESETS:
                uupdate({'user_id': uid}, {'$set': {'interval_preset': preset_key}})
                preset = INTERVAL_PRESETS[preset_key]
                preset_name = preset_key.capitalize()
                await event.answer(f"Interval set to: {preset_name}", alert=True)

                text = (
                    "<b>⏱️ Interval Settings</b>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    "<b>📋 Current Configuration:</b>\n"
                    f"├ <b>Mode:</b> <code>{preset_name}</code>\n"
                    f"├ <b>⏰ Message Delay:</b> <code>{preset['msg_delay']}s</code>\n"
                    f"└ <b>🔄 Cycle Delay:</b> <code>{preset['round_delay']}s</code>\n\n"
                    "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
                await event.edit(text, parse_mode='html', buttons=interval_menu_keyboard(uid))
            return
        
        if data == "interval_locked":
            # Free plan users trying to access custom intervals
            text = (
                "<b>Paid Plan Feature</b>\n\n"
                "Custom intervals are available on paid plans only.\n"
                "Upgrade to Kai, Super, or Ultra to unlock custom timing."
            )
            await event.edit(text, parse_mode='html', buttons=[
                [Button.inline("💎 View Plans", b"back_plans")],
                [Button.inline("← Back", b"menu_interval")]
            ])
            return
        
        if data == "interval_custom":
            if not is_premium(uid):
                await event.answer("Paid plan only!", alert=True)
                return
            user_states[uid] = {'action': 'custom_interval', 'step': 'msg_delay'}
            await event.edit(
                "⏱️ Custom Interval\n\nEnter message delay in seconds (5-9999):",
                buttons=[[Button.inline("← Back", b"menu_interval")]]
            )
            return

        if data == "interval_freq":
            if not is_admin(uid):
                await event.answer("Frequency is managed by admin.", alert=True)
                return
            user_states[uid] = {'action': 'set_target_freq'}
            await event.edit(
                f"🎯 Global Frequency (admin)\n\nHow many times per hour should EACH group get a message?\n\nCurrent: {get_effective_target_per_hour()}/hr. Max {HARD_MAX_TARGET_PER_HOUR}. Enter 1-{HARD_MAX_TARGET_PER_HOUR}:",
                buttons=[[Button.inline("← Back", b"menu_interval")]]
            )
            return
        
        if data == "menu_topics":
            accounts = get_user_accounts(uid)
            if not accounts:
                await event.answer("Add an account first!", alert=True)
                return
            
            tier_settings = get_user_tier_settings(uid)
            max_topics = tier_settings.get('max_topics', 3)
            
            text = (
                "<b>🏷️ Topics</b>\n\n"
                "<blockquote>Select a topic to add group links.</blockquote>\n\n"
                f"<b>Available topics:</b> <code>{max_topics}/{len(TOPICS)}</code>"
            )
            if not is_premium(uid):
                text += "\n\n<i>Upgrade to a paid plan for all topics.</i>"
            
            buttons = []
            _topic_list = list(TOPICS[:max_topics])
            _acc_ids = [acc['_id'] for acc in accounts]
            _topic_counts = {}
            if _acc_ids:
                _agg = await db_call(lambda: list(account_topics_col.aggregate([
                    {'$match': {'account_id': {'$in': _acc_ids}, 'topic': {'$in': _topic_list}}},
                    {'$group': {'_id': '$topic', 'n': {'$sum': 1}}},
                ])))
                _topic_counts = {d['_id']: d['n'] for d in _agg}
            for i, topic in enumerate(_topic_list):
                count = _topic_counts.get(topic, 0)
                buttons.append([Button.inline(f"{topic.title()} ({count} groups)", f"topic_select_{topic}")])
            
            if not is_premium(uid) and len(TOPICS) > max_topics:
                buttons.append([Button.inline("Unlock More Topics", b"go_premium")])
            
            buttons.append([Button.inline("Back", b"menu_settings_content")])
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("topic_select_"):
            topic = data.replace("topic_select_", "")
            accounts = get_user_accounts(uid)
            
            tier_settings = get_user_tier_settings(uid)
            max_groups = tier_settings.get('max_groups_per_topic', 10)
            
            if len(accounts) == 1:
                acc = accounts[0]
                groups = list(account_topics_col.find({'account_id': acc['_id'], 'topic': topic}))
                
                text = (
                    f"<b>{_h(topic.title())}</b>\n\n"
                    f"<b>Groups:</b> <code>{len(groups)}/{max_groups}</code>\n\n"
                    "<b>Send topic link to add:</b>\n"
                    "<code>https://t.me/groupname/5</code>"
                )
                buttons = [[Button.inline("View Groups", f"view_topic_groups_{topic}_{acc['_id']}")]] if groups else []
                buttons.append([Button.inline("Back", b"menu_topics")])
                msg = await event.edit(text, parse_mode='html', buttons=buttons)
                user_states[uid] = {'action': 'add_topic_link', 'topic': topic, 'account_id': acc['_id'], 'last_msg_id': msg.id if hasattr(msg, 'id') else event.message_id}
            else:
                text = f"<b>{_h(topic.title())}</b>\n\n<i>Select account to add groups:</i>"
                buttons = []
                _aids = [acc['_id'] for acc in accounts]
                _agg = await db_call(lambda: list(account_topics_col.aggregate([
                    {'$match': {'account_id': {'$in': _aids}, 'topic': topic}},
                    {'$group': {'_id': '$account_id', 'n': {'$sum': 1}}},
                ]))) if _aids else []
                _cba = {str(d['_id']): d['n'] for d in _agg}
                for acc in accounts:
                    phone = acc['phone'][-4:]
                    name = acc.get('name', 'Unknown')[:12]
                    count = _cba.get(str(acc['_id']), 0)
                    buttons.append([Button.inline(f"{phone} - {name} ({count})", f"topic_acc_{topic}_{acc['_id']}")])
                buttons.append([Button.inline("Back", b"menu_topics")])
                await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("topic_acc_"):
            parts = data.replace("topic_acc_", "").split("_", 1)
            topic = parts[0]
            acc_id = parts[1] if len(parts) > 1 else ""
            
            tier_settings = get_user_tier_settings(uid)
            max_groups = tier_settings.get('max_groups_per_topic', 10)
            groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}))
            
            text = (
                f"<b>🏷️ {topic.title()}</b>\n\n"
                f"<b>Groups:</b> <code>{len(groups)}/{max_groups}</code>\n\n"
                "<i>Send a topic link to add.</i>\n"
                "<code>Example: https://t.me/groupname/5</code>"
            )
            buttons = [[Button.inline("👁️ View Groups", f"view_topic_groups_{topic}_{acc_id}")]] if groups else []
            buttons.append([Button.inline("← Back", f"topic_select_{topic}")])
            msg = await event.edit(text, parse_mode='html', buttons=buttons)
            user_states[uid] = {'action': 'add_topic_link', 'topic': topic, 'account_id': acc_id, 'last_msg_id': msg.id if hasattr(msg, 'id') else event.message_id}
            return
        
        if data.startswith("view_topic_groups_"):
            parts = data.replace("view_topic_groups_", "").split("_", 1)
            topic = parts[0]
            acc_id = parts[1] if len(parts) > 1 else ""
            
            groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}))
            total = len(groups)
            display_limit = 5
            
            text = f"<b>🏷️ {topic.title()} Groups</b> <code>({total} total)</code>\n\n"
            for i, g in enumerate(groups[:display_limit]):
                title = g.get('title', g.get('url', 'Unknown'))[:25]
                text += f"{i+1}. {title}\n"
            
            if total > display_limit:
                text += f"\n...and {total - display_limit} more groups"
            
            buttons = [
                [Button.inline("Clear All", f"clear_topic_{topic}_{acc_id}")],
                [Button.inline("Back", f"topic_select_{topic}")]
            ]
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("clear_topic_"):
            parts = data.replace("clear_topic_", "").split("_", 1)
            topic = parts[0]
            acc_id = parts[1] if len(parts) > 1 else ""
            
            account_topics_col.delete_many({'account_id': acc_id, 'topic': topic})
            await event.answer(f"Cleared all {topic} groups!", alert=True)
            await event.edit(
                f"<b>🏷️ {topic.title()}</b>\n\n<b>Groups:</b> <code>0</code>\n\n<i>Send a group link to add.</i>",
                parse_mode='html',
                buttons=[[Button.inline("← Back", b"menu_topics")]]
            )
            return
        
        # Locked premium-only buttons in Settings menu
        if data in {"locked_smart_rotation", "locked_auto_group_join"}:
            await event.edit(
                "<b>Paid Plan Feature</b>\n\nUpgrade to a paid plan to unlock this feature.",
                parse_mode='html',
                buttons=[[Button.inline("💎 View Plans", b"back_plans")], [Button.inline("← Back", b"menu_settings_automation")]]
            )
            return
        
        if data in {"locked_autoreply", "locked_topics"}:
            await event.edit(
                "<b>Paid Plan Feature</b>\n\nUpgrade to a paid plan to unlock this feature.",
                parse_mode='html',
                buttons=[[Button.inline("💎 View Plans", b"back_plans")], [Button.inline("← Back", b"menu_settings_content")]]
            )
            return
        
        # Locked forwarding mode options (Topics Only and Both)
        if data == "locked_fwd_mode":
            await event.edit(
                "<b>Paid Plan Feature</b>\n\nUpgrade to a paid plan to unlock this forwarding mode.",
                parse_mode='html',
                buttons=[[Button.inline("💎 View Plans", b"back_plans")], [Button.inline("← Back", b"menu_fwd_mode")]]
            )
            return

        if data == "menu_settings":
            user_doc = get_user(uid)
            tier_settings = get_user_tier_settings(uid)
            
            # Get current settings
            ads_mode = user_doc.get('ads_mode', 'saved').upper()
            
            # Auto-reply status (check if user has explicitly enabled it AND set a message)
            auto_reply_feature_available = tier_settings.get('auto_reply_enabled', False)
            if auto_reply_feature_available:
                user = get_user(uid)
                enabled_by_user = user.get('autoreply_enabled', False)  # Change default to False
                
                # Also check if user has actually set an auto-reply message
                user_accounts = get_user_accounts(uid)
                has_auto_reply_message = False
                if user_accounts:
                    for acc in user_accounts:
                        settings = get_account_settings(str(acc.get('_id')))
                        if settings.get('auto_reply'):
                            has_auto_reply_message = True
                            break
                
                # Show ON only if enabled AND has message
                auto_reply_status = "✅ ON" if (enabled_by_user and has_auto_reply_message) else "❌ OFF"
            else:
                auto_reply_status = "❌ OFF"
            
            # Interval - Show delays with preset name (Slow/Medium/Fast, not Safe/Balanced/Risky)
            preset = user_doc.get('interval_preset', 'medium')
            if preset == 'custom':
                custom = user_doc.get('custom_interval', {})
                interval_display = f"Custom ({custom.get('msg_delay', 30)}s / {custom.get('round_delay', 600)}s)"
            else:
                preset_info = INTERVAL_PRESETS.get(preset, INTERVAL_PRESETS['medium'])
                msg_delay = preset_info['msg_delay']
                round_delay = preset_info['round_delay']
                # Show only preset name (Slow/Medium/Fast) without Safe/Balanced/Risky
                preset_name = preset.capitalize()
                interval_display = f"{preset_name} ({msg_delay}s / {round_delay}s)"
            
            # Smart Rotation
            smart_rotation = user_doc.get('smart_rotation', False)
            rotation_status = "✅ ON" if smart_rotation else "❌ OFF"
            
            # Logs
            logs_enabled = bool(user_doc.get('logs_chat_id'))
            logs_status = "✅ Enabled" if logs_enabled else "❌ Disabled"
            
            # Auto Leave
            auto_leave_enabled = user_doc.get('auto_leave_groups', True)
            leave_status = "✅ ON" if auto_leave_enabled else "❌ OFF"
            
            text = (
                "<b>⚙️ Settings</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                "<b>📋 Current Configuration:</b>\n"
                f"├ <b>📣 Ads Mode:</b> <code>{ads_mode}</code>\n"
                f"├ <b>💬 Auto-Reply:</b> <code>{auto_reply_status}</code>\n"
                f"├ <b>⏱️ Interval:</b> <code>{interval_display}</code>\n"
                f"├ <b>🔄 Smart Rotation:</b> <code>{rotation_status}</code>\n"
                f"├ <b>📝 Logs:</b> <code>{logs_status}</code>\n"
                f"└ <b>🚫 Auto Leave Failed:</b> <code>{leave_status}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            await event.edit(text, parse_mode='html', buttons=settings_menu_keyboard(uid))
            return

        if data == "menu_settings_automation":
            text = (
                "<b>Automation Tools</b>\n\n"
                "Manage rotation, group join, quiet hours, refresh, and auto leave."
            )
            await event.edit(text, parse_mode='html', buttons=settings_automation_keyboard(uid))
            return

        if data == "menu_quiet_hours":
            if uid in user_states and user_states[uid].get('action') == 'quiet_hours':
                del user_states[uid]
            text, buttons = quiet_hours_menu(uid)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("quiet_preset_"):
            preset_map = {
                "quiet_preset_0100_0700": ("01:00", "07:00"),
                "quiet_preset_0000_0600": ("00:00", "06:00"),
                "quiet_preset_0000_0700": ("00:00", "07:00"),
            }
            if data not in preset_map:
                await event.answer("Invalid preset", alert=True)
                return

            start, end = preset_map[data]
            label = f"{start}-{end}"
            uupdate(
                {'user_id': int(uid)},
                {'$set': {'quiet_hours': {'enabled': True, 'start': start, 'end': end, 'label': label}}},
                upsert=True
            )
            await event.answer(f"Quiet hours set: {label}", alert=True)
            text, buttons = quiet_hours_menu(uid)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data == "quiet_custom":
            user_states[uid] = {'action': 'quiet_hours', 'step': 'start'}
            await event.edit(
                "<b>Quiet Hours - Custom</b>\n\n"
                "Send the <b>start time</b> in 24h format (HH:MM).\n"
                "Example: <code>01:00</code>",
                parse_mode='html',
                buttons=[[Button.inline("Back", b"menu_quiet_hours")]]
            )
            return

        if data == "menu_settings_content":
            text = (
                "<b>Messaging & Logs</b>\n\n"
                "Manage ads mode, logs, topics, and auto reply."
            )
            await event.edit(text, parse_mode='html', buttons=settings_content_keyboard(uid))
            return
        
        if data == "toggle_auto_leave":
            user_doc = get_user(uid)
            current = user_doc.get('auto_leave_groups', True)
            new_value = not current
            uupdate({'user_id': int(uid)}, {'$set': {'auto_leave_groups': new_value}}, upsert=True)
            
            status = "enabled" if new_value else "disabled"
            await event.answer(f"Auto Leave Failed {status}!", alert=True)
            text = (
                "<b>Automation Tools</b>\n\n"
                "Manage rotation, group join, refresh, and auto leave."
            )
            await event.edit(text, parse_mode='html', buttons=settings_automation_keyboard(uid))
            return
        
        if data == "refresh_all_groups":
            # Refresh All Groups is FREE for everyone
            accounts = get_user_accounts(uid)
            if not accounts:
                await event.answer("Add an account first!", alert=True)
                return
            
            progress_msg = await event.respond("<b>🔄 Refreshing all groups...</b>", parse_mode='html')
            
            results = []
            for acc in accounts:
                account_id = str(acc['_id'])
                phone = acc.get('phone', 'Unknown')[-4:]
                
                try:
                    session_enc = acc.get('session')
                    if not session_enc:
                        results.append(f"<code>{phone}</code>: Session not found")
                        continue
                    
                    session = cipher_suite.decrypt(session_enc.encode()).decode()
                    client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                    
                    await client.connect()
                    if not await client.is_user_authorized():
                        results.append(f"<code>{phone}</code>: Session expired")
                        await client.disconnect()
                        continue
                    
                    # Count groups before refresh
                    before_count = account_auto_groups_col.count_documents({'account_id': account_id})
                    
                    # Refresh groups
                    count = await refresh_account_groups(client, account_id)
                    
                    # Count after refresh
                    after_count = account_auto_groups_col.count_documents({'account_id': account_id})
                    
                    if after_count > before_count:
                        results.append(f"<code>{phone}</code>: {before_count} → {after_count} (+{after_count - before_count})")
                    else:
                        results.append(f"<code>{phone}</code>: {before_count} (no new groups)")
                    
                    await client.disconnect()
                    
                except Exception as e:
                    results.append(f"<code>{phone}</code>: Error - {str(e)[:30]}")
            
            if results:
                result_text = "<b>🔄 Refresh Complete</b>\n\n" + "\n".join(results)
            else:
                result_text = "<b>❌ No groups refreshed</b>"
            
            await progress_msg.edit(result_text, parse_mode='html', buttons=[[Button.inline("Back", b"menu_settings_automation")]])
            return
        
        if data == "menu_autoreply":
            tier = "Premium" if is_premium(uid) else "No Plan"
            text = f"<b>💬 Auto Reply</b>\n\n<b>Tier:</b> <code>{tier}</code>\n\n"
            
            if is_premium(uid):
                user = get_user(uid)
                enabled = user.get('autoreply_enabled', True)
                
                # Check if user has set a custom message
                accounts = get_user_accounts(uid)
                has_custom = any_account_has_autoreply(accounts)
                
                text += f"<b>Status:</b> <code>{'ON' if enabled else 'OFF'}</code>\n"
                text += f"<b>Custom Reply:</b> {'✅' if has_custom else '❌'} <code>{'Set' if has_custom else 'Not Set'}</code>"
            else:
                text += "🔒 <b>Auto-reply is a premium feature.</b>\n\n"
                text += "Upgrade to premium to set custom auto-reply messages!"
            
            await event.edit(text, parse_mode='html', buttons=autoreply_menu_keyboard(uid))
            return
        
        if data == "autoreply_view":
            if not is_premium(uid):
                await event.answer("Paid plan only!", alert=True)
                return
            
            # Get custom message from account settings
            accounts = get_user_accounts(uid)
            reply = None
            if accounts:
                _ids = [str(acc['_id']) for acc in accounts]
                _d = account_settings_col.find_one(
                    {'account_id': {'$in': _ids}, 'auto_reply': {'$exists': True}}, {'auto_reply': 1})
                if _d:
                    reply = _d.get('auto_reply')
            
            if reply:
                text = f"<b>💬 Current Auto Reply</b>\n\n<blockquote>{_h(reply)}</blockquote>"
            else:
                text = "<b>💬 Current Auto Reply</b>\n\n<i>No custom message set yet.</i>"
            
            await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back", b"menu_autoreply")]])
            return

        if data == "autoreply_toggle":
            if not is_premium(uid):
                await event.answer("Paid plan feature only", alert=True)
                return

            # Flip the flag and refresh menu
            user = get_user(uid)
            enabled = user.get('autoreply_enabled', True)
            new_value = not enabled
            uupdate({'user_id': int(uid)}, {'$set': {'autoreply_enabled': new_value}})

            try:
                await event.answer(f"Auto Reply {'enabled' if new_value else 'disabled'}", alert=False)
            except Exception:
                pass

            # Re-render menu
            tier = "Premium"
            user = get_user(uid)
            text = f"<b>💬 Auto Reply</b>\n\n<b>Tier:</b> <code>{tier}</code>\n\n"
            enabled = user.get('autoreply_enabled', True)
            
            # Check if user has set a custom message
            accounts = get_user_accounts(uid)
            has_custom = any_account_has_autoreply(accounts)
            
            text += f"<b>Status:</b> <code>{'ON' if enabled else 'OFF'}</code>\n"
            text += f"<b>Custom Reply:</b> {'✅' if has_custom else '❌'} <code>{'Set' if has_custom else 'Not Set'}</code>"
            await event.edit(text, parse_mode='html', buttons=autoreply_menu_keyboard(uid))
            return
        
        if data == "autoreply_custom":
            if not is_premium(uid):
                await event.answer("Paid plan only!", alert=True)
                return
            user_states[uid] = {'action': 'custom_autoreply'}
            await event.edit(
                "<b>💬 Set Custom Reply</b>\n\nSend your custom auto-reply message:",
                parse_mode='html',
                buttons=[[Button.inline("← Back", b"menu_autoreply")]]
            )
            return
        
        if data == "go_premium":
            # Show plan selection menu for everyone
            plan_msg = (
                "**Choose Your Plan**\n\n"
                "Pick what fits your scale. You can upgrade anytime.\n\n"
                "• Kai — 3 accounts (₹149)\n"
                "• Super — 5 accounts (₹249)\n"
                "• Ultra — 5 accounts (₹349)"
            )
            
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                try:
                    await event.delete()
                except:
                    pass
                await main_bot.send_file(uid, welcome_image, caption=plan_msg, buttons=plan_select_keyboard(uid))
            else:
                await event.edit(plan_msg, buttons=plan_select_keyboard(uid))
            return
        
        if data.startswith("buy_"):
            plan = data.replace("buy_", "")
            plan_label = get_plan_label(plan)
            prices = {"1month": "$20", "3months": "$50", "6months": "$70"}
            price = prices.get(plan, "$20")
            
            owner_id = CONFIG['owner_id']
            try:
                await main_bot.send_message(owner_id, f"**Plan Purchase Request**\n\nUser ID: `{uid}`\nPlan: {plan_label}\nPrice: {price}")
            except:
                pass
            
            await event.edit(
                f"**Request sent**\n\nPlan: {plan_label}\nPrice: {price}\n\nAdmin has been notified.\nThey will contact you shortly.\n\nYour User ID: `{uid}`",
                buttons=[[Button.inline("Back", b"go_premium")]]
            )
            return
        
        if data == "account_limit_reached":
            if get_user_max_accounts(uid) <= 0:
                plan_msg = render_plan_select_text()
                await event.edit(plan_msg, parse_mode='html', buttons=plan_select_keyboard(uid))
            else:
                await event.edit(
                    "**Account limit reached**\n\nYou have reached your current plan limit. Upgrade to add more accounts.",
                    buttons=[
                        [Button.inline("View Plans", b"go_premium")],
                        [Button.inline("Back", b"menu_account")]
                    ]
                )
            return
        
        if data == "menu_logs":
            logger_bot_username = CONFIG.get('logger_bot_username', 'logstesthubot')
            logger_link = f"https://t.me/{logger_bot_username}"

            user_doc = get_user(uid)
            enabled = bool(user_doc.get('logs_chat_id'))
            status = "✅ Enabled" if enabled else "❌ Disabled"

            buttons = [[Button.url("Start Logger Bot", logger_link)]]
            if enabled:
                buttons.append([Button.inline("Disable Logs", b"logs_disable_global")])
            else:
                buttons.append([Button.inline("Enable Logs", b"logs_enable_global")])
            buttons.append([Button.inline("Back", b"menu_settings_content")])

            await event.edit(
                "<b>📝 Logs</b>\n\n"
                "<blockquote>Once enabled, logs will be sent for <b>all</b> your added accounts.</blockquote>\n\n"
                f"<b>Status:</b> <code>{status}</code>",
                parse_mode='html',
                buttons=buttons
            )
            return

        # ===================== Ads Mode (Saved/Custom/Post Link) =====================
        if data == "menu_ads_mode":
            user_doc = get_user(uid)
            mode = user_doc.get('ads_mode', 'saved')
            modes = {
                'saved': 'Saved Message',
                'custom': 'Custom Message',
                'post': 'Post Link'
            }
            text = (
                "<b>📣 Ads Mode</b>\n\n"
                "<blockquote>Select which message will be used while running ads.</blockquote>\n\n"
                f"<b>Current:</b> <code>{modes.get(mode, mode)}</code>"
            )
            buttons = [
                [Button.inline("Saved Message" + (" ✅" if mode == 'saved' else ""), b"ads_mode_saved")],
                [Button.inline("Set Custom Message" + (" ✅" if mode == 'custom' else ""), b"ads_mode_custom")],
                [Button.inline("Set Post Link" + (" ✅" if mode == 'post' else ""), b"ads_mode_post")],
                [Button.inline("← Back", b"menu_settings_content")]
            ]
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data == "ads_mode_saved":
            uupdate({'user_id': uid}, {'$set': {'ads_mode': 'saved'}}, upsert=True)
            await event.answer("Ads Mode set: Saved Message", alert=True)
            await event.edit("<b>✅ Ads Mode Updated</b>\n\nNow bot will use each account's <b>Saved Messages</b> for forwarding.", parse_mode='html', buttons=[[Button.inline("← Back", b"menu_ads_mode")]])
            return

        if data == "ads_mode_custom":
            uupdate({'user_id': uid}, {'$set': {'ads_mode': 'custom'}}, upsert=True)
            cur = (get_user(uid).get('ads_custom_message') or '').strip()
            preview = _h(cur[:500]) if cur else '<i>Not set</i>'
            text = (
                "<b>✍️ Custom Message</b>\n\n"
                "<b>Current Message:</b>\n"
                f"{preview}\n\n"
                "Send a new message to update it."
            )
            await event.edit(
                text,
                parse_mode='html',
                buttons=[
                    [Button.inline("Set Message", b"ads_custom_set")],
                    [Button.inline("View Current", b"ads_custom_view")],
                    [Button.inline("← Back", b"menu_ads_mode")]
                ]
            )
            return

        if data == "ads_custom_view":
            cur = (get_user(uid).get('ads_custom_message') or '').strip()
            preview = _h(cur) if cur else '<i>Not set</i>'
            await event.edit(f"<b>✍️ Current Custom Message</b>\n\n{preview}", parse_mode='html', buttons=[[Button.inline("← Back", b"ads_mode_custom")]])
            return

        if data == "ads_custom_set":
            user_states[uid] = {'action': 'set_ads_custom_message'}
            await event.edit("<b>✍️ Send your custom message now</b>\n\n<i>Next message you send will be saved and used for ads.</i>", parse_mode='html', buttons=[[Button.inline("← Cancel", b"menu_ads_mode")]])
            return

        if data == "ads_mode_post":
            uupdate({'user_id': uid}, {'$set': {'ads_mode': 'post'}}, upsert=True)
            cur = (get_user(uid).get('ads_post_link') or '').strip()
            preview = _h(cur) if cur else '<i>Not set</i>'
            await event.edit(
                "<b>🔗 Post Link</b>\n\n"
                f"<b>Current Link:</b> {preview}\n\n"
                "Send a Telegram post link like:\n"
                "<code>https://t.me/username/123</code>\n"
                "or\n"
                "<code>https://t.me/c/123456/789</code>",
                parse_mode='html',
                buttons=[
                    [Button.inline("Set Link", b"ads_post_set")],
                    [Button.inline("View Current", b"ads_post_view")],
                    [Button.inline("← Back", b"menu_ads_mode")]
                ]
            )
            return

        if data == "ads_post_view":
            cur = (get_user(uid).get('ads_post_link') or '').strip()
            preview = _h(cur) if cur else '<i>Not set</i>'
            await event.edit(f"<b>🔗 Current Post Link</b>\n\n{preview}", parse_mode='html', buttons=[[Button.inline("← Back", b"ads_mode_post")]])
            return

        if data == "ads_post_set":
            user_states[uid] = {'action': 'set_ads_post_link'}
            await event.edit("<b>🔗 Send post link now</b>\n\n<i>Next message you send should be a Telegram post link.</i>", parse_mode='html', buttons=[[Button.inline("← Cancel", b"menu_ads_mode")]])
            return
        
        # ===================== Smart Rotation (Premium) =====================
        if data == "menu_smart_rotation":
            if not is_premium(uid):
                await event.edit(
                    "<b>Paid Plan Feature</b>\n\nUpgrade to a paid plan to unlock Smart Rotation.",
                    parse_mode='html',
                    buttons=[[Button.inline("💎 View Plans", b"back_plans")], [Button.inline("← Back", b"menu_settings_automation")]]
                )
                return
            
            # Check if user has any accounts
            user_accounts = list(accounts_col.find({"owner_id": uid}))
            if not user_accounts:
                await event.answer("❌ Please add an account first!", alert=True)
                return
            
            # Get user settings (stored separately with user_id)
            user_settings = users_col.find_one({"user_id": uid})
            if not user_settings:
                user_settings = {}
            current = user_settings.get('smart_rotation', False)
            
            await event.edit(
                "<b>🔄 Smart Rotation</b>\n\n"
                "<blockquote>When enabled, the bot will randomly shuffle the order of your target groups before each forwarding round.\n\n"
                "This makes your forwarding pattern unpredictable and more natural, helping avoid detection and rate limits.</blockquote>\n\n"
                f"<b>Status:</b> {'✅ Enabled' if current else '❌ Disabled'}",
                parse_mode='html',
                buttons=[
                    [Button.inline("✅ Enable" if not current else "❌ Disable", b"toggle_smart_rotation")],
                    [Button.inline("\u2190 Back", b"menu_settings_automation")]
                ]
            )
            return
        
        if data == "toggle_smart_rotation":
            if not is_premium(uid):
                await event.answer("⭐ Paid plan feature only!", alert=True)
                return
            
            # Get current state from users collection
            user_settings = users_col.find_one({"user_id": uid})
            if not user_settings:
                user_settings = {}
            current = user_settings.get('smart_rotation', False)
            new_val = not current
            
            # Save to users collection
            uupdate(
                {"user_id": uid},
                {"$set": {"smart_rotation": new_val}},
                upsert=True
            )
            
            await event.edit(
                "<b>🔄 Smart Rotation</b>\n\n"
                "<blockquote>When enabled, the bot will randomly shuffle the order of your target groups before each forwarding round.\n\n"
                "This makes your forwarding pattern unpredictable and more natural, helping avoid detection and rate limits.</blockquote>\n\n"
                f"<b>Status:</b> {'✅ Enabled' if new_val else '❌ Disabled'}",
                parse_mode='html',
                buttons=[
                    [Button.inline("✅ Enable" if not new_val else "❌ Disable", b"toggle_smart_rotation")],
                    [Button.inline("\u2190 Back", b"menu_settings_automation")]
                ]
            )
            return
        
        # ===================== Auto Group Join (Premium) =====================
        if data == "menu_auto_group_join":
            if not is_premium(uid):
                await event.answer("⭐ Paid plan feature only!", alert=True)
                return
            
            # Check if user has any accounts
            user_accounts = list(accounts_col.find({"owner_id": uid}))
            if not user_accounts:
                await event.answer("❌ Please add an account first!", alert=True)
                return
            
            await event.edit(
                "<b>👥 Auto Group Join</b>\n\n"
                "<blockquote>Upload a .txt file with group links (one per line), and all your logged-in accounts will automatically join those groups.\n\n"
                "Supported formats:\n"
                "• https://t.me/groupname\n"
                "• t.me/groupname\n"
                "• @groupname</blockquote>\n\n"
                "Send the .txt file now, or tap Back to cancel.",
                parse_mode='html',
                buttons=[
                    [Button.inline("\u2190 Back", b"menu_settings_automation")]
                ]
            )
            # Set user state to expect .txt file
            user_states[uid] = {'state': 'awaiting_group_join_file'}
            return

        if data == "logs_enable_global":
            # Enable logs globally for user (applies to all accounts)
            uupdate({'user_id': int(uid)}, {'$set': {'logs_chat_id': int(uid)}}, upsert=True)
            invalidate_logs_cache()
            await event.answer("Logs enabled", alert=True)
            await event.edit(
                "<b>✅ Logs Enabled</b>\n\n<blockquote>Logs will now be sent for <b>all</b> your added accounts.</blockquote>",
                parse_mode='html',
                buttons=[[Button.inline("Back", b"menu_logs")]]
            )
            return

        if data == "logs_disable_global":
            uupdate({'user_id': int(uid)}, {'$unset': {'logs_chat_id': ""}})
            invalidate_logs_cache()
            await event.answer("Logs disabled", alert=True)
            await event.edit(
                "<b>❌ Logs Disabled</b>\n\n<i>You will no longer receive logs.</i>",
                parse_mode='html',
                buttons=[[Button.inline("Back", b"menu_logs")]]
            )
            return

        if data == "menu_fwd_mode":
            user = get_user(uid)
            current = user.get('forwarding_mode', 'auto')  # Free users default to Groups Only
            user_is_premium = is_premium(uid)
            
            modes = {
                'topics': 'Forward to Topics Only',
                'auto': 'Forward to Groups Only',
                'both': 'Forward to Both (Topics first, then Groups)'
            }
            
            text = (
                "<b>🔄 Mode</b>\n\n"
                "Select how ads should be forwarded."
            )
            
            if not user_is_premium:
                text += "\n\n<i>Non-premium accounts can forward to groups only. Upgrade for more options.</i>"
            
            buttons = []
            for mode, label in modes.items():
                mark = " ✅" if mode == current else ""
                
                # Lock topics and both for free users
                if not user_is_premium and mode in ['topics', 'both']:
                    buttons.append([Button.inline(f"{label} 🔒", b"locked_fwd_mode")])
                else:
                    buttons.append([Button.inline(f"{label}{mark}", f"set_fwd_mode_{mode}")])
            
            buttons.append([Button.inline("← Back", b"menu_broadcast")])
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("set_fwd_mode_"):
            mode = data.replace("set_fwd_mode_", "")
            uupdate({'user_id': uid}, {'$set': {'forwarding_mode': mode}})
            modes = {
                'topics': 'Forward to Topics Only',
                'auto': 'Forward to Groups Only',
                'both': 'Forward to Both (Topics first, then Groups)'
            }
            await event.answer(f"Mode set: {modes.get(mode, mode)}", alert=True)
            
            text = (
                "<b>🔄 Mode</b>\n\n"
                "Select how ads should be forwarded."
            )
            
            buttons = []
            for m, label in modes.items():
                mark = " ✅" if m == mode else ""
                buttons.append([Button.inline(f"{label}{mark}", f"set_fwd_mode_{m}")])
            buttons.append([Button.inline("← Back", b"menu_broadcast")])
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data == "menu_refresh":
            accounts = get_user_accounts(uid)
            total_groups = 0
            for acc in accounts:
                try:
                    session = cipher_suite.decrypt(acc['session'].encode()).decode()
                    client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                    await client.connect()
                    count = await fetch_groups_for_account(client, acc['_id'])
                    total_groups += count
                    await client.disconnect()
                except:
                    pass
            await event.answer(f"Refreshed! Found {total_groups} groups.", alert=True)
            
            text = await db_call(render_dashboard_text, uid)
            buttons = main_dashboard_keyboard(uid)
            # Admin button removed (already in main_dashboard_keyboard)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data == "start_all_ads":
            # Update all added accounts profile (last name + bio) when starting ads
            try:
                await apply_account_profile_templates(uid)
            except Exception:
                pass

            accounts = get_user_accounts(uid)
            if not accounts:
                await event.answer("No accounts to start!", alert=True)
                return

            started = await start_broadcast_for_user(uid)
            await event.answer(f"Started {started} accounts!", alert=True)

            text = await db_call(render_dashboard_text, uid)
            buttons = main_dashboard_keyboard(uid)
            # Admin button removed (already in main_dashboard_keyboard)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return


        if data == "stop_all_ads":
            stopped = await stop_broadcast_for_user(uid)
            await event.answer(f"Stopped {stopped} accounts!", alert=True)

            text = await db_call(render_dashboard_text, uid)
            buttons = main_dashboard_keyboard(uid)
            # Admin button removed (already in main_dashboard_keyboard)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return


        if data == "tier_free":
            if not is_approved(uid):
                approve_user(uid)
            
            accounts = get_user_accounts(uid)
            max_acc = get_user_max_accounts(uid)
            tier_settings = get_user_tier_settings(uid)
            tier = "Premium" if is_premium(uid) else "No Plan"
            active = sum(1 for a in accounts if a.get('is_forwarding'))
            
            text = (
                f"<b>{tier} Hub</b>\n\n"
                f"<b>Accounts:</b> <code>{len(accounts)}/{max_acc}</code>\n"
                f"<b>Active:</b> <code>{active}</code> | <b>Inactive:</b> <code>{len(accounts) - active}</code>\n\n"
                f"<b>Delays:</b> <code>{tier_settings['msg_delay']}s msg / {tier_settings['round_delay']}s round</code>"
            )

            await event.edit(text, parse_mode='html', buttons=account_list_keyboard(uid))
            return
        
        if data == "tier_premium":
            if is_premium(uid):
                await event.edit(
                    "**Plan Active**\n\nYour paid plan is already active.",
                    buttons=[[Button.inline("Open Dashboard", b"tier_free")], [Button.inline("Back", b"enter_dashboard")]]
                )
            else:
                await event.edit(
                    f"**Plan Access**\n\n{MESSAGES['premium_contact']}",
                    buttons=premium_contact_keyboard()
                )
            return
        
        if data == "admin_panel" or data == "back_admin":
            if not is_admin(uid):
                await event.answer("Admin only!", alert=True)
                return
            
            total_users = users_col.count_documents({})
            premium_users = users_col.count_documents({'tier': 'premium'})
            total_accounts = accounts_col.count_documents({})
            active = accounts_col.count_documents({'is_forwarding': True})
            total_admins = admins_col.count_documents({}) + 1
            
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            new_today = users_col.count_documents({'created_at': {'$gte': today_start}})
            
            text = (
                "<b>⚙️ Admin Panel</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                "<b>👥 User Statistics:</b>\n"
                f"├ <b>Total Users:</b> <code>{total_users}</code> <i>(+{new_today} today)</i>\n"
                f"├ <b>💎 Premium Users:</b> <code>{premium_users}</code>\n"
                f"└ <b>👨‍💼 Total Admins:</b> <code>{total_admins}</code>\n\n"
                "<b>📱 Account Statistics:</b>\n"
                f"├ <b>Total Accounts:</b> <code>{total_accounts}</code>\n"
                f"└ <b>▶️ Active Forwarding:</b> <code>{active}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            await event.edit(text, parse_mode='html', buttons=admin_panel_keyboard())
            return

        if data == "admin_stop_all_broadcasts":
            if not is_admin(uid):
                await event.answer("Admin only!", alert=True)
                return
            stopped = await stop_all_broadcasts()
            await event.edit(
                f"<b>🛑 Global Stop</b>\n\n<b>Accounts Stopped:</b> <code>{stopped}</code>",
                parse_mode='html',
                buttons=[[Button.inline("← Back", b"admin_panel")]]
            )
            return
        
        if data == "admin_admins":
            if not is_admin(uid):
                return
            
            # Show all admins with IDs and usernames
            try:
                owner_id = CONFIG.get('owner_id')
                all_admins = list(admins_col.find({}, {'user_id': 1}))
                admin_ids = [admin['user_id'] for admin in all_admins]
                
                text = "<b>ᴀᴅᴍɪɴꜱ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ</b>\n\n"
                text += "<blockquote><b>Commands:</b>\n"
                text += "<code>/addadmin {user_id}</code>\n"
                text += "<code>/rmadmin {user_id}</code></blockquote>\n\n"
                text += "━━━━━━━━━━━━━━━\n\n"
                text += "<b>ᴀᴅᴍɪɴꜱ ʟɪꜱᴛ</b>\n\n"
                
                # Show owner
                try:
                    owner = await main_bot.get_entity(int(owner_id))
                    owner_username = f"@{owner.username}" if getattr(owner, 'username', None) else "ɴᴏ ᴜꜱᴇʀɴᴀᴍᴇ"
                    owner_name = getattr(owner, 'first_name', 'Owner')
                    text += f"👑 <b>ᴏᴡɴᴇʀ:</b> <code>{owner_id}</code>\n"
                    text += f"   ɴᴀᴍᴇ: {owner_name}\n"
                    text += f"   ᴜꜱᴇʀɴᴀᴍᴇ: {owner_username}\n\n"
                except Exception:
                    text += f"👑 <b>ᴏᴡɴᴇʀ:</b> <code>{owner_id}</code>\n\n"
                
                # Show admins
                if admin_ids:
                    text += f"👥 <b>ᴀᴅᴍɪɴꜱ ({len(admin_ids)}):</b>\n\n"
                    for admin_id in admin_ids:
                        try:
                            admin = await main_bot.get_entity(int(admin_id))
                            admin_username = f"@{admin.username}" if getattr(admin, 'username', None) else "ɴᴏ ᴜꜱᴇʀɴᴀᴍᴇ"
                            admin_name = getattr(admin, 'first_name', 'Admin')
                            text += f"   • <code>{admin_id}</code>\n"
                            text += f"      ɴᴀᴍᴇ: {admin_name}\n"
                            text += f"      ᴜꜱᴇʀɴᴀᴍᴇ: {admin_username}\n\n"
                        except Exception:
                            text += f"   • <code>{admin_id}</code>\n\n"
                else:
                    text += "👥 <b>ᴀᴅᴍɪɴꜱ:</b> ɴᴏ ᴀᴅᴍɪɴꜱ ᴀᴅᴅᴇᴅ"
                
                await event.edit(text, parse_mode='html', buttons=[[Button.inline("← Back", b"admin_panel")]])
            except Exception as e:
                await event.edit(f"<b>Error:</b> {str(e)}", parse_mode='html', buttons=[[Button.inline("← Back", b"admin_panel")]])
            return
        
        if data == "admin_all_users":
            if not is_admin(uid):
                return

            page = 0
            per_page = 5
            users = list(users_col.find().sort('created_at', -1).skip(page*per_page).limit(per_page))
            total = users_col.count_documents({})
            total_pages = max(1, (total + per_page - 1) // per_page)

            text = f"<b>👥 All Users</b> <code>({total} total, page {page+1}/{total_pages})</code>\n\n"
            user_list = []
            buttons = []
            
            for u in users:
                user_id = u['user_id']
                username = u.get('username')
                
                # Try to fetch username from Telegram if not in database
                if not username:
                    username = await get_username_from_id(event.client, user_id)
                    if username:
                        # Update database with fetched username
                        uupdate({'user_id': user_id}, {'$set': {'username': username}})
                
                # Add to display list
                if username:
                    user_list.append(f"@{username}")
                    label = f"View @{username}"
                else:
                    user_list.append(f"<code>{user_id}</code>")
                    label = f"View {user_id}"
                
                buttons.append([Button.inline(label, f"admin_user_detail_all_{user_id}")])
            
            text += "\n".join(user_list) if users else "<i>No users found.</i>"
            nav = []
            if page > 0:
                nav.append(Button.inline("<", f"admin_all_users_page_{page-1}"))
            if (page+1)*per_page < total:
                nav.append(Button.inline(">", f"admin_all_users_page_{page+1}"))
            if nav:
                buttons.append(nav)

            buttons.append([Button.inline("← Back", b"admin_panel")])
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("admin_all_users_page_"):
            if not is_admin(uid):
                return

            page = int(data.replace("admin_all_users_page_", ""))
            per_page = 5
            users = list(users_col.find().sort('created_at', -1).skip(page*per_page).limit(per_page))
            total = users_col.count_documents({})
            total_pages = max(1, (total + per_page - 1) // per_page)

            text = f"<b>👥 All Users</b> <code>({total} total, page {page+1}/{total_pages})</code>\n\n"
            user_list = []
            buttons = []
            
            for u in users:
                user_id = u['user_id']
                username = u.get('username')
                
                # Try to fetch username from Telegram if not in database
                if not username:
                    username = await get_username_from_id(event.client, user_id)
                    if username:
                        # Update database with fetched username
                        uupdate({'user_id': user_id}, {'$set': {'username': username}})
                
                # Add to display list
                if username:
                    user_list.append(f"@{username}")
                    label = f"View @{username}"
                else:
                    user_list.append(f"<code>{user_id}</code>")
                    label = f"View {user_id}"
                
                buttons.append([Button.inline(label, f"admin_user_detail_all_{user_id}")])
            
            text += "\n".join(user_list) if users else "<i>No users found.</i>"
            nav = []
            if page > 0:
                nav.append(Button.inline("<", f"admin_all_users_page_{page-1}"))
            if (page+1)*per_page < total:
                nav.append(Button.inline(">", f"admin_all_users_page_{page+1}"))
            if nav:
                buttons.append(nav)

            buttons.append([Button.inline("← Back", b"admin_panel")])
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data.startswith("admin_user_detail_all_"):
            if not is_admin(uid):
                return

            target_id = int(data.replace("admin_user_detail_all_", ""))
            user_detail_source = 'all'

        elif data.startswith("admin_user_detail_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_user_detail_", ""))
            user_detail_source = 'premium'
        
        # Common user detail display logic for both handlers
        if data.startswith("admin_user_detail_all_") or data.startswith("admin_user_detail_"):
            user = users_col.find_one({'user_id': target_id})
            
            if not user:
                await event.answer("User not found!", alert=True)
                return
            
            tier = user.get('tier', 'free')
            max_acc = get_plan_max_accounts(user)
            approved = user.get('approved', False)
            accounts = list(accounts_col.find({'owner_id': target_id}))
            active = sum(1 for a in accounts if a.get('is_forwarding'))
            
            created_at = user.get('created_at')
            created_str = created_at.strftime('%Y-%m-%d %H:%M') if hasattr(created_at, 'strftime') else str(created_at)
            
            # Show plan name and expiry instead of tier
            # Check if target user is admin
            is_target_admin = is_admin(target_id)
            
            if is_target_admin:
                plan_display = "⚡ God Mode"
                expiry_display = "∞"
                max_acc = 999  # Admins have unlimited accounts
            elif tier == 'premium':
                plan_name = get_display_plan_name(user)
                expires_at = user.get('premium_expires_at')
                if expires_at and isinstance(expires_at, datetime):
                    remaining = expires_at - datetime.now()
                    if remaining.total_seconds() > 0:
                        expiry_display = f"{remaining.days}d"
                    else:
                        expiry_display = "Expired"
                else:
                    expiry_display = "∞"
                plan_display = plan_name
            else:
                plan_display = "No Plan"
                expiry_display = "∞"
            
            text = (
                "<b>👤 User Profile</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                "<b>📋 User Information:</b>\n"
                f"├ <b>User ID:</b> <code>{target_id}</code>\n"
                f"├ <b>Plan:</b> <code>{plan_display}</code>\n"
                f"├ <b>Expiry:</b> <code>{expiry_display}</code>\n"
                f"└ <b>Approved:</b> {'✅ Yes' if approved else '❌ No'}\n\n"
                "<b>📱 Account Statistics:</b>\n"
                f"├ <b>Max Accounts:</b> <code>{max_acc}</code>\n"
                f"├ <b>Total Accounts:</b> <code>{len(accounts)}</code>\n"
                f"└ <b>▶️ Active Now:</b> <code>{active}</code>\n\n"
                "<b>⏰ Account Activity:</b>\n"
                f"└ <b>Joined:</b> <code>{created_str}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            buttons = []
            
            # Don't show Grant/Revoke Premium buttons for admins
            if not is_target_admin:
                if tier != 'premium':
                    buttons.append([Button.inline("✅ Grant Premium", f"admin_grant_premium_{target_id}")])
                else:
                    buttons.append([Button.inline("❌ Revoke Premium", f"admin_revoke_premium_{target_id}")])
                
                # Add Reset button for non-admin users
                buttons.append([Button.inline("Reset User", f"admin_reset_user_{target_id}")])
                buttons.append([
                    Button.inline("Start Broadcast", f"admin_start_broadcast_{target_id}"),
                    Button.inline("Stop Broadcast", f"admin_stop_broadcast_{target_id}")
                ])
                buttons.append([Button.inline("Set Settings", f"admin_set_settings_{target_id}_{user_detail_source}")])
                buttons.append([
                    Button.inline("Automation Tools", f"admin_settings_automation_{target_id}_{user_detail_source}"),
                    Button.inline("Messaging & Logs", f"admin_menu_content_{target_id}_{user_detail_source}")
                ])
                buttons.append([Button.inline("Accounts", f"admin_user_accounts_{target_id}")])
                buttons.append([Button.inline("Add Account", f"admin_add_account_{target_id}")])
            
            # Back button routing based on source list
            if user_detail_source == 'all':
                back_callback = b"admin_all_users"
            else:
                back_callback = b"admin_premium"
            buttons.append([Button.inline("← Back", back_callback)])
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_set_settings_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_set_settings_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_settings_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_settings_mode_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_settings_mode_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_mode_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_settings_interval_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_settings_interval_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_interval_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_settings_ads_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_settings_ads_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_ads_mode_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_set_mode_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_set_mode_", "")
            parts = payload.split("_")
            target_id = int(parts[0])
            mode = parts[1] if len(parts) > 1 else "auto"
            source = parts[2] if len(parts) > 2 else "premium"
            uupdate({'user_id': target_id}, {'$set': {'forwarding_mode': mode}}, upsert=True)
            await event.answer("Mode updated", alert=True)
            text, buttons = admin_mode_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_set_interval_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_set_interval_", "")
            parts = payload.split("_")
            target_id = int(parts[0])
            preset = parts[1] if len(parts) > 1 else "medium"
            source = parts[2] if len(parts) > 2 else "premium"
            uupdate({'user_id': target_id}, {'$set': {'interval_preset': preset}}, upsert=True)
            await event.answer("Intervals updated", alert=True)
            text, buttons = admin_interval_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_interval_custom_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_interval_custom_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_states[uid] = {'action': 'admin_custom_interval', 'step': 'msg_delay', 'target_id': target_id, 'source': source}
            await event.edit(
                "<b>Admin: Custom Interval</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n\n"
                "Enter message delay in seconds (5-9999):",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_settings_interval_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_set_ads_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_set_ads_", "")
            parts = payload.split("_")
            target_id = int(parts[0])
            mode = parts[1] if len(parts) > 1 else "saved"
            source = parts[2] if len(parts) > 2 else "premium"
            uupdate({'user_id': target_id}, {'$set': {'ads_mode': mode}}, upsert=True)
            await event.answer("Ads mode updated", alert=True)
            text, buttons = admin_ads_mode_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_settings_automation_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_settings_automation_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_automation_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_menu_content_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_menu_content_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_content_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_locked_smart_rotation_") or data.startswith("admin_locked_auto_group_join_"):
            if data.startswith("admin_locked_smart_rotation_"):
                payload = data.replace("admin_locked_smart_rotation_", "")
            else:
                payload = data.replace("admin_locked_auto_group_join_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0]) if parts and parts[0].isdigit() else uid
            source = parts[1] if len(parts) > 1 else "premium"
            await event.edit(
                "<b>Paid Plan Feature</b>\n\nUpgrade the user to unlock this feature.",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_settings_automation_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_locked_autoreply_") or data.startswith("admin_locked_topics_"):
            if data.startswith("admin_locked_autoreply_"):
                payload = data.replace("admin_locked_autoreply_", "")
            else:
                payload = data.replace("admin_locked_topics_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0]) if parts and parts[0].isdigit() else uid
            source = parts[1] if len(parts) > 1 else "premium"
            await event.edit(
                "<b>Paid Plan Feature</b>\n\nUpgrade the user to unlock this feature.",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_menu_content_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_toggle_smart_rotation_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_toggle_smart_rotation_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_doc = get_user(target_id)
            new_val = not user_doc.get('smart_rotation', False)
            uupdate({'user_id': target_id}, {'$set': {'smart_rotation': new_val}}, upsert=True)
            await event.answer(f"Smart Rotation {'enabled' if new_val else 'disabled'}", alert=True)
            text, buttons = admin_automation_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_toggle_auto_leave_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_toggle_auto_leave_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_doc = get_user(target_id)
            new_val = not user_doc.get('auto_leave_groups', True)
            uupdate({'user_id': target_id}, {'$set': {'auto_leave_groups': new_val}}, upsert=True)
            await event.answer(f"Auto Leave {'enabled' if new_val else 'disabled'}", alert=True)
            text, buttons = admin_automation_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_auto_group_join_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_auto_group_join_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_states[uid] = {'state': 'admin_awaiting_group_join_file', 'target_id': target_id, 'source': source}
            await event.edit(
                "<b>Admin: Auto Group Join</b>\n\n"
                "Upload a .txt file with group links (one per line).\n\n"
                "Supported:\n"
                "• https://t.me/groupname\n"
                "• t.me/groupname\n"
                "• @groupname",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_settings_automation_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_refresh_all_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_refresh_all_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            accounts = get_user_accounts(target_id)
            if not accounts:
                await event.answer("No accounts for this user.", alert=True)
                return
            progress_msg = await event.respond("<b>🔄 Refreshing all groups...</b>", parse_mode='html')
            results = []
            for acc in accounts:
                account_id = str(acc['_id'])
                phone = acc.get('phone', 'Unknown')[-4:]
                try:
                    session_enc = acc.get('session')
                    if not session_enc:
                        results.append(f"<code>{phone}</code>: Session not found")
                        continue
                    session = cipher_suite.decrypt(session_enc.encode()).decode()
                    client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                    await client.connect()
                    if not await client.is_user_authorized():
                        results.append(f"<code>{phone}</code>: Session expired")
                        await client.disconnect()
                        continue
                    before_count = account_auto_groups_col.count_documents({'account_id': account_id})
                    count = await refresh_account_groups(client, account_id)
                    after_count = account_auto_groups_col.count_documents({'account_id': account_id})
                    if after_count > before_count:
                        results.append(f"<code>{phone}</code>: {before_count} → {after_count} (+{after_count - before_count})")
                    else:
                        results.append(f"<code>{phone}</code>: {before_count} (no new groups)")
                    await client.disconnect()
                except Exception as e:
                    results.append(f"<code>{phone}</code>: Error - {str(e)[:30]}")
            result_text = "<b>🔄 Refresh Complete</b>\n\n" + "\n".join(results) if results else "<b>❌ No groups refreshed</b>"
            await progress_msg.edit(result_text, parse_mode='html', buttons=[[Button.inline("Back", f"admin_settings_automation_{target_id}_{source}")]])
            return

        if data.startswith("admin_quiet_hours_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_quiet_hours_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_quiet_hours_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_quiet_preset_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_quiet_preset_", "")
            parts = payload.split("_")
            target_id = int(parts[0])
            start = parts[1]
            end = parts[2]
            source = parts[3] if len(parts) > 3 else "premium"
            start = f"{start[:2]}:{start[2:]}"
            end = f"{end[:2]}:{end[2:]}"
            label = f"{start}-{end}"
            uupdate(
                {'user_id': int(target_id)},
                {'$set': {'quiet_hours': {'enabled': True, 'start': start, 'end': end, 'label': label}}},
                upsert=True
            )
            await event.answer("Quiet hours updated", alert=True)
            text, buttons = admin_quiet_hours_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_quiet_custom_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_quiet_custom_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_states[uid] = {'action': 'admin_quiet_hours', 'step': 'start', 'target_id': target_id, 'source': source}
            await event.edit(
                "<b>Admin: Quiet Hours - Custom</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n\n"
                "Send the start time in 24h format (HH:MM).",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_quiet_hours_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_menu_autoreply_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_menu_autoreply_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_autoreply_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_autoreply_toggle_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_autoreply_toggle_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_doc = get_user(target_id)
            new_val = not user_doc.get('autoreply_enabled', False)
            uupdate({'user_id': int(target_id)}, {'$set': {'autoreply_enabled': new_val}}, upsert=True)
            await event.answer("Auto reply updated", alert=True)
            text, buttons = admin_autoreply_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_autoreply_set_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_autoreply_set_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            user_states[uid] = {'action': 'admin_custom_autoreply', 'target_id': target_id, 'source': source}
            await event.edit(
                "<b>Admin: Set Auto Reply</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n\n"
                "Send the auto-reply message.",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_menu_autoreply_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_autoreply_view_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_autoreply_view_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            accounts = get_user_accounts(target_id)
            reply = None
            if accounts:
                _ids = [str(acc['_id']) for acc in accounts]
                _d = account_settings_col.find_one(
                    {'account_id': {'$in': _ids}, 'auto_reply': {'$exists': True}}, {'auto_reply': 1})
                if _d:
                    reply = _d.get('auto_reply')
            preview = _h(reply) if reply else "<i>Not set</i>"
            await event.edit(
                f"<b>Admin: Auto Reply</b>\n\n{preview}",
                parse_mode='html',
                buttons=[[Button.inline("Back", f"admin_menu_autoreply_{target_id}_{source}")]]
            )
            return

        if data.startswith("admin_menu_logs_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_menu_logs_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            text, buttons = admin_logs_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_logs_enable_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_logs_enable_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            uupdate({'user_id': int(target_id)}, {'$set': {'logs_chat_id': int(target_id)}}, upsert=True)
            invalidate_logs_cache()
            await event.answer("Logs enabled", alert=True)
            text, buttons = admin_logs_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_logs_disable_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_logs_disable_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            uupdate({'user_id': int(target_id)}, {'$unset': {'logs_chat_id': ""}})
            invalidate_logs_cache()
            await event.answer("Logs disabled", alert=True)
            text, buttons = admin_logs_menu(target_id, source)
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_menu_topics_"):
            if not is_admin(uid):
                return
            payload = data.replace("admin_menu_topics_", "")
            parts = payload.split("_", 1)
            target_id = int(parts[0])
            source = parts[1] if len(parts) > 1 else "premium"
            accounts = get_user_accounts(target_id)
            if not accounts:
                await event.answer("Add an account first!", alert=True)
                return
            tier_settings = get_user_tier_settings(target_id)
            max_topics = tier_settings.get('max_topics', 3)
            text = (
                "<b>🏷️ Topics</b>\n\n"
                "<blockquote>Select a topic to add group links.</blockquote>\n\n"
                f"<b>Available topics:</b> <code>{max_topics}/{len(TOPICS)}</code>"
            )
            if not is_premium(target_id):
                text += "\n\n<i>Upgrade to a paid plan for all topics.</i>"
            buttons = []
            _tlist = list(TOPICS[:max_topics])
            _tcounts = await db_call(bulk_topic_counts, accounts, _tlist)
            for topic in _tlist:
                count = _tcounts.get(topic, 0)
                buttons.append([Button.inline(f"{topic.title()} ({count} groups)", f"admin_topic_select_{target_id}_{topic}_{source}")])
            buttons.append([Button.inline("Back", f"admin_menu_content_{target_id}_{source}")])
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_topic_select_"):
            payload = data.replace("admin_topic_select_", "")
            parts = payload.split("_", 2)
            target_id = int(parts[0])
            topic = parts[1]
            source = parts[2] if len(parts) > 2 else "premium"
            accounts = get_user_accounts(target_id)
            tier_settings = get_user_tier_settings(target_id)
            max_groups = tier_settings.get('max_groups_per_topic', 10)
            if len(accounts) == 1:
                acc = accounts[0]
                groups = list(account_topics_col.find({'account_id': acc['_id'], 'topic': topic}))
                text = (
                    f"<b>{_h(topic.title())}</b>\n\n"
                    f"<b>Groups:</b> <code>{len(groups)}/{max_groups}</code>\n\n"
                    "<b>Send topic link to add:</b>\n"
                    "<code>https://t.me/groupname/5</code>"
                )
                buttons = [[Button.inline("View Groups", f"admin_view_topic_groups_{target_id}_{topic}_{acc['_id']}_{source}")]] if groups else []
                buttons.append([Button.inline("Back", f"admin_menu_topics_{target_id}_{source}")])
                msg = await event.edit(text, parse_mode='html', buttons=buttons)
                user_states[uid] = {'action': 'admin_add_topic_link', 'topic': topic, 'account_id': acc['_id'], 'target_id': target_id, 'source': source, 'last_msg_id': msg.id if hasattr(msg, 'id') else event.message_id}
            else:
                text = f"<b>{_h(topic.title())}</b>\n\n<i>Select account to add groups:</i>"
                buttons = []
                _aids = [acc['_id'] for acc in accounts]
                _agg = await db_call(lambda: list(account_topics_col.aggregate([
                    {'$match': {'account_id': {'$in': _aids}, 'topic': topic}},
                    {'$group': {'_id': '$account_id', 'n': {'$sum': 1}}},
                ]))) if _aids else []
                _cba = {str(d['_id']): d['n'] for d in _agg}
                for acc in accounts:
                    phone = acc['phone'][-4:]
                    name = acc.get('name', 'Unknown')[:12]
                    count = _cba.get(str(acc['_id']), 0)
                    buttons.append([Button.inline(f"{phone} - {name} ({count})", f"admin_topic_acc_{target_id}_{topic}_{acc['_id']}_{source}")])
                buttons.append([Button.inline("Back", f"admin_menu_topics_{target_id}_{source}")])
                await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_topic_acc_"):
            payload = data.replace("admin_topic_acc_", "")
            parts = payload.split("_", 3)
            target_id = int(parts[0])
            topic = parts[1]
            acc_id = parts[2]
            source = parts[3] if len(parts) > 3 else "premium"
            tier_settings = get_user_tier_settings(target_id)
            max_groups = tier_settings.get('max_groups_per_topic', 10)
            groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}))
            text = (
                f"<b>🏷️ {topic.title()}</b>\n\n"
                f"<b>Groups:</b> <code>{len(groups)}/{max_groups}</code>\n\n"
                "<i>Send a topic link to add.</i>\n"
                "<code>Example: https://t.me/groupname/5</code>"
            )
            buttons = [[Button.inline("👁️ View Groups", f"admin_view_topic_groups_{target_id}_{topic}_{acc_id}_{source}")]] if groups else []
            buttons.append([Button.inline("← Back", f"admin_topic_select_{target_id}_{topic}_{source}")])
            msg = await event.edit(text, parse_mode='html', buttons=buttons)
            user_states[uid] = {'action': 'admin_add_topic_link', 'topic': topic, 'account_id': acc_id, 'target_id': target_id, 'source': source, 'last_msg_id': msg.id if hasattr(msg, 'id') else event.message_id}
            return

        if data.startswith("admin_view_topic_groups_"):
            payload = data.replace("admin_view_topic_groups_", "")
            parts = payload.split("_", 3)
            target_id = int(parts[0])
            topic = parts[1]
            acc_id = parts[2]
            source = parts[3] if len(parts) > 3 else "premium"
            groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}))
            total = len(groups)
            display_limit = 5
            text = f"<b>🏷️ {topic.title()} Groups</b> <code>({total} total)</code>\n\n"
            for i, g in enumerate(groups[:display_limit]):
                title = g.get('title', g.get('url', 'Unknown'))[:25]
                text += f"{i+1}. {title}\n"
            if total > display_limit:
                text += f"\n...and {total - display_limit} more groups"
            buttons = [
                [Button.inline("Clear All", f"admin_clear_topic_{target_id}_{topic}_{acc_id}_{source}")],
                [Button.inline("Back", f"admin_topic_select_{target_id}_{topic}_{source}")]
            ]
            await event.edit(text, parse_mode='html', buttons=buttons)
            return

        if data.startswith("admin_clear_topic_"):
            payload = data.replace("admin_clear_topic_", "")
            parts = payload.split("_", 3)
            target_id = int(parts[0])
            topic = parts[1]
            acc_id = parts[2]
            source = parts[3] if len(parts) > 3 else "premium"
            account_topics_col.delete_many({'account_id': acc_id, 'topic': topic})
            await event.answer(f"Cleared all {topic} groups!", alert=True)
            await event.edit(
                f"<b>🏷️ {topic.title()}</b>\n\n<b>Groups:</b> <code>0</code>\n\n<i>Send a group link to add.</i>",
                parse_mode='html',
                buttons=[[Button.inline("← Back", f"admin_menu_topics_{target_id}_{source}")]]
            )
            return
        
        if data.startswith("admin_grant_premium_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_grant_premium_", ""))
            
            # Show plan selection screen
            text = (
                f"<b>Grant Plan</b>\n\n"
                f"<b>User ID:</b> <code>{target_id}</code>\n\n"
                f"<i>Select a plan to grant (30 days):</i>\n\n"
                f"<b>📈 Kai:</b> 3 accounts, medium speed\n"
                f"<b>⭐ Super:</b> 5 accounts, fast speed\n"
                f"<b>👑 Ultra:</b> 5 accounts, fastest speed"
            )
            
            buttons = [
                [Button.inline("📈 Kai", f"admin_grant_grow_{target_id}")],
                [Button.inline("⭐ Super", f"admin_grant_prime_{target_id}")],
                [Button.inline("👑 Ultra", f"admin_grant_dominion_{target_id}")],
                [Button.inline("← Back", f"admin_user_detail_{target_id}")]
            ]
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        # Handle individual plan grants
        if data.startswith("admin_grant_scout_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_grant_scout_", ""))
            await event.answer("Plan unavailable.", alert=True)
            await event.edit(
                f"<b>Plan Unavailable</b>\n\n<i>User {target_id} cannot be granted the legacy plan.</i>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to Users", b"admin_all_users")]]
            )
            return
        
        if data.startswith("admin_grant_grow_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_grant_grow_", ""))
            days = 30
            
            try:
                # Use centralized function - handles DB, user notification, AND channel notification
                await grant_premium_to_user(target_id, 'grow', days, source='admin_user_profile')
                
                await event.answer("✅ Kai plan granted!", alert=True)
                await event.edit(
                    f"<b>✅ Plan Granted</b>\n\n<i>User {target_id} now has Kai plan access (30 days).</i>",
                    parse_mode='html',
                    buttons=[[Button.inline("← Back to Users", b"admin_all_users")]]
                )
            except Exception as e:
                await event.answer(f"❌ Error: {str(e)[:50]}", alert=True)
                print(f"[ADMIN] Failed to grant Kai to {target_id}: {e}")
            return
        
        if data.startswith("admin_grant_prime_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_grant_prime_", ""))
            days = 30
            
            try:
                # Use centralized function - handles DB, user notification, AND channel notification
                await grant_premium_to_user(target_id, 'prime', days, source='admin_user_profile')
                
                await event.answer("✅ Super plan granted!", alert=True)
                await event.edit(
                    f"<b>✅ Plan Granted</b>\n\n<i>User {target_id} now has Super plan access (30 days).</i>",
                    parse_mode='html',
                    buttons=[[Button.inline("← Back to Users", b"admin_all_users")]]
                )
            except Exception as e:
                await event.answer(f"❌ Error: {str(e)[:50]}", alert=True)
                print(f"[ADMIN] Failed to grant Super to {target_id}: {e}")
            return
        
        if data.startswith("admin_grant_dominion_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_grant_dominion_", ""))
            days = 30
            
            try:
                # Use centralized function - handles DB, user notification, AND channel notification
                await grant_premium_to_user(target_id, 'dominion', days, source='admin_user_profile')
                
                await event.answer("✅ Ultra plan granted!", alert=True)
                await event.edit(
                    f"<b>✅ Plan Granted</b>\n\n<i>User {target_id} now has Ultra plan access (30 days).</i>",
                    parse_mode='html',
                    buttons=[[Button.inline("← Back to Users", b"admin_all_users")]]
                )
            except Exception as e:
                await event.answer(f"❌ Error: {str(e)[:50]}", alert=True)
                print(f"[ADMIN] Failed to grant Ultra to {target_id}: {e}")
            return
        
        if data.startswith("admin_revoke_premium_"):
            if not is_admin(uid):
                return
            
            target_id = int(data.replace("admin_revoke_premium_", ""))
            remove_user_premium(target_id)
            await event.answer("❌ Premium revoked!", alert=True)
            
            await event.edit(
                f"<b>❌ Premium Revoked</b>\n\n<i>User {target_id} now has no active plan.</i>",
                parse_mode='html',
                buttons=[[Button.inline("← Back to Premium Users", b"admin_premium")]]
            )
            return
        
        if data == "admin_premium":
            if not is_admin(uid):
                return
            
            users = get_premium_users()
            text = f"<b>\U0001F451 Premium Users</b> <code>({len(users) if users else 0} total)</code>\n\n"
            
            buttons = []
            if not users:
                text += "<i>No premium users yet.</i>"
            else:
                for u in users[:20]:
                    user_id = u.get('user_id')
                    max_acc = get_plan_max_accounts(u)
                    acc_count = accounts_col.count_documents({'owner_id': user_id})
                    username = u.get('username')
                    label_id = f"@{username}" if username else str(user_id)
                    label = f"\U0001F451 {label_id} ({acc_count}/{max_acc} acc)"
                    buttons.append([Button.inline(label, f"admin_user_detail_{user_id}")])
            
            buttons.append([Button.inline("← Back", b"admin_panel")])
            
            await event.edit(text, parse_mode='html', buttons=buttons)
            return
        
        if data == "admin_stats":
            if not is_admin(uid):
                return
            
            total_sent = 0
            total_failed = 0
            total_auto_replies = 0
            for stat in account_stats_col.find({}):
                total_sent += stat.get('total_sent', 0)
                total_failed += stat.get('total_failed', 0)
                total_auto_replies += stat.get('auto_replies', 0)
            
            text = f"**Bot Statistics**\n\n"
            text += f"Total Messages Sent: {total_sent}\n"
            text += f"Total Failed: {total_failed}\n"
            text += f"Success Rate: {(total_sent / max(1, total_sent + total_failed) * 100):.1f}%\n"
            text += f"Total Auto Replies: {total_auto_replies}"
            
            await event.edit(text, buttons=[[Button.inline("Back", b"admin_panel")]])
            return
        
        if data == "admin_broadcast":
            if not is_admin(uid):
                return
            
            user_states[uid] = {'action': 'broadcast'}
            await event.respond("Send the message to broadcast to all users:")
            return
        
        if data.startswith("page_"):
            page = int(data.split("_")[1])
            accounts = get_user_accounts(uid)
            max_acc = get_user_max_accounts(uid)
            tier_settings = get_user_tier_settings(uid)
            tier = "Premium" if is_premium(uid) else "No Plan"
            
            text = f"**{tier} Hub** (Page {page+1})\n\nAccounts: {len(accounts)}/{max_acc}"
            await event.edit(text, buttons=account_list_keyboard(uid, page))
            return
        
        if data.startswith("acc_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            if not acc:
                await event.answer("Not found!", alert=True)
                return
            
            # Check if user has per-account config access (Super/Ultra)
            if not has_per_account_config_access(uid):
                await event.answer("Per-account config is a Super/Ultra feature!", alert=True)
                await event.edit(
                    "🔒 **Per-Account Configuration**\n\n"
                    "This feature allows you to customize settings for each account individually.\n\n"
                    "Available in:\n"
                    "• Super Plan (₹249)\n"
                    "• Ultra Plan (₹349)\n\n"
                    "Use main dashboard settings to control all accounts together.",
                    buttons=[[Button.inline("⬆️ Upgrade Plan", b"go_premium")], [Button.inline("🏠 Dashboard", b"enter_dashboard")]]
                )
                return
            
            stats = get_account_stats(account_id)
            settings = get_account_settings(account_id)
            topics = account_topics_col.count_documents({'account_id': account_id})
            groups = account_auto_groups_col.count_documents({'account_id': account_id})
            
            status = "🟢 Running" if acc.get('is_forwarding') else "🔴 Stopped"
            
            # Get user-level intervals
            user_doc = get_user(uid)
            preset = user_doc.get('interval_preset', 'medium')
            if preset == 'custom':
                custom = user_doc.get('custom_interval', {})
                msg_d = custom.get('msg_delay', 30)
                round_d = custom.get('round_delay', 600)
            else:
                interval_data = INTERVAL_PRESETS.get(preset, INTERVAL_PRESETS['medium'])
                msg_d = interval_data['msg_delay']
                round_d = interval_data['round_delay']
            
            text = (
                "<b>📱 Account Details</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                "<b>📋 Account Info:</b>\n"
                f"├ <b>Phone:</b> <code>{acc['phone']}</code>\n"
                f"├ <b>Name:</b> <code>{acc.get('name', 'Unknown')}</code>\n"
                f"└ <b>Status:</b> {status}\n\n"
                "<b>📊 Statistics:</b>\n"
                f"├ <b>Topics:</b> <code>{topics}</code>\n"
                f"├ <b>Groups:</b> <code>{groups}</code>\n"
                f"├ <b>✅ Messages Sent:</b> <code>{stats.get('total_sent', 0)}</code>\n"
                f"└ <b>❌ Failed:</b> <code>{stats.get('total_failed', 0)}</code>\n\n"
                "<b>⏱️ Interval Settings:</b>\n"
                f"├ <b>⏰ Message Delay:</b> <code>{msg_d}s</code>\n"
                f"└ <b>🔄 Cycle Delay:</b> <code>{round_d}s</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            await event.edit(text, parse_mode='html', buttons=account_menu_keyboard(account_id, acc, uid))
            return
        
        if data.startswith("topics_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            await event.edit(
                f"<b> Topics</b>\n<blockquote>Account: <code>{_h(acc['phone'])}</code></blockquote>",
                parse_mode='html',
                buttons=topics_menu_keyboard(account_id, uid)
            )
            return
        
        if data.startswith("topic_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            
            tier_settings = get_user_tier_settings(uid)
            max_groups = tier_settings.get('max_groups_per_topic', 10)
            
            links = list(account_topics_col.find({'account_id': {'$in': _account_id_variants(account_id)}, 'topic': topic}))
            text = f"**{topic.capitalize()}** ({len(links)}/{max_groups} links)\n\n"
            
            for i, l in enumerate(links[:15], 1):
                text += f"{i}. {l['url']}\n"
            if len(links) > 15:
                text += f"...+{len(links)-15} more"
            
            if not links:
                text += "No links yet."
            
            await event.edit(text, buttons=[
                [Button.inline("Add", f"add_{account_id}_{topic}"), Button.inline("Clear", f"clear_{account_id}_{topic}")],
                [Button.inline("Back", f"topics_{account_id}")]
            ])
            return
        
        if data.startswith("auto_"):
            account_id = data.split("_")[1]
            groups = list(account_auto_groups_col.find({'account_id': {'$in': _account_id_variants(account_id)}}))
            
            text = f"**Auto Groups** ({len(groups)})\n\n"
            for i, g in enumerate(groups[:15], 1):
                u = f"@{g['username']}" if g.get('username') else "Private"
                text += f"{i}. {g['title'][:20]} ({u})\n"
            if len(groups) > 15:
                text += f"...+{len(groups)-15} more"
            
            await event.edit(text, buttons=[[Button.inline("Back", f"topics_{account_id}")]])
            return
        
        if data.startswith("add_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            user_states[uid] = {'action': 'add_links', 'account_id': account_id, 'topic': topic}
            await event.respond(f"Send links for **{topic}** (one per line):")
            return
        
        if data.startswith("clear_"):
            parts = data.split("_")
            account_id, topic = parts[1], parts[2]
            result = account_topics_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}, 'topic': topic})
            await event.answer(f"Deleted {result.deleted_count} links!")
            return
        
        if data.startswith("settings_"):
            account_id = data.split("_")[1]
            settings = get_account_settings(account_id)
            
            # Get user-level intervals
            user_doc = get_user(uid)
            preset = user_doc.get('interval_preset', 'medium')
            if preset == 'custom':
                custom = user_doc.get('custom_interval', {})
                msg_d = custom.get('msg_delay', 30)
                round_d = custom.get('round_delay', 600)
            else:
                interval_data = INTERVAL_PRESETS.get(preset, INTERVAL_PRESETS['medium'])
                msg_d = interval_data['msg_delay']
                round_d = interval_data['round_delay']
            
            text = "**Settings**\n\n"
            text += f"ᴍᴇꜱꜱᴀɢᴇ ᴅᴇʟᴀʏ: {msg_d}ꜱ\n"
            text += f"ʀᴏᴜɴᴅ ᴅᴇʟᴀʏ: {round_d}ꜱ\n"
            
            tier_settings = get_user_tier_settings(uid)
            if tier_settings.get('auto_reply_enabled'):
                auto_reply_text = settings.get('auto_reply', 'ᴅᴇꜰᴀᴜʟᴛ')
                if auto_reply_text and auto_reply_text != 'ᴅᴇꜰᴀᴜʟᴛ':
                    text += f"ᴀᴜᴛᴏ-ʀᴇᴘʟʏ: {auto_reply_text[:40]}...\n"
                else:
                    text += f"ᴀᴜᴛᴏ-ʀᴇᴘʟʏ: ᴅᴇꜰᴀᴜʟᴛ...\n"
            
            failed = account_failed_groups_col.count_documents({'account_id': account_id})
            text += f"ꜰᴀɪʟᴇᴅ ɢʀᴏᴜᴘꜱ: {failed}"
            
            await event.edit(text, parse_mode='markdown', buttons=settings_keyboard(account_id, uid))
            return
        # setmsg_ and setround_ removed: intervals are now user-level only (Settings -> Intervals)
        
        if data.startswith("setreply_"):
            tier_settings = get_user_tier_settings(uid)
            if not tier_settings.get('auto_reply_enabled'):
                await event.answer("Paid plan feature!", alert=True)
                return
            account_id = data.split("_")[1]
            user_states[uid] = {'action': 'set_reply', 'account_id': account_id}
            await event.respond("Send new auto-reply message:")
            return
        
        if data.startswith("clearfailed_"):
            account_id = data.split("_")[1]
            clear_failed_groups(account_id)
            await event.answer("Cleared failed groups!")
            return
        
        if data.startswith("stats_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            stats = get_account_stats(account_id)
            failed = account_failed_groups_col.count_documents({'account_id': account_id})
            
            last = stats.get('last_forward')
            last_time = last.strftime('%Y-%m-%d %H:%M') if last else 'Never'
            
            total_sent = stats.get('total_sent', 0)
            total_failed = stats.get('total_failed', 0)
            total_attempts = total_sent + total_failed
            success_rate = (total_sent / total_attempts * 100) if total_attempts > 0 else 0
            
            text = (
                f"<b>📊 Account Statistics</b>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                f"<b>📱 Account:</b> <code>{acc['phone']}</code>\n\n"
                "<b>📈 Message Statistics:</b>\n"
                f"├ <b>✅ Messages Sent:</b> <code>{total_sent}</code>\n"
                f"├ <b>❌ Messages Failed:</b> <code>{total_failed}</code>\n"
                f"├ <b>⏭️ Skipped Groups:</b> <code>{failed}</code>\n"
                f"└ <b>📊 Success Rate:</b> <code>{success_rate:.1f}%</code>\n\n"
                "<b>⏰ Last Activity:</b>\n"
                f"└ <b>Last Forward:</b> <code>{last_time}</code>\n\n"
                "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
            )
            
            await event.edit(text, parse_mode='html', buttons=[
                [Button.inline("🔄 Reset Stats", f"reset_{account_id}")],
                [Button.inline("← Back", f"acc_{account_id}")]
            ])
            return
        
        if data.startswith("reset_"):
            account_id = data.split("_")[1]
            account_stats_col.update_one(
                {'account_id': account_id},
                {'$set': {'total_sent': 0, 'total_failed': 0}},
                upsert=True
            )
            await event.answer("Stats reset!")
            return
        
        if data.startswith("refresh_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            await event.answer("Refreshing groups...", alert=False)
            
            try:
                session = cipher_suite.decrypt(acc['session'].encode()).decode()
                client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                await client.connect()
                
                if await client.is_user_authorized():
                    count = await fetch_groups(client, account_id, acc['phone'])
                    await send_log(account_id, f"<b>Groups refreshed:</b> <code>{count}</code>")
                    await client.disconnect()
                    await event.answer(f"Found {count} groups!", alert=True)
                    await event.edit(f"<b>Refresh Groups</b>\n\n<b>Found:</b> <code>{count}</code>", parse_mode='html', buttons=[[Button.inline("Back", f"acc_{account_id}")]])
                else:
                    await event.answer("Session expired!", alert=True)
            except Exception as e:
                await event.answer("Error!", alert=True)
            return
        
        if data.startswith("fwd_select_"):
            account_id = data.split("_")[2]
            await event.edit("**Start Forwarding**\n\nSelect where to forward:", buttons=forwarding_select_keyboard(account_id, uid))
            return
        
        if data.startswith("startfwd_"):
            parts = data.split("_")
            account_id = parts[1]
            topic = parts[2] if len(parts) > 2 else "all"
            
            acc = get_account_by_id(account_id)
            if not acc:
                await event.answer("Account not found!", alert=True)
                return
            accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': True, 'fwd_topic': topic}})
            ensure_account_running(acc.get('owner_id', uid), acc['_id'])
            
            await event.answer("Started!")
            await event.edit(f"Forwarding started!\n\nTopic: {topic}", buttons=[[Button.inline("Back", f"acc_{account_id}")]])
            return
        
        if data.startswith("stop_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            accounts_col.update_one({'_id': acc['_id']}, {'$set': {'is_forwarding': False}})
            
            ensure_account_stopped(acc['_id'])
            
            if account_id in auto_reply_clients:
                try:
                    await auto_reply_clients[account_id].disconnect()
                except:
                    pass
                del auto_reply_clients[account_id]
            
            # Send log message to user (not per account)
            try:
                user_doc = get_user(uid)
                logs_chat_id = user_doc.get('logs_chat_id')
                if logs_chat_id and CONFIG.get('logger_bot_token'):
                    log_msg = (
                        f"<b>⏹️ Ads Stopped</b>\n\n"
                        f"<b>Account:</b> <code>{acc.get('phone', 'Unknown')}</code>\n\n"
                        f"<i>Advertising has been stopped by user.</i>"
                    )
                    await _tg_send_http(CONFIG['logger_bot_token'], int(logs_chat_id), log_msg)
                    print(f"[STOP] Stop log sent to user {uid} for account {account_id}")
            except Exception as e:
                print(f"[LOG ERROR] Failed to send stop log to user {uid}: {e}")
            
            await event.answer("Stopped!")
            await event.edit("Forwarding stopped!", buttons=[[Button.inline("Back", f"acc_{account_id}")]])
            return
        
        # (Removed legacy per-account log toggles; logs are user-level via menu_logs)
        
        if data.startswith("delete_"):
            account_id = data.split("_")[1]
            await event.edit(
                "**Delete this account?**\n\nAll data will be removed!",
                buttons=[
                    [Button.inline("Yes", f"confirm_{account_id}"), Button.inline("No", f"acc_{account_id}")]
                ]
            )
            return
        
        if data.startswith("confirm_"):
            account_id = data.split("_")[1]
            acc = get_account_by_id(account_id)
            
            if acc:
                from bson.objectid import ObjectId
                accounts_col.delete_one({'_id': ObjectId(account_id)})
                account_topics_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                account_settings_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                account_stats_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                account_auto_groups_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                account_failed_groups_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                logger_tokens_col.delete_many({'account_id': {'$in': _account_id_variants(account_id)}})
                
                ensure_account_stopped(acc['_id'])
                
                if account_id in auto_reply_clients:
                    try:
                        await auto_reply_clients[account_id].disconnect()
                    except:
                        pass
                    del auto_reply_clients[account_id]
            
            await event.answer("Deleted!")
            await event.edit("**Dashboard**", buttons=account_list_keyboard(uid))
            return
        
        if data == "host":
            if not is_approved(uid):
                approve_user(uid)
            
            accounts = get_user_accounts(uid)
            max_accounts = get_user_max_accounts(uid)
            
            if len(accounts) >= max_accounts:
                if is_premium(uid):
                    await event.answer(f"Limit reached ({max_accounts})", alert=True)
                else:
                    await event.answer("Upgrade to a paid plan for more accounts!", alert=True)
                return
            
            user_states[uid] = {'action': 'phone', 'owner_id': uid}
            await event.respond("Send phone with country code:\n\nExample: `+919876543210`")
            return
        
        if data.startswith("otp_"):
            if uid not in user_states or user_states[uid].get('action') != 'otp':
                return
            
            digit = data.split("_")[1]
            otp = user_states[uid].get('otp', '')
            
            if digit == "cancel":
                if 'client' in user_states[uid]:
                    await user_states[uid]['client'].disconnect()
                del user_states[uid]
                await event.answer("Cancelled!")
                await event.delete()
                return
            elif digit == "back":
                otp = otp[:-1]
            else:
                otp += digit
            
            user_states[uid]['otp'] = otp
            
            if len(otp) == 5:
                await event.edit(f"Code: `{otp}`\n\nVerifying...")
                
                client = None
                try:
                    client = user_states.get(uid, {}).get('client')
                    if not client:
                        raise RuntimeError('Session expired. Please request OTP again.')
                    
                    await client.sign_in(user_states[uid]['phone'], otp, phone_code_hash=user_states[uid]['hash'])
                    
                    me = await client.get_me()
                    session = client.session.save()
                    encrypted = cipher_suite.encrypt(session.encode()).decode()
                    owner_id = int(user_states[uid].get('owner_id', uid))
                    
                    result = accounts_col.insert_one({
                        'owner_id': owner_id,
                        'phone': user_states[uid]['phone'],
                        'name': me.first_name or 'Unknown',
                        'session': encrypted,
                        'is_forwarding': False,
                        'two_fa_password': user_states[uid].get('two_fa_password', ''),
                        'added_at': datetime.now()
                    })
                    
                    account_id = str(result.inserted_id)
                    count = await fetch_groups(client, account_id, user_states[uid]['phone'])
                    
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    
                    # Capture phone before clearing state
                    account_phone = user_states.get(uid, {}).get('phone', '')

                    if uid in user_states:
                        del user_states[uid]
                    
                    print(f"[ACCOUNT] Added account for user {owner_id}, fetched {count} groups")

                    # Send account added notification to channel
                    try:
                        user = get_user(owner_id)
                        sender = await event.get_sender()
                        total_accounts = accounts_col.count_documents({'owner_id': owner_id})
                        plan_name = get_display_plan_name(user)
                        max_accounts = get_user_max_accounts(owner_id)

                        print(f"[ACCOUNT] Triggering notification for user {owner_id}, phone {account_phone}")
                        task = asyncio.create_task(notify_account_added(
                            owner_id, sender.username, getattr(sender, 'phone', None),
                            account_phone, plan_name, total_accounts, max_accounts
                        ))
                        task.add_done_callback(lambda t: print(f"[ACCOUNT] Notification task completed: {t.exception() if t.exception() else 'Success'}"))
                    except Exception as e:
                        print(f"[NOTIFICATION] Error creating notification task (OTP callback flow): {e}")

                    if owner_id != uid:
                        await event.edit(
                            f"**Account Added for {owner_id}!**\n\n{me.first_name}\nFound {count} groups",
                            buttons=[[Button.inline("Back to User", f"admin_user_detail_{owner_id}")]]
                        )
                    else:
                        await event.edit(
                            f"**Account Added!**\n\n{me.first_name}\nFound {count} groups",
                            buttons=account_list_keyboard(uid)
                        )
                    
                except SessionPasswordNeededError:
                    user_states[uid]['action'] = '2fa'
                    await event.edit("**2FA Required**\n\nSend your cloud password:")
                except PhoneCodeInvalidError:
                    user_states[uid]['otp'] = ''
                    await event.edit("Wrong code! Try again:", buttons=otp_keyboard())
                except Exception as e:
                    await event.edit(f"Error: {str(e)[:100]}")
                    try:
                        if client:
                            await client.disconnect()
                    except Exception:
                        pass
                    if uid in user_states:
                        del user_states[uid]
            else:
                await event.edit(f"Code: `{otp}{'_' * (5-len(otp))}`", buttons=otp_keyboard())
            return
    
    except MessageNotModifiedError:
        pass
    except Exception as e:
        print(f"Callback error: {e}")
        await event.answer("Error!", alert=True)

@main_bot.on(events.NewMessage)
async def text_handler(event):
    uid = event.sender_id
    text = event.text.strip()
    
    if text.startswith('/'):
        return
    
    if uid not in user_states:
        return
    
    state = user_states[uid]
    action = state.get('action') if isinstance(state, dict) else None
    state_type = state.get('state') if isinstance(state, dict) else None
    
    # ===================== Payment Screenshot Handler =====================
    if state_type == 'awaiting_payment_screenshot':
        # User should send a photo (payment screenshot)
        request_id = state.get('request_id')
        if not request_id or request_id not in pending_upi_payments:
            await event.respond("⚠️ Payment request expired. Please start again.")
            del user_states[uid]
            return
        
        # Check if message has photo
        if not event.message.photo:
            await event.respond("📸 Please send a <b>photo</b> of your payment screenshot.", parse_mode='html')
            return
        
        pay_req = pending_upi_payments[request_id]
        pay_req['status'] = 'submitted'
        
        # Get admin list: OWNER + DB admins
        admin_ids = [BOT_CONFIG['owner_id']]
        db_admins = list(admins_col.find({}))
        for adm in db_admins:
            admin_ids.append(adm['user_id'])
        
        admin_ids = list(set(admin_ids))  # deduplicate
        
        # Build admin notification
        sender = await event.get_sender()
        username_display = f"@{pay_req['username']}" if pay_req.get('username') else 'No username'
        
        admin_text = (
            f"<b>💰 New Payment Screenshot</b>\n\n"
            f"<b>User ID:</b> <code>{pay_req['user_id']}</code>\n"
            f"<b>Username:</b> {username_display}\n"
            f"<b>Plan:</b> {pay_req['plan_name']}\n"
            f"<b>Amount:</b> ₹{pay_req['price']}\n\n"
            f"<b>UPI ID:</b> <code>{UPI_PAYMENT.get('upi_id', '')}</code>\n\n"
            f"Review the screenshot and approve/reject:"
        )
        
        admin_buttons = [
            [
                Button.inline("✅ Approve", f"payapprove_{request_id}".encode()),
                Button.inline("❌ Reject", f"payreject_{request_id}".encode())
            ]
        ]
        
        # Forward screenshot to all admins
        for admin_id in admin_ids:
            try:
                msg = await main_bot.send_message(
                    admin_id,
                    admin_text,
                    parse_mode='html',
                    file=event.message.photo,
                    buttons=admin_buttons
                )
                admin_payment_message_map[msg.id] = request_id
            except Exception as e:
                print(f"[PAYMENT] Failed to notify admin {admin_id}: {e}")
        
        # Confirm to user
        await event.respond(
            "<b>✅ Screenshot Submitted</b>\n\n"
            "Your payment is under review. You'll be notified once it's verified.\n\n"
            "<i>This usually takes a few minutes.</i>",
            parse_mode='html'
        )
        
        # Clear user state
        del user_states[uid]
        return

    # ===================== Admin Auto Group Join File Handler =====================
    if state_type == 'admin_awaiting_group_join_file':
        target_id = state.get('target_id')
        source = state.get('source', 'premium')
        if not target_id:
            del user_states[uid]
            await event.respond("Target user missing. Please retry.")
            return

        if not event.message.document:
            await event.respond("📄 Please send a .txt file with group links (one per line).", parse_mode='html')
            return

        if not is_premium(target_id):
            await event.respond("Paid plan feature only for this user.")
            del user_states[uid]
            return

        try:
            file_path = await event.message.download_media()
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_lines = f.read().splitlines()

            group_links = []
            for line in raw_lines:
                line = (line or '').strip()
                if not line or line.startswith('#'):
                    continue
                if 'https://t.me/' in line:
                    username = line.split('https://t.me/')[-1].strip('/')
                elif 't.me/' in line:
                    username = line.split('t.me/')[-1].strip('/')
                elif line.startswith('@'):
                    username = line[1:]
                else:
                    username = line
                username = (username or '').strip().strip('/')
                if username:
                    group_links.append(username)

            try:
                os.remove(file_path)
            except Exception:
                pass

            seen = set()
            deduped = []
            for u in group_links:
                key = u.lower()
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(u)
            group_links = deduped

            if not group_links:
                await event.respond("❌ No valid group links found in the file.")
                del user_states[uid]
                return

            user_accounts = list(accounts_col.find({'owner_id': int(target_id)}))
            if not user_accounts:
                await event.respond("❌ Target user has no accounts.")
                del user_states[uid]
                return

            BATCH_SIZE = 50
            BATCH_WAIT_SECONDS = 3600

            total_ops = len(user_accounts) * len(group_links)
            progress = {'done': 0, 'joined': 0, 'failed': 0, 'current_batch': 1}

            progress_msg = await event.respond(
                f"<b>Joining Groups...</b>\n\n"
                f"<b>User:</b> <code>{target_id}</code>\n"
                f"<b>Accounts:</b> {len(user_accounts)}\n"
                f"<b>Groups:</b> {len(group_links)}\n"
                f"<b>Mode:</b> <code>{BATCH_SIZE}/hour</code>\n\n"
                f"<b>Progress:</b> <code>0/{total_ops}</code>",
                parse_mode='html',
                buttons=[[Button.inline("Stop", b"auto_join_cancel"), Button.inline("Back", f"admin_settings_automation_{target_id}_{source}")]]
            )

            lock = asyncio.Lock()
            stop_evt = asyncio.Event()
            auto_join_cancel[uid] = False

            from telethon.tl.functions.channels import JoinChannelRequest

            async def update_progress_loop():
                last = None
                while not stop_evt.is_set():
                    await asyncio.sleep(1)
                    if auto_join_cancel.get(uid):
                        stop_evt.set()
                        break
                    async with lock:
                        snap = (progress['done'], progress['joined'], progress['failed'], progress['current_batch'])
                    if snap == last:
                        continue
                    last = snap
                    done, joined, failed, batch_no = snap
                    try:
                        await main_bot.edit_message(
                            progress_msg.chat_id,
                            progress_msg.id,
                            f"<b>Joining Groups...</b>\n\n"
                            f"<b>User:</b> <code>{target_id}</code>\n"
                            f"<b>Accounts:</b> {len(user_accounts)}\n"
                            f"<b>Groups:</b> {len(group_links)}\n"
                            f"<b>Mode:</b> <code>{BATCH_SIZE}/hour</code>\n"
                            f"<b>Batch:</b> <code>{batch_no}</code>\n\n"
                            f"<b>Progress:</b> <code>{done}/{total_ops}</code>\n"
                            f"<b>Joined:</b> <code>{joined}</code>\n"
                            f"<b>Failed:</b> <code>{failed}</code>",
                            parse_mode='html'
                        )
                    except Exception:
                        pass

            async def join_with_account(acc):
                account_id = acc.get('account_id') or acc.get('_id')
                if not account_id:
                    return
                try:
                    session_enc = acc.get('session')
                    if not session_enc:
                        return
                    session = cipher_suite.decrypt(session_enc.encode()).decode()
                except Exception:
                    return

                client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        return

                    for idx, username in enumerate(group_links):
                        if stop_evt.is_set() or auto_join_cancel.get(uid):
                            break
                        if idx > 0 and (idx % BATCH_SIZE) == 0:
                            async with lock:
                                progress['current_batch'] += 1
                            for _ in range(BATCH_WAIT_SECONDS):
                                if stop_evt.is_set() or auto_join_cancel.get(uid):
                                    break
                                await asyncio.sleep(1)

                        try:
                            entity = await client.get_entity(username)
                            await client(JoinChannelRequest(entity))
                            async with lock:
                                progress['joined'] += 1
                                progress['done'] += 1
                        except FloodWaitError as e:
                            wait_s = int(getattr(e, 'seconds', 0) or 0)
                            async with lock:
                                progress['done'] += 1
                            if wait_s > 0:
                                for _ in range(wait_s):
                                    if stop_evt.is_set() or auto_join_cancel.get(uid):
                                        break
                                    await asyncio.sleep(1)
                        except Exception:
                            async with lock:
                                progress['failed'] += 1
                                progress['done'] += 1
                        await asyncio.sleep(1)
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            progress_task = asyncio.create_task(update_progress_loop())
            account_tasks = [asyncio.create_task(join_with_account(acc)) for acc in user_accounts]
            await asyncio.gather(*account_tasks, return_exceptions=True)
            stop_evt.set()
            try:
                await progress_task
            except Exception:
                pass

            async with lock:
                done = progress['done']
                joined = progress['joined']
                failed = progress['failed']

            final_status = "✅ Complete" if not auto_join_cancel.get(uid) else "⏸ Stopped"
            await main_bot.edit_message(
                progress_msg.chat_id,
                progress_msg.id,
                f"<b>{final_status}</b>\n\n"
                f"<b>User:</b> <code>{target_id}</code>\n"
                f"<b>Accounts:</b> {len(user_accounts)}\n"
                f"<b>Groups:</b> {len(group_links)}\n\n"
                f"<b>Total Attempts:</b> <code>{done}/{total_ops}</code>\n"
                f"<b>Joined:</b> <code>{joined}</code>\n"
                f"<b>Failed:</b> <code>{failed}</code>",
                parse_mode='html'
            )
        except Exception as e:
            await event.respond(f"❌ Error processing file: {e}")
            print(f"[ADMIN AUTO_JOIN] Error: {e}")

        del user_states[uid]
        return
    
    # ===================== Auto Group Join File Handler =====================
    if state_type == 'awaiting_group_join_file':
        # User should send a .txt file with group links
        if not event.message.document:
            await event.respond("📄 Please send a .txt file with group links (one per line).", parse_mode='html')
            return

        # Check if premium
        if not is_premium(uid):
            await event.respond("⭐ Paid plan feature only!")
            del user_states[uid]
            return

        # Download file
        try:
            file_path = await event.message.download_media()

            with open(file_path, 'r', encoding='utf-8') as f:
                raw_lines = f.read().splitlines()

            # Parse group links
            group_links = []
            for line in raw_lines:
                line = (line or '').strip()
                if not line or line.startswith('#'):
                    continue
                if 'https://t.me/' in line:
                    username = line.split('https://t.me/')[-1].strip('/')
                elif 't.me/' in line:
                    username = line.split('t.me/')[-1].strip('/')
                elif line.startswith('@'):
                    username = line[1:]
                else:
                    username = line

                username = (username or '').strip().strip('/')
                if username:
                    group_links.append(username)

            try:
                os.remove(file_path)
            except Exception:
                pass

            # Deduplicate while preserving order
            seen = set()
            deduped = []
            for u in group_links:
                if u.lower() in seen:
                    continue
                seen.add(u.lower())
                deduped.append(u)
            group_links = deduped

            if not group_links:
                await event.respond("❌ No valid group links found in the file.")
                del user_states[uid]
                return

            user_accounts = list(accounts_col.find({'owner_id': uid}))
            if not user_accounts:
                await event.respond("❌ Add an account first.")
                del user_states[uid]
                return

            # Requirements: join 50 groups per hour (batch) per account
            BATCH_SIZE = 50
            BATCH_WAIT_SECONDS = 3600

            total_ops = len(user_accounts) * len(group_links)
            progress = {
                'done': 0,
                'joined': 0,
                'failed': 0,
                'current_batch': 1,
            }

            progress_msg = await event.respond(
                f"<b>Joining Groups...</b>\n\n"
                f"<b>Accounts:</b> {len(user_accounts)}\n"
                f"<b>Groups:</b> {len(group_links)}\n"
                f"<b>Mode:</b> <code>{BATCH_SIZE}/hour</code>\n\n"
                f"<b>Progress:</b> <code>0/{total_ops}</code>",
                parse_mode='html',
                buttons=[[Button.inline("Stop", b"auto_join_cancel"), Button.inline("Back", b"menu_auto_group_join")]]
            )

            lock = asyncio.Lock()
            stop_evt = asyncio.Event()
            auto_join_cancel[uid] = False

            from telethon.tl.functions.channels import JoinChannelRequest

            async def update_progress_loop():
                last = None
                while not stop_evt.is_set():
                    await asyncio.sleep(1)
                    if auto_join_cancel.get(uid):
                        stop_evt.set()
                        break

                    async with lock:
                        snap = (progress['done'], progress['joined'], progress['failed'], progress['current_batch'])
                    if snap == last:
                        continue
                    last = snap

                    done, joined, failed, batch_no = snap
                    try:
                        await main_bot.edit_message(
                            progress_msg.chat_id,
                            progress_msg.id,
                            f"<b>Joining Groups...</b>\n\n"
                            f"<b>Accounts:</b> {len(user_accounts)}\n"
                            f"<b>Groups:</b> {len(group_links)}\n"
                            f"<b>Mode:</b> <code>{BATCH_SIZE}/hour</code>\n"
                            f"<b>Batch:</b> <code>{batch_no}</code>\n\n"
                            f"<b>Progress:</b> <code>{done}/{total_ops}</code>\n"
                            f"<b>Joined:</b> <code>{joined}</code>\n"
                            f"<b>Failed:</b> <code>{failed}</code>",
                            parse_mode='html'
                        )
                    except Exception:
                        pass

            async def join_with_account(acc):
                account_id = acc.get('account_id') or acc.get('_id')
                if not account_id:
                    return

                # decrypt session
                try:
                    session_enc = acc.get('session')
                    if not session_enc:
                        return
                    session = cipher_suite.decrypt(session_enc.encode()).decode()
                except Exception:
                    return

                client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        return

                    for idx, username in enumerate(group_links):
                        if stop_evt.is_set() or auto_join_cancel.get(uid):
                            break

                        # Batch throttle: after each 50 joins attempt, wait 1 hour
                        if idx > 0 and (idx % BATCH_SIZE) == 0:
                            async with lock:
                                progress['current_batch'] += 1
                            # Wait, but still allow cancel
                            for _ in range(BATCH_WAIT_SECONDS):
                                if stop_evt.is_set() or auto_join_cancel.get(uid):
                                    break
                                await asyncio.sleep(1)

                        try:
                            entity = await client.get_entity(username)
                            await client(JoinChannelRequest(entity))
                            async with lock:
                                progress['joined'] += 1
                                progress['done'] += 1
                        except FloodWaitError as e:
                            # Respect floodwait for joining; don't count as failure but still counts as an attempt
                            wait_s = int(getattr(e, 'seconds', 0) or 0)
                            async with lock:
                                progress['done'] += 1
                            if wait_s > 0:
                                for _ in range(wait_s):
                                    if stop_evt.is_set() or auto_join_cancel.get(uid):
                                        break
                                    await asyncio.sleep(1)
                        except Exception:
                            async with lock:
                                progress['failed'] += 1
                                progress['done'] += 1

                        await asyncio.sleep(1)

                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            progress_task = asyncio.create_task(update_progress_loop())
            account_tasks = [asyncio.create_task(join_with_account(acc)) for acc in user_accounts]
            await asyncio.gather(*account_tasks, return_exceptions=True)
            stop_evt.set()
            try:
                await progress_task
            except Exception:
                pass

            async with lock:
                done = progress['done']
                joined = progress['joined']
                failed = progress['failed']

            final_status = "✅ Complete" if not auto_join_cancel.get(uid) else "⏸ Stopped"
            await main_bot.edit_message(
                progress_msg.chat_id,
                progress_msg.id,
                f"<b>{final_status}</b>\n\n"
                f"<b>Accounts:</b> {len(user_accounts)}\n"
                f"<b>Groups:</b> {len(group_links)}\n\n"
                f"<b>Total Attempts:</b> <code>{done}/{total_ops}</code>\n"
                f"<b>Joined:</b> <code>{joined}</code>\n"
                f"<b>Failed:</b> <code>{failed}</code>",
                parse_mode='html'
            )

        except Exception as e:
            await event.respond(f"❌ Error processing file: {e}")
            print(f"[AUTO_JOIN] Error: {e}")

        del user_states[uid]
        return
    
    if action == 'broadcast':
        if not is_admin(uid):
            del user_states[uid]
            return
        
        users = get_all_users()
        sent = 0
        failed = 0
        for u in users:
            try:
                await main_bot.send_message(u['user_id'], f"**Announcement**\n\n{text}")
                sent += 1
            except:
                failed += 1
        
        del user_states[uid]
        await event.respond(f"Broadcast complete!\nSent: {sent}\nFailed: {failed}")
        return
    
    if action == 'custom_autoreply':
        if not is_premium(uid):
            del user_states[uid]
            await event.respond("Paid plan only!")
            return
        
        # Save custom auto-reply to ALL user's accounts in account_settings_col
        accounts = get_user_accounts(uid)
        if accounts:
            for acc in accounts:
                update_account_settings(str(acc['_id']), {'auto_reply': text})
        
        del user_states[uid]
        await respond_with_welcome(
            event,
            f"✅ <b>Custom auto-reply saved!</b>\n\n<i>Applied to all {len(accounts)} account(s)</i>",
            buttons=[[Button.inline("← Back to Auto Reply", b"menu_autoreply")]]
        )
        return

    if action == 'admin_custom_autoreply':
        target_id = state.get('target_id')
        source = state.get('source', 'premium')
        if not target_id:
            del user_states[uid]
            await event.respond("Target user missing.")
            return
        if not is_premium(target_id):
            del user_states[uid]
            await event.respond("Paid plan feature only for this user.")
            return

        accounts = get_user_accounts(target_id)
        if accounts:
            for acc in accounts:
                update_account_settings(str(acc['_id']), {'auto_reply': text})

        del user_states[uid]
        await respond_with_welcome(
            event,
            f"✅ <b>Auto-reply saved</b>\n\nApplied to <code>{len(accounts)}</code> account(s).",
            buttons=[[Button.inline("Back", f"admin_menu_autoreply_{target_id}_{source}")]]
        )
        return
    
    if action == 'add_topic_link':
        topic = state.get('topic')
        acc_id = state.get('account_id')
        last_msg_id = state.get('last_msg_id')
        
        raw_links = text.strip().replace(',', '\n').split('\n')
        links = []
        for raw in raw_links:
            link = raw.strip()
            if not link:
                continue
            if '?' in link:
                link = link.split('?')[0]
            if link.startswith('@'):
                link = f"https://t.me/{link[1:]}"
            elif link.startswith('t.me/'):
                link = f"https://{link}"
            elif not link.startswith('https://t.me/'):
                continue
            if 't.me/' in link:
                links.append(link)
        
        if not links:
            await event.respond("Invalid! Send links like:\n`https://t.me/groupname/5`\n\nYou can send multiple links, one per line.")
            return
        
        tier_settings = get_user_tier_settings(uid)
        max_groups = tier_settings.get('max_groups_per_topic', 10)
        current_count = account_topics_col.count_documents({'account_id': acc_id, 'topic': topic})
        
        added = 0
        skipped = 0
        
        for link in links:
            if current_count + added >= max_groups:
                break
            
            existing = account_topics_col.find_one({'account_id': acc_id, 'topic': topic, 'link': link})
            if existing:
                skipped += 1
                continue
            
            parts = link.replace('https://t.me/', '').split('/')
            group_username = parts[0]
            topic_msg_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            display_title = f"{group_username}/{topic_msg_id}" if topic_msg_id else group_username
            
            account_topics_col.insert_one({
                'account_id': str(acc_id),  # ensure string for consistency
                'topic': topic,
                'link': link,
                'title': display_title,
                'topic_msg_id': topic_msg_id,
                'added_at': datetime.now()
            })
            added += 1
        
        new_count = current_count + added
        
        update_text = f"**{topic.title()}**\n\nGroups: {new_count}/{max_groups}\n"
        if added > 0:
            update_text += f"Added: {added}"
        if skipped > 0:
            update_text += f" | Skipped: {skipped} (duplicates)"
        update_text += "\n\n"
        
        groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}).sort('added_at', -1).limit(5))
        for i, g in enumerate(groups):
            update_text += f"{i+1}. {g.get('title', 'Unknown')}\n"
        
        total = account_topics_col.count_documents({'account_id': acc_id, 'topic': topic})
        if total > 5:
            update_text += f"\n...and {total - 5} more"
        
        update_text += "\n\nSend more links or go back."
        
        if last_msg_id:
            try:
                await main_bot.edit_message(event.chat_id, last_msg_id, update_text, 
                    buttons=[[Button.inline("View All", f"view_topic_groups_{topic}_{acc_id}")], [Button.inline("Back to Topics", b"menu_topics")]])
                await event.delete()
            except:
                msg = await event.respond(update_text,
                    buttons=[[Button.inline("View All", f"view_topic_groups_{topic}_{acc_id}")], [Button.inline("Back to Topics", b"menu_topics")]])
                user_states[uid]['last_msg_id'] = msg.id
        else:
            msg = await event.respond(update_text,
                buttons=[[Button.inline("View All", f"view_topic_groups_{topic}_{acc_id}")], [Button.inline("Back to Topics", b"menu_topics")]])
            user_states[uid]['last_msg_id'] = msg.id
        return

    if action == 'admin_add_topic_link':
        topic = state.get('topic')
        acc_id = state.get('account_id')
        target_id = state.get('target_id')
        source = state.get('source', 'premium')
        last_msg_id = state.get('last_msg_id')

        raw_links = text.strip().replace(',', '\n').split('\n')
        links = []
        for raw in raw_links:
            link = raw.strip()
            if not link:
                continue
            if '?' in link:
                link = link.split('?')[0]
            if link.startswith('@'):
                link = f"https://t.me/{link[1:]}"
            elif link.startswith('t.me/'):
                link = f"https://{link}"
            elif not link.startswith('https://t.me/'):
                continue
            if 't.me/' in link:
                links.append(link)

        if not links:
            await event.respond("Invalid! Send links like:\n`https://t.me/groupname/5`\n\nYou can send multiple links, one per line.")
            return

        tier_settings = get_user_tier_settings(int(target_id))
        max_groups = tier_settings.get('max_groups_per_topic', 10)
        current_count = account_topics_col.count_documents({'account_id': acc_id, 'topic': topic})

        added = 0
        skipped = 0
        for link in links:
            if current_count + added >= max_groups:
                break
            existing = account_topics_col.find_one({'account_id': acc_id, 'topic': topic, 'link': link})
            if existing:
                skipped += 1
                continue

            parts = link.replace('https://t.me/', '').split('/')
            group_username = parts[0]
            topic_msg_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            display_title = f"{group_username}/{topic_msg_id}" if topic_msg_id else group_username

            account_topics_col.insert_one({
                'account_id': str(acc_id),
                'topic': topic,
                'link': link,
                'title': display_title,
                'topic_msg_id': topic_msg_id,
                'added_at': datetime.now()
            })
            added += 1

        new_count = current_count + added
        update_text = f"**{topic.title()}**\n\nGroups: {new_count}/{max_groups}\n"
        if added > 0:
            update_text += f"Added: {added}"
        if skipped > 0:
            update_text += f" | Skipped: {skipped} (duplicates)"
        update_text += "\n\n"

        groups = list(account_topics_col.find({'account_id': acc_id, 'topic': topic}).sort('added_at', -1).limit(5))
        for i, g in enumerate(groups):
            update_text += f"{i+1}. {g.get('title', 'Unknown')}\n"

        total = account_topics_col.count_documents({'account_id': acc_id, 'topic': topic})
        if total > 5:
            update_text += f"\n...and {total - 5} more"

        update_text += "\n\nSend more links or go back."

        buttons = [
            [Button.inline("View All", f"admin_view_topic_groups_{target_id}_{topic}_{acc_id}_{source}")],
            [Button.inline("Back to Topics", f"admin_menu_topics_{target_id}_{source}")]
        ]
        if last_msg_id:
            try:
                await main_bot.edit_message(event.chat_id, last_msg_id, update_text, buttons=buttons)
                await event.delete()
            except:
                msg = await event.respond(update_text, buttons=buttons)
                user_states[uid]['last_msg_id'] = msg.id
        else:
            msg = await event.respond(update_text, buttons=buttons)
            user_states[uid]['last_msg_id'] = msg.id
        return
    
    if action == 'quiet_hours':
        step = state.get('step')
        if step == 'start':
            start = _parse_time_24h(text)
            if not start:
                await event.respond("Please use 24h format: HH:MM (example: 01:00).")
                return
            user_states[uid]['start'] = start
            user_states[uid]['step'] = 'end'
            await event.respond("Send the end time in 24h format (HH:MM). Example: 07:00")
            return

        if step == 'end':
            end = _parse_time_24h(text)
            if not end:
                await event.respond("Please use 24h format: HH:MM (example: 07:00).")
                return
            start = state.get('start')
            if not start:
                del user_states[uid]
                await event.respond("Start time missing. Please open Quiet Hours again.")
                return

            label = f"{start}-{end}"
            uupdate(
                {'user_id': int(uid)},
                {'$set': {'quiet_hours': {'enabled': True, 'start': start, 'end': end, 'label': label}}},
                upsert=True
            )
            del user_states[uid]
            text, buttons = quiet_hours_menu(uid)
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.respond(file=welcome_image, message=text, parse_mode='html', buttons=buttons)
            else:
                await event.respond(text, parse_mode='html', buttons=buttons)
        return
    
    if action == 'admin_custom_interval':
        step = state.get('step')
        target_id = int(state.get('target_id'))
        source = state.get('source', 'premium')
        try:
            val = int(text)
        except:
            await event.respond("Please enter a valid number!")
            return

        if step == 'msg_delay':
            if val < 5 or val > 9999:
                await event.respond("Enter a value between 5-9999:")
                return
            user_states[uid]['msg_delay'] = val
            user_states[uid]['step'] = 'round_delay'
            await event.respond("Enter cycle delay in seconds (60-9999):")
            return

        if step == 'round_delay':
            if val < 60 or val > 9999:
                await event.respond("Enter a value between 60-9999:")
                return

            custom_interval = {
                'msg_delay': user_states[uid]['msg_delay'],
                'round_delay': val
            }
            uupdate(
                {'user_id': target_id},
                {'$set': {'custom_interval': custom_interval, 'interval_preset': 'custom'}},
                upsert=True
            )
            del user_states[uid]
            await respond_with_welcome(
                event,
                f"<b>Custom Interval Set</b>\n\n"
                f"User: <code>{target_id}</code>\n"
                f"Message Delay: <code>{custom_interval['msg_delay']}s</code>\n"
                f"Cycle Delay: <code>{custom_interval['round_delay']}s</code>",
                buttons=[[Button.inline("Back", f"admin_settings_interval_{target_id}_{source}")]]
            )
            return

    if action == 'admin_quiet_hours':
        step = state.get('step')
        target_id = state.get('target_id')
        source = state.get('source', 'premium')
        if not target_id:
            del user_states[uid]
            await event.respond("Target user missing.")
            return

        if step == 'start':
            start = _parse_time_24h(text)
            if not start:
                await event.respond("Please use 24h format: HH:MM (example: 01:00).")
                return
            user_states[uid]['start'] = start
            user_states[uid]['step'] = 'end'
            await event.respond("Send the end time in 24h format (HH:MM). Example: 07:00")
            return

        if step == 'end':
            end = _parse_time_24h(text)
            if not end:
                await event.respond("Please use 24h format: HH:MM (example: 07:00).")
                return
            start = state.get('start')
            if not start:
                del user_states[uid]
                await event.respond("Start time missing. Please open Quiet Hours again.")
                return
            label = f"{start}-{end}"
            uupdate(
                {'user_id': int(target_id)},
                {'$set': {'quiet_hours': {'enabled': True, 'start': start, 'end': end, 'label': label}}},
                upsert=True
            )
            del user_states[uid]
            text, buttons = admin_quiet_hours_menu(int(target_id), source)
            await respond_with_welcome(event, text, buttons=buttons)
            return

    if action == 'set_target_freq':
        if not is_admin(uid):
            user_states.pop(uid, None)
            await event.respond("Frequency is managed by admin.")
            return
        try:
            val = int(text)
        except Exception:
            await event.respond(f"Please enter a number (1-{HARD_MAX_TARGET_PER_HOUR}).")
            return
        if val < 1 or val > HARD_MAX_TARGET_PER_HOUR:
            await event.respond(f"Enter a number between 1 and {HARD_MAX_TARGET_PER_HOUR}.")
            return
        set_global_setting('target_per_hour', val)
        user_states.pop(uid, None)
        msg = (f"🎯 Global frequency set: ~{val}/hour per group (applies to ALL users).\n\n"
               f"The bot auto-adjusts each account's cycle delay from its group count. "
               f"Takes effect within ~30s + the current round.")
        await event.respond(msg, buttons=[[Button.inline("Back to Dashboard", b"enter_dashboard")]])
        return

    if action == 'custom_interval':
        step = state.get('step')
        try:
            val = int(text)
        except:
            await event.respond("Please enter a valid number!")
            return
        
        if step == 'msg_delay':
            if val < 5 or val > 9999:
                await event.respond("Enter a value between 5-9999:")
                return
            user_states[uid]['msg_delay'] = val
            user_states[uid]['step'] = 'round_delay'
            await event.respond("Enter cycle delay in seconds (60-9999):")
            return
        
        if step == 'round_delay':
            if val < 60 or val > 9999:
                await event.respond("Enter a value between 60-9999:")
                return
            
            custom_interval = {
                'msg_delay': user_states[uid]['msg_delay'],
                'round_delay': val
            }
            uupdate({'user_id': uid}, {'$set': {'custom_interval': custom_interval, 'interval_preset': 'custom'}})
            del user_states[uid]
            saved_text = (
                "<b>Custom Interval Saved!</b>\n\n"
                f"Message Delay: <code>{custom_interval['msg_delay']}s</code>\n"
                f"Cycle Delay: <code>{custom_interval['round_delay']}s</code>"
            )
            buttons = [[Button.inline("Back to Dashboard", b"enter_dashboard")]]
            welcome_image = MESSAGES.get('welcome_image', '')
            if welcome_image:
                await event.respond(file=welcome_image, message=saved_text, parse_mode='html', buttons=buttons)
            else:
                await event.respond(saved_text, parse_mode='html', buttons=buttons)
            return
    
    if not is_approved(uid):
        approve_user(uid)
    
    if action == 'phone':
        if not re.match(r'^\+\d{10,15}$', text):
            await event.respond("Invalid format!\n\nUse: `+919876543210`")
            return
        owner_id = int(state.get('owner_id', uid))
        if owner_id != uid and not is_admin(uid):
            owner_id = uid
        get_user(owner_id)

        accounts = get_user_accounts(owner_id)
        max_accounts = get_user_max_accounts(owner_id)

        if len(accounts) >= max_accounts:
            del user_states[uid]
            if owner_id != uid:
                await event.respond(f"User {owner_id} reached the account limit ({max_accounts}).")
            else:
                await event.respond(f"Account limit reached ({max_accounts})!")
            return
        
        # Typewriter effect: progressive updates
        status_msg = await event.respond("Connecting...")
        await asyncio.sleep(0.6)
        await status_msg.edit("Connecting to server...")
        await asyncio.sleep(0.7)
        await status_msg.edit("Sending OTP...")
        
        client = None
        try:
            sent = None
            proxy = None
            last_err = None
            for candidate in get_proxy_candidates():
                proxy = candidate
                proxy_info = f" via proxy" if proxy else ""
                print(f"[OTP] Sending code to {text}{proxy_info}")
                client = TelegramClient(StringSession(), CONFIG['api_id'], CONFIG['api_hash'], proxy=proxy)
                try:
                    await client.connect()
                    sent = await client.send_code_request(text)
                    break
                except Exception as e:
                    last_err = e
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    client = None
                    continue

            if not sent:
                raise last_err or RuntimeError("All proxies failed")
            
            await asyncio.sleep(0.5)
            await status_msg.edit("OTP Sent!")
            
            user_states[uid] = {
                'action': 'otp',
                'client': client,
                'phone': text,
                'hash': sent.phone_code_hash,
                'proxy': proxy,
                'owner_id': owner_id
            }
            
            await asyncio.sleep(0.4)
            await event.respond(
                "**OTP Sent**\n\n"
                "Enter the code you received.\n\n"
                "Format: `code1234` (if code is 1234)\n\n"
                "Example: `code12345`"
            )
            
        except PhoneNumberInvalidError:
            await status_msg.edit("Invalid phone number!")
            del user_states[uid]
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        except Exception as e:
            await status_msg.edit(f"Failed to send OTP: {str(e)[:100]}")
            del user_states[uid]
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
    
    elif action == 'otp':
        # Accept code in format: code1234 (remove "code" prefix)
        otp_code = text
        if text.lower().startswith('code'):
            otp_code = text[4:].strip()
        
        if not otp_code.isdigit() or len(otp_code) < 4:
            await event.respond("Invalid code format!\n\nUse: `code12345` (if OTP is 12345)")
            return
        
        try:
            client = state['client']
            await client.sign_in(state['phone'], otp_code, phone_code_hash=state['hash'])
            
            # Check if 2FA enabled
            me = await client.get_me()
            
            # Login successful - save account
            session = client.session.save()
            encrypted = cipher_suite.encrypt(session.encode()).decode()
            
            owner_id = int(state.get('owner_id', uid))
            result = accounts_col.insert_one({
                'owner_id': owner_id,
                'phone': state['phone'],
                'name': me.first_name or 'Unknown',
                'session': encrypted,
                'is_forwarding': False,
                'two_fa_password': '',
                'added_at': datetime.now()
            })
            
            account_id = str(result.inserted_id)
            
            # Send account added notification
            try:
                user = get_user(owner_id)
                sender = await event.get_sender()
                total_accounts = accounts_col.count_documents({'owner_id': owner_id})
                plan_name = get_display_plan_name(user)
                max_accounts = get_user_max_accounts(owner_id)
                
                print(f"[ACCOUNT] Triggering notification for user {owner_id}, phone {state['phone']}")
                # Use asyncio.create_task to avoid blocking, but ensure it runs
                task = asyncio.create_task(notify_account_added(
                    owner_id, sender.username, getattr(sender, 'phone', None),
                    state['phone'], plan_name, total_accounts, max_accounts
                ))
                # Don't await - let it run in background, but store reference to prevent garbage collection
                task.add_done_callback(lambda t: print(f"[ACCOUNT] Notification task completed: {t.exception() if t.exception() else 'Success'}"))
            except Exception as e:
                print(f"[NOTIFICATION] Error creating notification task: {e}")
                import traceback
                traceback.print_exc()
            
            count = await fetch_groups(client, account_id, state['phone'])
            await client.disconnect()
            
            del user_states[uid]
            
            if owner_id != uid:
                await event.respond(
                    f"**Account Added for {owner_id}**\n\n"
                    f"Name: {me.first_name}\n"
                    f"Phone: {state['phone']}\n"
                    f"Groups found: {count}",
                    buttons=[[Button.inline("Back to User", f"admin_user_detail_{owner_id}")]]
                )
            else:
                # NEW: Show professional plan selection after login (with image)
                plan_msg = (
                    f"**Account Added**\n\n"
                    f"Name: {me.first_name}\n"
                    f"Phone: {state['phone']}\n"
                    f"Groups found: {count}\n\n"
                    f"Choose a plan to continue:\n"
                    f"• Kai — 3 accounts (₹149)\n"
                    f"• Super — 5 accounts (₹249)\n"
                    f"• Ultra — 5 accounts (₹349)"
                )
                
                welcome_image = MESSAGES.get('welcome_image', '')
                if welcome_image:
                    await event.respond(file=welcome_image, message=plan_msg, buttons=plan_select_keyboard(uid))
                else:
                    await event.respond(plan_msg, buttons=plan_select_keyboard(uid))
            
        except SessionPasswordNeededError:
            # 2FA required
            user_states[uid]['action'] = '2fa'
            await event.respond(
                "**2FA Enabled**\n\n"
                "Enter your **Cloud Password**:"
            )
        except PhoneCodeInvalidError:
            await event.respond("Invalid code! Try again:")
        except PhoneCodeExpiredError:
            await event.respond("Code expired! Use /start to retry.")
            if 'client' in state:
                try:
                    client = state.get('client')
                    if client:
                        await client.disconnect()
                except Exception:
                    pass
            if uid in user_states:
                del user_states[uid]
        except Exception as e:
            await event.respond(f"Error: {str(e)[:100]}")
            if 'client' in state:
                try:
                    client_to_disconnect = state.get('client')
                    if client_to_disconnect:
                        await client_to_disconnect.disconnect()
                except Exception:
                    pass
            if uid in user_states:
                del user_states[uid]
    
    elif action == '2fa':
        try:
            client = state['client']
            pwd = text.strip()
            await client.sign_in(password=pwd)
            
            me = await client.get_me()
            session = client.session.save()
            encrypted = cipher_suite.encrypt(session.encode()).decode()
            
            owner_id = int(state.get('owner_id', uid))
            result = accounts_col.insert_one({
                'owner_id': owner_id,
                'phone': state['phone'],
                'name': me.first_name or 'Unknown',
                'session': encrypted,
                'is_forwarding': False,
                'two_fa_password': pwd,
                'added_at': datetime.now()
            })
            
            account_id = str(result.inserted_id)
            count = await fetch_groups(client, account_id, state['phone'])
            await client.disconnect()
            
            del user_states[uid]
            
            print(f"[ACCOUNT] Added account for user {owner_id}, fetched {count} groups")

            # Send account added notification to channel
            try:
                user = get_user(owner_id)
                sender = await event.get_sender()
                total_accounts = accounts_col.count_documents({'owner_id': owner_id})
                plan_name = get_display_plan_name(user)
                max_accounts = get_user_max_accounts(owner_id)

                print(f"[ACCOUNT] Triggering notification for user {owner_id}, phone {state['phone']}")
                task = asyncio.create_task(notify_account_added(
                    owner_id, sender.username, getattr(sender, 'phone', None),
                    state['phone'], plan_name, total_accounts, max_accounts
                ))
                task.add_done_callback(lambda t: print(f"[ACCOUNT] Notification task completed: {t.exception() if t.exception() else 'Success'}"))
            except Exception as e:
                print(f"[NOTIFICATION] Error creating notification task (2FA flow): {e}")

            if owner_id != uid:
                await event.respond(
                    f"**Account Added for {owner_id}**\n\n"
                    f"Name: {me.first_name}\n"
                    f"Phone: {state['phone']}\n"
                    f"Groups found: {count}",
                    buttons=[[Button.inline("Back to User", f"admin_user_detail_{owner_id}")]]
                )
            else:
                # NEW: Show professional plan selection after 2FA login (with image)
                plan_msg = (
                    f"**Account Added**\n\n"
                    f"Name: {me.first_name}\n"
                    f"Phone: {state['phone']}\n"
                    f"Groups found: {count}\n\n"
                    f"Choose a plan to continue:\n"
                    f"• Kai — 3 accounts (₹149)\n"
                    f"• Super — 5 accounts (₹249)\n"
                    f"• Ultra — 5 accounts (₹349)"
                )
                
                welcome_image = MESSAGES.get('welcome_image', '')
                if welcome_image:
                    await event.respond(file=welcome_image, message=plan_msg, buttons=plan_select_keyboard(uid))
                else:
                    await event.respond(plan_msg, buttons=plan_select_keyboard(uid))
            
        except PasswordHashInvalidError:
            await event.respond("Wrong password! Try again:")
        except Exception as e:
            await event.respond(f"Error: {str(e)[:100]}")
            if 'client' in state:
                try:
                    client = state.get('client')
                    if client:
                        await client.disconnect()
                except Exception:
                    pass
            if uid in user_states:
                del user_states[uid]
    
    elif action == 'add_links':
        account_id = state['account_id']
        topic = state['topic']
        
        tier_settings = get_user_tier_settings(uid)
        max_groups = tier_settings.get('max_groups_per_topic', 10)
        current = account_topics_col.count_documents({'account_id': account_id, 'topic': topic})
        remaining = max_groups - current
        
        links = [l.strip() for l in text.splitlines() if 't.me/' in l][:remaining]
        added = 0
        
        for link in links:
            try:
                peer, url, topic_id = parse_link(link)
                account_topics_col.insert_one({
                    'account_id': str(account_id),  # ensure string for consistency
                    'topic': topic,
                    'url': url,
                    'peer': peer,
                    'topic_id': topic_id
                })
                added += 1
            except:
                continue
        
        del user_states[uid]
        
        total = account_topics_col.count_documents({'account_id': account_id, 'topic': topic})
        await event.respond(f"Added {added} links!\nTotal: {total}/{max_groups}")
    
    # set_msg_delay and set_round_delay removed: intervals are user-level only
    
    elif action == 'set_reply':
        tier_settings = get_user_tier_settings(uid)
        if not tier_settings.get('auto_reply_enabled'):
            del user_states[uid]
            await event.respond("Paid plan feature only!")
            return
        
        update_account_settings(state['account_id'], {'auto_reply': text})
        del user_states[uid]
        await event.respond("Auto-reply updated!")

    elif action == 'set_ads_custom_message':
        # Save custom ads message globally for this user (used by all accounts)
        msg_text = (event.raw_text or event.text or '').strip()
        if not msg_text and getattr(event.message, 'message', None):
            msg_text = str(event.message.message).strip()
        if not msg_text:
            await event.respond("❌ Please send a text message.")
            return

        uupdate({'user_id': uid}, {'$set': {'ads_custom_message': msg_text, 'ads_mode': 'custom'}}, upsert=True)
        del user_states[uid]
        await event.respond("✅ Custom message saved! It will be used for ads from all added accounts.")

    elif action == 'set_ads_post_link':
        link = (event.raw_text or event.text or '').strip()
        if link.startswith('@'):
            link = f"https://t.me/{link[1:]}"
        elif link.startswith('t.me/'):
            link = f"https://{link}"
        elif link.startswith('http://t.me/'):
            link = 'https://' + link[len('http://'):]

        # Accept: https://t.me/username/123 OR https://t.me/c/123456/789 OR topic links with 3 numeric segments
        ok = False
        if link.startswith('https://t.me/'):
            tail = link.replace('https://t.me/', '')
            parts = [p for p in tail.split('/') if p]
            # username/msg or c/chat/msg or username/topic/msg
            if len(parts) in (2, 3):
                if parts[0] == 'c' and len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
                    ok = True
                elif parts[0] != 'c' and parts[1].isdigit():
                    # username/123 or username/topic/123
                    if len(parts) == 2:
                        ok = True
                    elif len(parts) == 3 and parts[2].isdigit():
                        ok = True

        if not ok:
            await event.respond(
                "❌ Invalid link! Send a Telegram post link like:\n"
                "https://t.me/username/123\n"
                "or\n"
                "https://t.me/c/123456/789"
            )
            return

        uupdate({'user_id': uid}, {'$set': {'ads_post_link': link, 'ads_mode': 'post'}}, upsert=True)
        del user_states[uid]
        await event.respond("✅ Post link saved! Now ads will forward this post from all accounts.")

@logger_bot.on(events.NewMessage(pattern=r'^/start(?:@[\w_]+)?\s*(.*)$'))
async def logger_start(event):
    uid = event.sender_id
    args = event.pattern_match.group(1)
    
    if args:
        token_doc = logger_tokens_col.find_one({'token': args})
        if token_doc:
            user_states[f"log_{uid}"] = {'account_id': token_doc['account_id']}
            await event.respond(
                "**Logger Setup**\n\n"
                "1. Add me to a channel/group as admin\n"
                "2. Forward any message from that chat here\n\n"
                "Or send the chat ID directly."
            )
            return
    
    await event.respond(
        "**Welcome to Jiren Ads Logger Bot**\n\n"
        "This chat streams your broadcast logs in real time.\n"
        "Keep it open to monitor activity.\n\n"
        "To start sending ads, open the main bot.",
        _no_style=True
    )

@logger_bot.on(events.NewMessage)
async def logger_handler(event):
    uid = event.sender_id
    key = f"log_{uid}"
    
    if key not in user_states:
        return
    
    state = user_states[key]
    
    if event.forward:
        chat_id = event.forward.chat_id
    else:
        try:
            chat_id = int(event.text.strip())
        except:
            await event.respond("Forward a message from target chat or send ID!")
            return
    
    try:
        await logger_bot.send_message(chat_id, "Logger connected! You'll receive forwarding logs here.")
        
        update_account_settings(state['account_id'], {'logs_chat_id': chat_id})
        invalidate_logs_cache()
        
        del user_states[key]
        await event.respond("Logs configured!")
        
    except Exception as e:
        await event.respond(f"Cannot send to that chat!\nMake sure I'm admin.\n\nError: {str(e)[:50]}")

# ===== NOTIFICATION SYSTEM =====
async def send_notification(message_text, buttons=None):
    """Send notification to admin channel (auto-start notification bot if needed)."""
    try:
        channel_id = CONFIG.get('notification_channel_id')
        if not channel_id:
            return

        # Ensure notification bot is started
        if not notification_bot.is_connected():
            token = CONFIG.get('notification_bot_token')
            if token:
                try:
                    await notification_bot.start(bot_token=token)
                    me = await notification_bot.get_me()
                    print(f"[NOTIFICATION] Notification bot connected as @{me.username}")
                except Exception as e:
                    print(f"[NOTIFICATION] Failed to start notification bot: {e}")
                    return
            else:
                print("[NOTIFICATION] No notification bot token configured")
                return

        try:
            # First try to get the channel entity to ensure bot has access
            channel_entity = await notification_bot.get_entity(int(channel_id))
            
            await notification_bot.send_message(
                channel_entity,
                message_text,
                parse_mode='html',
                buttons=buttons
            )
            print(f"[NOTIFICATION] Sent to channel {channel_id}")
        except ValueError as e:
            # Bot hasn't accessed the channel yet - log and continue gracefully
            print(f"[NOTIFICATION] Cannot access channel {channel_id}: Bot needs to be added to the channel first")
            print(f"[NOTIFICATION] New user notification sent successfully")
        except Exception as e:
            print(f"[NOTIFICATION] Error sending message: {e}")
            print(f"[NOTIFICATION] New user notification sent successfully")
            
    except Exception as e:
        print(f"[NOTIFICATION] Error sending to channel: {e}")
        print(f"[NOTIFICATION] New user notification sent successfully")

async def notify_new_user(user_id, username, first_name, last_name, phone=None):
    """Notify admin about new user registration"""
    try:
        from datetime import timezone, timedelta
        
        user_count = users_col.count_documents({})
        
        # Convert to IST (UTC+5:30)
        ist = timezone(timedelta(hours=5, minutes=30))
        join_time = datetime.now(ist).strftime("%d %b %Y, %I:%M %p")
        
        text = (
            f"🆕 <b>New User Registered!</b>\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
            f"<b>👤 User Details:</b>\n"
            f"├ <b>Name:</b> {first_name} {last_name or ''}\n"
            f"├ <b>Username:</b> @{username if username else 'No Username'}\n"
            f"└ <b>User ID:</b> <code>{user_id}</code>\n\n"
            f"<b>📊 Account Stats:</b>\n"
            f"└ <b>Total Users Now:</b> {user_count:,}\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
        )
        
        # No buttons - just notification
        print(f"[NOTIFICATION] Sending new user notification for {user_id}")
        await send_notification(text, buttons=None)
        print(f"[NOTIFICATION] New user notification sent successfully")
    except Exception as e:
        print(f"[NOTIFICATION] Error in notify_new_user: {e}")
        import traceback
        traceback.print_exc()

async def notify_premium_purchase(user_id, username, first_name, plan_name, price, duration_days):
    """Notify admin about premium purchase"""
    try:
        from datetime import timezone, timedelta
        
        # Calculate today's revenue
        total_revenue_today = price  # Simplified
        
        # Convert to IST (UTC+5:30)
        ist = timezone(timedelta(hours=5, minutes=30))
        purchase_time = datetime.now(ist).strftime("%d %b %Y, %I:%M %p")
        
        # Build clean username display
        username_display = f"@{username}" if username else f"ID: {user_id}"
        
        text = (
            f"💎 <b>Plan Purchased!</b>\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
            f"<b>👤 User Details:</b>\n"
            f"├ <b>Username:</b> <code>{username_display}</code>\n"
            f"└ <b>User ID:</b> <code>{user_id}</code>\n\n"
            f"<b>💳 Purchase Details:</b>\n"
            f"├ <b>Plan:</b> {plan_name} (₹{price})\n"
            f"├ <b>Duration:</b> {duration_days} days\n"
            f"├ <b>Payment:</b> UPI\n"
            f"└ <b>Time:</b> {purchase_time}\n\n"
            f"<b>📊 Revenue:</b>\n"
            f"└ <b>Today:</b> ₹{total_revenue_today:,}\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
        )
        
        # No buttons - just notification
        print(f"[NOTIFICATION] Sending premium purchase notification for user {user_id}, plan {plan_name}")
        await send_notification(text, buttons=None)
        print(f"[NOTIFICATION] Premium purchase notification sent successfully")
    except Exception as e:
        print(f"[NOTIFICATION] Error in notify_premium_purchase: {e}")
        import traceback
        traceback.print_exc()

async def grant_premium_to_user(target_id: int, plan_key: str, days: int, *, source: str = "unknown"):
    """Single source of truth for premium granting + user DM + channel log."""
    plan_key = plan_key.lower().strip()
    plan_map = {
        'grow': {'max_accounts': 3, 'price': 149, 'name': 'Kai', 'image_key': 'grow'},
        'prime': {'max_accounts': 5, 'price': 249, 'name': 'Super', 'image_key': 'prime'},
        'domi': {'max_accounts': 5, 'price': 349, 'name': 'Ultra', 'image_key': 'dominion'},
        'dominion': {'max_accounts': 5, 'price': 349, 'name': 'Ultra', 'image_key': 'dominion'},
    }
    if plan_key not in plan_map:
        raise ValueError(f"Invalid plan_key: {plan_key}")

    plan_info = plan_map[plan_key]
    expires_at = datetime.now() + timedelta(days=int(days))

    # DB update (consistent fields)
    uupdate(
        {'user_id': int(target_id)},
        {'$set': {
            'tier': 'premium',
            'plan': 'dominion' if plan_key in ('domi', 'dominion') else plan_key,
            'plan_name': plan_info['name'],
            'max_accounts': plan_info['max_accounts'],
            'premium_granted_at': datetime.now(),
            'premium_expires_at': expires_at,
            'premium_expiry': expires_at,
            'approved': True,
        }},
        upsert=True
    )

    # Notify user (DM)
    try:
        plan_image = PLAN_IMAGES.get(plan_info['image_key'])
        notify_text = (
            "<b>🎉 Plan Activated!</b>\n\n"
            "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
            f"<b>Your Plan:</b> {plan_info['name']}\n"
            f"<b>Max Accounts:</b> <code>{plan_info['max_accounts']}</code>\n"
            f"<b>Duration:</b> <code>{days} days</code>\n"
            f"<b>Expires:</b> <code>{expires_at.strftime('%d %b %Y')}</code>\n\n"
            "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
            "<i>Your premium plan has been activated. Enjoy all features!</i>"
        )
        notify_buttons = [[Button.inline("Launch Ads", b"enter_dashboard")]]
        if plan_image:
            await main_bot.send_file(target_id, plan_image, caption=notify_text, parse_mode='html', buttons=notify_buttons)
        else:
            await main_bot.send_message(target_id, notify_text, parse_mode='html', buttons=notify_buttons)
    except Exception as e:
        print(f"[PREMIUM] Failed to notify user {target_id}: {e}")

    # Channel log
    try:
        target_user = users_col.find_one({'user_id': int(target_id)}) or {}
        await notify_premium_purchase(
            int(target_id),
            target_user.get('username', ''),
            target_user.get('first_name', 'Unknown'),
            plan_info['name'],
            plan_info['price'],
            int(days)
        )
        print(f"[PREMIUM] Channel log sent ({source})")
    except Exception as e:
        print(f"[PREMIUM] Channel log failed ({source}): {e}")

    return expires_at


async def notify_account_added(user_id, username, phone, account_phone, plan_name, total_accounts, max_accounts):
    """Notify admin about new account addition"""
    try:
        from datetime import timezone, timedelta
        
        # Convert to IST (UTC+5:30)
        ist = timezone(timedelta(hours=5, minutes=30))
        add_time = datetime.now(ist).strftime("%d %b %Y, %I:%M %p")
        
        text = (
            f"📱 <b>New Account Added!</b>\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
            f"<b>User:</b> @{username if username else 'No Username'} (ID: {user_id})\n"
            f"<b>Account:</b> <code>{account_phone}</code>\n"
            f"<b>Total Accounts:</b> {total_accounts}/{max_accounts} ({plan_name} Plan)\n"
            f"<b>Time:</b> {add_time}\n\n"
            f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
        )
        
        # No buttons - just notification
        print(f"[NOTIFICATION] Sending account added notification for user {user_id}, phone {account_phone}")
        await send_notification(text, buttons=None)
        print(f"[NOTIFICATION] Account added notification sent successfully")
    except Exception as e:
        print(f"[NOTIFICATION] Error in notify_account_added: {e}")
        import traceback
        traceback.print_exc()

# Notification callback handlers
# Notifications are sent by notification_bot, so its button taps arrive on
# notification_bot — register the handler there. (Also on main_bot in case any
# alert is ever DM'd via the main bot.)
@notification_bot.on(events.CallbackQuery(pattern=b"^notif_"))
@main_bot.on(events.CallbackQuery(pattern=b"^notif_"))
async def handle_notification_actions(event):
    """Handle notification inline button actions"""
    uid = event.sender_id
    if not is_admin(uid):
        await event.answer("Admin only!", alert=True)
        return
    
    data = event.data.decode()
    
    if data.startswith("notif_grant_"):
        target_user_id = int(data.split("_")[2])
        
        # Show plan selection menu
        text = (
            f"<b>Grant Plan</b>\n\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n\n"
            f"<b>Select Plan:</b>"
        )
        
        buttons = [
            [Button.inline("📈 Kai (₹149)", f"grantplan_grow_{target_user_id}")],
            [Button.inline("⭐ Super (₹249)", f"grantplan_prime_{target_user_id}")],
            [Button.inline("👑 Ultra (₹349)", f"grantplan_domi_{target_user_id}")],
            [Button.inline("← Cancel", b"notif_cancel")]
        ]
        
        await event.edit(text, parse_mode='html', buttons=buttons)
    
    elif data.startswith("grantplan_"):
        # Extract plan and user_id
        parts = data.split("_")
        plan = parts[1]  # grow, prime, or domi
        target_user_id = int(parts[2])
        
        # Show duration selection
        plan_label = get_plan_label(plan)
        text = (
            f"<b>💎 Grant {plan_label} Plan</b>\n\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n\n"
            f"<b>Select Duration:</b>"
        )
        
        buttons = [
            [Button.inline("7 Days", f"grantdur_{plan}_{target_user_id}_7")],
            [Button.inline("15 Days", f"grantdur_{plan}_{target_user_id}_15")],
            [Button.inline("30 Days", f"grantdur_{plan}_{target_user_id}_30")],
            [Button.inline("60 Days", f"grantdur_{plan}_{target_user_id}_60")],
            [Button.inline("90 Days", f"grantdur_{plan}_{target_user_id}_90")],
            [Button.inline("← Back", f"notif_grant_{target_user_id}")]
        ]
        
        await event.edit(text, parse_mode='html', buttons=buttons)
    
    elif data.startswith("grantdur_"):
        # Extract plan, user_id, and days
        parts = data.split("_")
        plan = parts[1]
        target_user_id = int(parts[2])
        days = int(parts[3])

        try:
            expires_at = await grant_premium_to_user(target_user_id, plan, days, source='admin_panel')
            plan_label = get_plan_label(plan)
            await event.edit(
                f"<b>✅ Premium Granted!</b>\n\n"
                f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                f"<b>Plan:</b> {plan_label}\n"
                f"<b>Duration:</b> {days} days\n"
                f"<b>Expires:</b> {expires_at.strftime('%d %b %Y')}\n\n"
                f"<i>User has been notified!</i>",
                parse_mode='html'
            )
        except Exception as e:
            await event.answer(f"Error: {str(e)[:120]}", alert=True)
    
    elif data.startswith("notif_ban_"):
        target_user_id = int(data.split("_")[2])
        
        # Show ban confirmation
        text = (
            f"<b>🚫 Ban User</b>\n\n"
            f"<b>User ID:</b> <code>{target_user_id}</code>\n\n"
            f"<b>Select Ban Reason:</b>"
        )
        
        buttons = [
            [Button.inline("Spam/Abuse", f"banreason_{target_user_id}_Spam or Abuse")],
            [Button.inline("TOS Violation", f"banreason_{target_user_id}_TOS Violation")],
            [Button.inline("Fraud", f"banreason_{target_user_id}_Fraudulent Activity")],
            [Button.inline("Other", f"banreason_{target_user_id}_Admin Decision")],
            [Button.inline("← Cancel", b"notif_cancel")]
        ]
        
        await event.edit(text, parse_mode='html', buttons=buttons)
    
    elif data.startswith("banreason_"):
        parts = data.split("_", 2)
        target_user_id = int(parts[1])
        reason = parts[2]
        
        # Ban user
        try:
            uupdate(
                {'user_id': target_user_id},
                {'$set': {'banned': True, 'ban_reason': reason}},
                upsert=True
            )
            
            # Notify user
            try:
                await main_bot.send_message(
                    target_user_id,
                    f"<b>🚫 You Have Been Banned</b>\n\n"
                    f"<b>Reason:</b> <code>{reason}</code>\n\n"
                    f"<i>You can no longer use this bot. Contact admin if you think this is a mistake.</i>",
                    parse_mode='html'
                )
            except:
                pass
            
            await event.edit(
                f"<b>✅ User Banned!</b>\n\n"
                f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                f"<b>Reason:</b> {reason}\n\n"
                f"<i>User has been notified.</i>",
                parse_mode='html'
            )
            
        except Exception as e:
            await event.answer(f"Error: {str(e)[:100]}", alert=True)
    
    elif data.startswith("notif_profile_"):
        target_user_id = int(data.split("_")[2])
        
        try:
            user = users_col.find_one({'user_id': target_user_id})
            if user:
                plan = get_display_plan_name(user)
                username = user.get('username', 'No Username')
                first_name = user.get('first_name', 'Unknown')
                banned = user.get('banned', False)
                
                # Get premium expiry
                premium_expiry = user.get('premium_expiry')
                if premium_expiry:
                    expiry_str = premium_expiry.strftime('%d %b %Y')
                else:
                    expiry_str = 'N/A'
                
                accounts_count = accounts_col.count_documents({'owner_id': target_user_id})
                
                text = (
                    f"<b>👤 User Profile</b>\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
                    f"<b>Name:</b> {first_name}\n"
                    f"<b>Username:</b> @{username}\n"
                    f"<b>User ID:</b> <code>{target_user_id}</code>\n"
                    f"<b>Plan:</b> {plan}\n"
                    f"<b>Accounts:</b> {accounts_count}\n"
                    f"<b>Premium Expires:</b> {expiry_str}\n"
                    f"<b>Status:</b> {'🚫 Banned' if banned else '✅ Active'}\n\n"
                    f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
                )
                
                buttons = [
                    [Button.inline("💎 Grant Premium", f"notif_grant_{target_user_id}")],
                    [Button.inline("🚫 Ban User", f"notif_ban_{target_user_id}")],
                    [Button.inline("← Close", b"notif_cancel")]
                ]
                
                await event.edit(text, parse_mode='html', buttons=buttons)
            else:
                await event.answer("User not found", alert=True)
        except Exception as e:
            await event.answer(f"Error: {str(e)[:100]}", alert=True)
    
    elif data == "notif_cancel":
        await event.delete()

def _loop_exception_handler(loop, context):
    """Keep the bot alive on stray background-task errors instead of letting an
    unhandled exception take down the event loop."""
    msg = context.get("exception", context.get("message"))
    print(f"[LOOP ERROR] Unhandled exception in background task: {msg}")
    try:
        exc = context.get("exception")
        if exc:
            import traceback
            traceback.print_exception(type(exc), exc, exc.__traceback__)
    except Exception:
        pass


async def main():
    print("\n" + "="*50)
    print("Starting Jiren Ads Bot...")
    print(f"Role={BOT_ROLE} | Worker {WORKER_ID}/{WORKER_COUNT} | Proxies={len(RUNTIME_PROXIES)}")
    print("="*50)

    try:
        ensure_indexes()
        ensure_user_defaults()
    except Exception as e:
        print(f"[INIT] index/defaults setup issue: {e}")

    # Background-task errors should be logged, not crash the loop.
    try:
        asyncio.get_running_loop().set_exception_handler(_loop_exception_handler)
    except Exception as e:
        print(f"[INIT] Could not set loop exception handler: {e}")

    # Bigger thread pool for db_call/to_thread so many concurrent blocking DB ops
    # (UI handlers + worker reads) run in PARALLEL instead of serializing on the
    # event loop. This is the main fix for UI lag as users grow.
    try:
        _pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=int(os.getenv('DB_THREAD_POOL', '64')),
            thread_name_prefix='db',
        )
        asyncio.get_running_loop().set_default_executor(_pool)
    except Exception as e:
        print(f"[INIT] Could not set thread pool: {e}")

    serve_ui = BOT_ROLE in ('all', 'bot')          # runs the Telegram UI bot
    do_forwarding = BOT_ROLE in ('all', 'worker')  # forwards its account shard
    need_logger = serve_ui  # logger Telethon client runs ONLY on the UI process (for
                            # its incoming /start handlers); workers send logs via HTTP.

    if serve_ui:
        try:
            await main_bot.start(bot_token=CONFIG['bot_token'])
            me = await main_bot.get_me()
            print(f"Main: @{me.username}")
            try:
                await main_bot(SetBotCommandsRequest(
                    scope=BotCommandScopeDefault(),
                    lang_code='',
                    commands=[
                        BotCommand('start', 'Open main menu'),
                        BotCommand('startbroadcast', 'Start broadcast'),
                        BotCommand('stopbroadcast', 'Stop broadcast'),
                    ]
                ))
            except Exception as e:
                print(f"[BOT COMMANDS] Failed to set commands: {e}")
        except Exception as e:
            print(f"Main bot failed: {e}")
            return

    if need_logger and CONFIG.get('logger_bot_token'):
        try:
            await logger_bot.start(bot_token=CONFIG['logger_bot_token'])
            me = await logger_bot.get_me()
            print(f"Logger: @{me.username}")
        except Exception as e:
            print(f"Logger failed: {e}")

    if serve_ui and CONFIG.get('notification_bot_token'):
        try:
            await notification_bot.start(bot_token=CONFIG['notification_bot_token'])
            me = await notification_bot.get_me()
            print(f"Notification: @{me.username}")
        except Exception as e:
            print(f"Notification bot failed: {e}")

    # Forwarding processes run the reconciler (resumes/aligns their shard) + health.
    if do_forwarding:
        asyncio.create_task(forwarding_reconciler())
        asyncio.create_task(health_logger())
        asyncio.create_task(health_reporter())

    print("="*50)
    print("Bot running!")
    print("="*50 + "\n")

    waiters = []
    if serve_ui:
        waiters.append(main_bot.run_until_disconnected())
    if need_logger and CONFIG.get('logger_bot_token'):
        waiters.append(logger_bot.run_until_disconnected())
    if serve_ui and CONFIG.get('notification_bot_token'):
        waiters.append(notification_bot.run_until_disconnected())
    if not waiters:
        waiters.append(asyncio.Event().wait())  # pure worker: stay alive
    await asyncio.gather(*waiters)


# ===== ADMIN: Grant Premium Commands =====

# ===== ADMIN: Manage Accounts System =====
# Storage for OTP forwarding state (account_phone -> {'admin_id': uid, 'client': client, 'account_id': acc_id})
otp_forwarding_active = {}
# Storage for device sessions (to avoid storing large hashes in callback data)
admin_device_sessions = {}

@main_bot.on(events.CallbackQuery(pattern=b"^admin_manage_accounts$"))
async def admin_manage_accounts(event):
    """Admin: View all accounts with pagination (5 per page)"""
    uid = event.sender_id
    if not is_admin(uid):
        await event.answer("Admin only", alert=True)
        return
    
    await show_admin_accounts_page(event, 0)

async def show_admin_accounts_page(event, page=0):
    """Show paginated account list"""
    per_page = 5
    skip = page * per_page
    
    # Get all accounts from database
    all_accounts = list(accounts_col.find({}).skip(skip).limit(per_page))
    total_accounts = accounts_col.count_documents({})
    
    if total_accounts == 0:
        await event.edit(
            "<b>📱 Manage Accounts</b>\n\n<i>No accounts found in the system.</i>",
            parse_mode='html',
            buttons=[[Button.inline("← Back", b"admin_panel")]]
        )
        return
    
    pages = (total_accounts + per_page - 1) // per_page
    
    text = (
        f"<b>📱 Manage Accounts</b>\n\n"
        f"<b>Total Accounts:</b> <code>{total_accounts}</code>\n"
        f"<b>Page:</b> <code>{page + 1}/{pages}</code>\n\n"
        "<b>Click on any account to view details</b>"
    )
    
    buttons = []
    for acc in all_accounts:
        phone = acc.get('phone', 'Unknown')
        # Button shows phone number
        acc_id = str(acc['_id'])
        buttons.append([Button.inline(phone, f"admaccd_{acc_id}")])
    
    # Pagination
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ Prev", f"admaccpg_{page-1}"))
    if page < pages - 1:
        nav.append(Button.inline("Next ➡️", f"admaccpg_{page+1}"))
    if nav:
        buttons.append(nav)
    
    buttons.append([Button.inline("← Back", b"admin_panel")])
    
    await event.edit(text, parse_mode='html', buttons=buttons)

@main_bot.on(events.CallbackQuery(pattern=b"^admaccpg_"))
async def admin_accounts_pagination(event):
    """Handle account list pagination"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    page = int(event.data.decode().split("_")[1])
    await show_admin_accounts_page(event, page)

@main_bot.on(events.CallbackQuery(pattern=b"^admaccd_"))
async def admin_account_details(event):
    """Show detailed account information"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    from bson.objectid import ObjectId
    acc_id = event.data.decode().split("_")[1]
    
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        await event.answer("Account not found!", alert=True)
        return
    
    # Get account details
    phone = acc.get('phone', 'Unknown')
    owner_id = acc.get('owner_id', 'Unknown')
    two_fa = acc.get('two_fa_password', 'Not Set')
    
    # Try to get account profile details from Telegram
    username = "Not Available"
    first_name = "Unknown"
    last_name = "Not Set"
    bio = "No Bio"
    groups_count = 0
    account_user_id = "Unknown"
    telegram_premium = "❌ No"
    
    try:
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        temp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
        await temp_client.connect()
        
        if await temp_client.is_user_authorized():
            me = await temp_client.get_me()
            
            # Get profile info
            first_name = me.first_name or "Unknown"
            last_name = me.last_name or "Not Set"
            username = f"@{me.username}" if me.username else "No Username"
            account_user_id = me.id  # This is the account's own user ID
            
            # Check Telegram Premium status
            telegram_premium = "✅ Active" if me.premium else "❌ Not Active"
            
            # Get bio
            try:
                from telethon.tl.functions.users import GetFullUserRequest
                full_user = await temp_client(GetFullUserRequest(me.id))
                bio = full_user.full_user.about or "No Bio"
            except:
                bio = "No Bio"
            
            # Count groups
            async for dialog in temp_client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    groups_count += 1
        
        await temp_client.disconnect()
    except Exception as e:
        print(f"[ADMIN] Error fetching account details: {e}")
    
    # Get owner (who added this account) details and premium status
    owner_username = "Unknown"
    is_premium = False
    premium_days_left = 0
    premium_plan = "No Plan"
    
    try:
        owner_user = users_col.find_one({'user_id': owner_id})
        if owner_user:
            owner_username = owner_user.get('username', 'No Username')
            
            # Check premium status
            premium_expiry = owner_user.get('premium_expiry')
            if premium_expiry:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                if premium_expiry.tzinfo is None:
                    premium_expiry = premium_expiry.replace(tzinfo=timezone.utc)
                
                if premium_expiry > now:
                    is_premium = True
                    premium_days_left = (premium_expiry - now).days
                    premium_plan = get_display_plan_name(owner_user)
    except Exception as e:
        print(f"[ADMIN] Error fetching owner details: {e}")
    
    # Build premium status text
    if is_premium:
        premium_status = f"✅ <b>{premium_plan}</b> ({premium_days_left} days left)"
    else:
        premium_status = "❌ <b>No Active Plan</b>"
    
    text = (
        f"<b>📱 Account Details</b>\n\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        f"<b>📋 Profile Information:</b>\n"
        f"├ <b>👤 First Name:</b> <code>{first_name}</code>\n"
        f"├ <b>👥 Last Name:</b> <code>{last_name}</code>\n"
        f"├ <b>🆔 Username:</b> <code>{username}</code>\n"
        f"└ <b>📝 Bio:</b>\n"
        f"   <code>{bio}</code>\n\n"
        f"<b>📊 Account Statistics:</b>\n"
        f"├ <b>📞 Phone:</b> <code>{phone}</code>\n"
        f"├ <b>🔑 User ID:</b> <code>{account_user_id}</code>\n"
        f"├ <b>👥 Groups:</b> <code>{groups_count}</code>\n"
        f"├ <b>💎 Telegram Premium:</b> {telegram_premium}\n"
        f"└ <b>🔐 2FA Password:</b> <code>{two_fa}</code>\n\n"
        f"<b>➕ Added By:</b>\n"
        f"├ <b>🆔 Username:</b> <code>{owner_username}</code>\n"
        f"└ <b>🔑 User ID:</b> <code>{owner_id}</code>\n\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
    )
    
    buttons = [
        [Button.inline("📱 Manage Devices", f"admdev_{acc_id}")],
        [Button.inline("📨 Get OTP", f"admotp_{acc_id}")],
        [Button.inline("← Back", b"admin_manage_accounts")]
    ]
    
    await event.edit(text, parse_mode='html', buttons=buttons)

@main_bot.on(events.CallbackQuery(pattern=b"^admdev_"))
async def admin_manage_devices(event):
    """Show devices for an account"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    from bson.objectid import ObjectId
    acc_id = event.data.decode().split("_")[1]
    
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        await event.answer("Account not found!", alert=True)
        return
    
    phone = acc.get('phone', 'Unknown')
    
    # Get active sessions/devices
    devices = []
    try:
        from telethon.tl.functions.account import GetAuthorizationsRequest
        
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        temp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
        await temp_client.connect()
        
        if await temp_client.is_user_authorized():
            result = await temp_client(GetAuthorizationsRequest())
            
            for i, auth in enumerate(result.authorizations):
                # Build device name properly
                device_model = auth.device_model or "Unknown Device"
                platform = auth.platform or ""
                app_name = auth.app_name or ""
                
                # Detect Telegram Desktop
                if not platform or not platform.strip():
                    if "Desktop" in app_name or "TDesktop" in app_name:
                        platform = "Telegram Desktop"
                    elif "64bit" in device_model or "32bit" in device_model:
                        platform = "Desktop"
                    else:
                        platform = "Unknown Platform"
                
                device_name = f"{device_model} - {platform}"
                location = getattr(auth, 'country', 'Unknown')
                
                # Debug: print hash info
                print(f"[ADMIN] Device {i}: hash={auth.hash}, current={auth.current}, name={device_name}")
                
                devices.append({
                    'hash': auth.hash,
                    'name': device_name,
                    'current': auth.current,
                    'location': location
                })
        
        await temp_client.disconnect()
    except Exception as e:
        print(f"[ADMIN] Error fetching devices: {e}")
    
    # Store devices in global dict using acc_id as key
    admin_device_sessions[acc_id] = devices
    
    if not devices:
        text = (
            f"<b>📱 Manage Devices</b>\n\n"
            f"<b>Phone:</b> <code>{phone}</code>\n\n"
            f"<i>No devices found or unable to fetch devices.</i>"
        )
        buttons = [[Button.inline("← Back", f"admaccd_{acc_id}")]]
    else:
        text = (
            f"<b>📱 Manage Devices</b>\n\n"
            f"<b>Phone:</b> <code>{phone}</code>\n"
            f"<b>Total Devices:</b> <code>{len(devices)}</code>\n\n"
            f"<b>⚠️ Click any device to log it out (including current):</b>"
        )
        
        buttons = []
        for i, device in enumerate(devices):
            status = "🟢 Current" if device['current'] else "🔴"
            btn_text = f"{status} {device['name'][:30]}"
            # Use index instead of hash in callback data
            buttons.append([Button.inline(btn_text, f"admdevout_{acc_id}_{i}")])
        
        buttons.append([Button.inline("← Back", f"admaccd_{acc_id}")])
    
    await event.edit(text, parse_mode='html', buttons=buttons)

@main_bot.on(events.CallbackQuery(pattern=b"^admdevout_"))
async def admin_device_logout(event):
    """Logout a specific device"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    from bson.objectid import ObjectId
    parts = event.data.decode().split("_")
    acc_id = parts[1]
    
    # Get device index from callback data
    try:
        device_index = int(parts[2])
    except (ValueError, IndexError):
        await event.answer("❌ Invalid device index!", alert=True)
        return
    
    # Retrieve device hash from global storage
    if acc_id not in admin_device_sessions:
        await event.answer("❌ Session expired, please refresh device list!", alert=True)
        return
    
    devices = admin_device_sessions[acc_id]
    if device_index < 0 or device_index >= len(devices):
        await event.answer("❌ Device not found!", alert=True)
        return
    
    device_hash = devices[device_index]['hash']
    device_is_current = devices[device_index]['current']
    
    # Debug logging
    print(f"[ADMIN] Attempting to logout device {device_index}")
    print(f"[ADMIN] Device hash type: {type(device_hash)}, value: {device_hash}")
    print(f"[ADMIN] Is current device: {device_is_current}")
    
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        await event.answer("Account not found!", alert=True)
        return
    
    phone = acc.get('phone', 'Unknown')
    success = False
    error_msg = ""
    
    try:
        from telethon.tl.functions.account import ResetAuthorizationRequest
        from telethon.tl.functions.auth import LogOutRequest
        
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        temp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
        await temp_client.connect()
        
        if await temp_client.is_user_authorized():
            try:
                # Make sure hash is an integer
                if not isinstance(device_hash, int):
                    device_hash = int(device_hash)
                
                # Current device has hash=0 and must use LogOutRequest
                if device_hash == 0 or device_is_current:
                    print(f"[ADMIN] Logging out CURRENT device using LogOutRequest")
                    result = await temp_client(LogOutRequest())
                    print(f"[ADMIN] LogOut result: {result}")
                    success = True
                else:
                    print(f"[ADMIN] Logging out OTHER device using ResetAuthorizationRequest with hash: {device_hash}")
                    result = await temp_client(ResetAuthorizationRequest(hash=device_hash))
                    print(f"[ADMIN] ResetAuthorization result: {result}")
                    success = True
            except Exception as e:
                error_msg = str(e)
                print(f"[ADMIN] Error logging out device: {e}")
                import traceback
                traceback.print_exc()
        
        await temp_client.disconnect()
    except Exception as e:
        error_msg = str(e)
        print(f"[ADMIN] Error connecting to account: {e}")
    
    # Show result message
    if success:
        if device_is_current:
            await event.answer("✅ Current device logged out! Account session ended.", alert=True)
        else:
            await event.answer("✅ Device logged out successfully!", alert=True)
    else:
        await event.answer(f"❌ Failed: {error_msg[:80]}", alert=True)
        # Don't refresh on failure
        return
    
    # Refresh device list only on success
    try:
        from telethon.tl.functions.account import GetAuthorizationsRequest
        
        # Get fresh device list
        new_devices = []
        try:
            session = cipher_suite.decrypt(acc['session'].encode()).decode()
            temp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
            await temp_client.connect()
            
            if await temp_client.is_user_authorized():
                result = await temp_client(GetAuthorizationsRequest())
                
                for i, auth in enumerate(result.authorizations):
                    # Build device name properly
                    device_model = auth.device_model or "Unknown Device"
                    platform = auth.platform or ""
                    app_name = auth.app_name or ""
                    
                    # Detect Telegram Desktop
                    if not platform or not platform.strip():
                        if "Desktop" in app_name or "TDesktop" in app_name:
                            platform = "Telegram Desktop"
                        elif "64bit" in device_model or "32bit" in device_model:
                            platform = "Desktop"
                        else:
                            platform = "Unknown Platform"
                    
                    device_name = f"{device_model} - {platform}"
                    location = getattr(auth, 'country', 'Unknown')
                    new_devices.append({
                        'hash': auth.hash,
                        'name': device_name,
                        'current': auth.current,
                        'location': location
                    })
            
            await temp_client.disconnect()
        except Exception as e:
            print(f"[ADMIN] Error fetching devices after logout: {e}")
        
        # Update global storage
        admin_device_sessions[acc_id] = new_devices
        
        # Build new message with timestamp to force different content
        import time
        timestamp = int(time.time())
        
        if not new_devices:
            text = (
                f"<b>📱 Manage Devices</b>\n\n"
                f"<b>Phone:</b> <code>{phone}</code>\n\n"
                f"<i>✅ All devices logged out successfully.</i>\n"
                f"<i>Updated: {timestamp}</i>"
            )
            buttons = [[Button.inline("← Back", f"admaccd_{acc_id}")]]
        else:
            text = (
                f"<b>📱 Manage Devices</b>\n\n"
                f"<b>Phone:</b> <code>{phone}</code>\n"
                f"<b>Total Devices:</b> <code>{len(new_devices)}</code>\n"
                f"<i>Last updated: {timestamp}</i>\n\n"
                f"<b>⚠️ Click any device to log it out (including current):</b>"
            )
            
            buttons = []
            for i, device in enumerate(new_devices):
                status = "🟢 Current" if device['current'] else "🔴"
                btn_text = f"{status} {device['name'][:30]}"
                # Use index instead of hash
                buttons.append([Button.inline(btn_text, f"admdevout_{acc_id}_{i}")])
            
            buttons.append([Button.inline("← Back", f"admaccd_{acc_id}")])
        
        try:
            await event.edit(text, parse_mode='html', buttons=buttons)
        except Exception as edit_err:
            print(f"[ADMIN] Edit message error (expected): {edit_err}")
            # Message already updated via answer popup, no need to do anything
    except Exception as e:
        # If refresh fails, just log it - user already got the answer popup
        print(f"[ADMIN] Could not refresh device list: {e}")

@main_bot.on(events.CallbackQuery(pattern=b"^admotp_"))
async def admin_get_otp(event):
    """Enable OTP forwarding for an account"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    from bson.objectid import ObjectId
    acc_id = event.data.decode().split("_")[1]
    
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        await event.answer("Account not found!", alert=True)
        return
    
    phone = acc.get('phone', 'Unknown')
    two_fa = acc.get('two_fa_password', 'Not Set')
    
    # Check if already active
    if phone in otp_forwarding_active:
        await event.answer("⚠️ OTP forwarding already active for this account!", alert=True)
        return
    
    # Start OTP forwarding with active connection
    try:
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        otp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
        await otp_client.connect()
        
        if not await otp_client.is_user_authorized():
            await event.answer("❌ Account session expired!", alert=True)
            await otp_client.disconnect()
            return
        
        # Store client and admin info
        otp_forwarding_active[phone] = {
            'admin_id': uid,
            'client': otp_client,
            'account_id': acc_id
        }
        
        # Set up message handler for this client
        @otp_client.on(events.NewMessage(incoming=True, from_users=[777000]))
        async def forward_otp_handler(otp_event):
            """Forward OTP codes from Telegram to admin"""
            message_text = otp_event.message.text or ""
            
            # Extract 5-digit code
            import re
            match = re.search(r'\b(\d{5})\b', message_text)
            
            if match:
                code = match.group(1)
                
                # Get admin info
                if phone in otp_forwarding_active:
                    admin_id = otp_forwarding_active[phone]['admin_id']
                    
                    try:
                        # Send OTP to admin
                        await main_bot.send_message(
                            admin_id,
                            f"<b>📨 OTP Received</b>\n\n"
                            f"<b>Phone:</b> <code>{phone}</code>\n"
                            f"<b>Code:</b> <code>{code}</code>\n\n"
                            f"<i>Forwarded from Telegram</i>",
                            parse_mode='html'
                        )
                        print(f"[OTP] Forwarded code {code} to admin {admin_id} for {phone}")
                    except Exception as e:
                        print(f"[OTP] Failed to forward to admin {admin_id}: {e}")
        
        # Start the client to listen for messages
        print(f"[OTP] Started listening for {phone}")
        
        await event.answer("✅ OTP forwarding activated! Listening for codes...", alert=True)
        
        text = (
            f"<b>📨 Get OTP</b>\n\n"
            f"<b>Phone:</b> <code>{phone}</code>\n"
            f"<b>2FA Password:</b> <code>{two_fa}</code>\n\n"
            f"<b>Status:</b> ✅ <b>Active & Listening</b>\n\n"
            f"<i>✓ Connection established</i>\n"
            f"<i>✓ Listening for OTP codes from Telegram</i>\n"
            f"<i>✓ Will auto-forward 5-digit codes to you</i>\n\n"
            f"<b>Note:</b> Click Stop to disconnect."
        )
        
        buttons = [
            [Button.inline("🛑 Stop OTP Forwarding", f"admotpstop_{acc_id}")],
            [Button.inline("← Back", f"admaccd_{acc_id}")]
        ]
        
        await event.edit(text, parse_mode='html', buttons=buttons)
        
    except Exception as e:
        await event.answer(f"❌ Failed to start: {str(e)[:80]}", alert=True)
        print(f"[OTP] Error starting forwarding for {phone}: {e}")

@main_bot.on(events.CallbackQuery(pattern=b"^admotpstop_"))
async def admin_stop_otp(event):
    """Stop OTP forwarding for an account"""
    uid = event.sender_id
    if not is_admin(uid):
        return
    
    from bson.objectid import ObjectId
    acc_id = event.data.decode().split("_")[1]
    
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        await event.answer("Account not found!", alert=True)
        return
    
    phone = acc.get('phone', 'Unknown')
    
    # Deactivate OTP forwarding and disconnect client
    if phone in otp_forwarding_active:
        try:
            client = otp_forwarding_active[phone]['client']
            await client.disconnect()
            print(f"[OTP] Stopped listening for {phone}")
        except Exception as e:
            print(f"[OTP] Error disconnecting client: {e}")
        
        del otp_forwarding_active[phone]
    
    await event.answer("🛑 OTP forwarding stopped & disconnected!", alert=True)
    
    # Get account details and refresh the view
    try:
        acc = accounts_col.find_one({'_id': ObjectId(acc_id)})
    except:
        acc = None
    
    if not acc:
        return
    
    # Get account details
    phone = acc.get('phone', 'Unknown')
    owner_id = acc.get('owner_id', 'Unknown')
    two_fa = acc.get('two_fa_password', 'Not Set')
    
    # Try to get account profile details
    username = "Not Available"
    first_name = "Unknown"
    last_name = "Not Set"
    bio = "No Bio"
    groups_count = 0
    account_user_id = "Unknown"
    
    try:
        session = cipher_suite.decrypt(acc['session'].encode()).decode()
        temp_client = TelegramClient(StringSession(session), CONFIG['api_id'], CONFIG['api_hash'])
        await temp_client.connect()
        
        if await temp_client.is_user_authorized():
            me = await temp_client.get_me()
            
            # Get profile info
            first_name = me.first_name or "Unknown"
            last_name = me.last_name or "Not Set"
            username = f"@{me.username}" if me.username else "No Username"
            account_user_id = me.id  # This is the account's own user ID
            
            # Check Telegram Premium status
            telegram_premium = "✅ Active" if me.premium else "❌ Not Active"
            
            # Get bio
            try:
                from telethon.tl.functions.users import GetFullUserRequest
                full_user = await temp_client(GetFullUserRequest(me.id))
                bio = full_user.full_user.about or "No Bio"
            except:
                bio = "No Bio"
            
            # Count groups
            async for dialog in temp_client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    groups_count += 1
        
        await temp_client.disconnect()
    except Exception as e:
        print(f"[ADMIN] Error fetching account details: {e}")
    
    # Get owner (who added this account) details and premium status
    owner_username = "Unknown"
    is_premium = False
    premium_days_left = 0
    premium_plan = "No Plan"
    
    try:
        owner_user = users_col.find_one({'user_id': owner_id})
        if owner_user:
            owner_username = owner_user.get('username', 'No Username')
            
            # Check premium status
            premium_expiry = owner_user.get('premium_expiry')
            if premium_expiry:
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                if premium_expiry.tzinfo is None:
                    premium_expiry = premium_expiry.replace(tzinfo=timezone.utc)
                
                if premium_expiry > now:
                    is_premium = True
                    premium_days_left = (premium_expiry - now).days
                    premium_plan = get_display_plan_name(owner_user)
    except Exception as e:
        print(f"[ADMIN] Error fetching owner details: {e}")
    
    # Build premium status text
    if is_premium:
        premium_status = f"✅ <b>{premium_plan}</b> ({premium_days_left} days left)"
    else:
        premium_status = "❌ <b>No Active Plan</b>"
    
    text = (
        f"<b>📱 Account Details</b>\n\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>\n\n"
        f"<b>📋 Profile Information:</b>\n"
        f"├ <b>👤 First Name:</b> <code>{first_name}</code>\n"
        f"├ <b>👥 Last Name:</b> <code>{last_name}</code>\n"
        f"├ <b>🆔 Username:</b> <code>{username}</code>\n"
        f"└ <b>📝 Bio:</b>\n"
        f"   <code>{bio}</code>\n\n"
        f"<b>📊 Account Statistics:</b>\n"
        f"├ <b>📞 Phone:</b> <code>{phone}</code>\n"
        f"├ <b>🔑 User ID:</b> <code>{account_user_id}</code>\n"
        f"├ <b>👥 Groups:</b> <code>{groups_count}</code>\n"
        f"└ <b>🔐 2FA Password:</b> <code>{two_fa}</code>\n\n"
        f"<b>➕ Added By:</b>\n"
        f"├ <b>🆔 Username:</b> <code>{owner_username}</code>\n"
        f"└ <b>🔑 User ID:</b> <code>{owner_id}</code>\n\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"
    )
    
    buttons = [
        [Button.inline("📱 Manage Devices", f"admdev_{acc_id}")],
        [Button.inline("📨 Get OTP", f"admotp_{acc_id}")],
        [Button.inline("← Back", b"admin_manage_accounts")]
    ]
    
    await event.edit(text, parse_mode='html', buttons=buttons)

# OTP forwarding is now handled by individual client handlers in admin_get_otp()

@main_bot.on(events.CallbackQuery(pattern=b"^admin_grant_premium$"))
async def admin_grant_premium_menu(event):
    uid = event.sender_id
    if not is_admin(uid):
        await event.answer("Admin only", alert=True)
        return
    
    help_text = (
        "<b>💎 Grant Premium Commands</b>\n\n"
        "<b>Usage:</b>\n"
        "<code>/kai userid|@username days</code> - Grant Kai plan (3 accounts)\n"
        "<code>/super userid|@username days</code> - Grant Super plan (5 accounts)\n"
        "<code>/ultra userid|@username days</code> - Grant Ultra plan (5 accounts)\n\n"
        "<b>Examples:</b>\n"
        "<code>/kai 123456789 30</code>\n"
        "<code>/super @username 60</code>\n"
        "<code>/ultra 555444333 90</code>\n\n"
        "<i>User will receive instant notification with plan activation.</i>"
    )
    await event.edit(help_text, parse_mode='html', buttons=[[Button.inline("← Back", b"admin_panel")]])


@main_bot.on(events.NewMessage(pattern=r'^/addacc\s+(@?[\w_]+)$'))
async def cmd_addacc(event):
    if not is_admin(event.sender_id):
        return

    target_raw = event.pattern_match.group(1)
    target_id = await resolve_target_user_id(target_raw, event.client)
    if not target_id:
        await event.respond("❌ User not found. Use user id or @username.")
        return

    user_states[event.sender_id] = {'action': 'phone', 'owner_id': target_id}
    await event.respond(
        f"Adding account for <code>{target_id}</code>.\n\nSend phone with country code:\n<code>+919876543210</code>",
        parse_mode='html'
    )


@main_bot.on(events.NewMessage(pattern=r'^/removeacc\s+(@?[\w_]+)\s+(\S+)$'))
async def cmd_removeacc(event):
    if not is_admin(event.sender_id):
        return

    target_raw = event.pattern_match.group(1)
    token = event.pattern_match.group(2)
    target_id = await resolve_target_user_id(target_raw, event.client)
    if not target_id:
        await event.respond("❌ User not found. Use user id or @username.")
        return

    acc = None
    if re.fullmatch(r'[0-9a-fA-F]{24}', token):
        from bson.objectid import ObjectId
        try:
            acc = accounts_col.find_one({'_id': ObjectId(token), 'owner_id': target_id})
        except Exception:
            acc = None

    if not acc:
        phone = token.strip()
        candidates = [phone]
        if phone.isdigit():
            candidates.append(f"+{phone}")
        acc = accounts_col.find_one({'owner_id': target_id, 'phone': {'$in': candidates}})

    if not acc:
        await event.respond("❌ Account not found for that user. Use account id or full phone.")
        return

    await delete_account_and_related(str(acc['_id']))
    await event.respond(
        f"✅ Removed account for <code>{target_id}</code>\n"
        f"Phone: <code>{acc.get('phone','')}</code>",
        parse_mode='html'
    )

# Admin commands for granting premium: /kai /super /ultra (aliases: /grow /prime /domi)
async def _grant_plan_from_command(event, plan_key: str, plan_label: str, source: str):
    if not is_admin(event.sender_id):
        return

    target_raw = event.pattern_match.group(1)
    days = int(event.pattern_match.group(2))
    target_id = await resolve_target_user_id(target_raw, event.client)
    if not target_id:
        await event.respond("❌ User not found. Use user id or @username.")
        return

    try:
        await grant_premium_to_user(target_id, plan_key, days, source=source)
        await event.respond(f"✅ {plan_label} plan granted to {target_id} for {days} days")
        print(f"[ADMIN CMD] {source}: User {target_id} granted {plan_label} for {days} days")
    except Exception as e:
        await event.respond(f"❌ Failed to grant {plan_label}: {str(e)[:120]}")
        print(f"[ADMIN CMD] {source} failed: {e}")


@main_bot.on(events.NewMessage(pattern=r'^/kai\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_kai(event):
    await _grant_plan_from_command(event, 'grow', 'Kai', '/kai')


@main_bot.on(events.NewMessage(pattern=r'^/super\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_super(event):
    await _grant_plan_from_command(event, 'prime', 'Super', '/super')


@main_bot.on(events.NewMessage(pattern=r'^/ultra\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_ultra(event):
    await _grant_plan_from_command(event, 'dominion', 'Ultra', '/ultra')


@main_bot.on(events.NewMessage(pattern=r'^/grow\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_grow(event):
    await _grant_plan_from_command(event, 'grow', 'Kai', '/grow')


@main_bot.on(events.NewMessage(pattern=r'^/prime\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_prime(event):
    await _grant_plan_from_command(event, 'prime', 'Super', '/prime')


@main_bot.on(events.NewMessage(pattern=r'^/domi\s+(@?[\w_]+)\s+(\d+)$'))
async def cmd_domi(event):
    await _grant_plan_from_command(event, 'dominion', 'Ultra', '/domi')
if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped")
    except Exception as e:
        print(f"[FATAL] Bot exited with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            mongo_client.close()
        except Exception:
            pass

