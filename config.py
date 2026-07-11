import os

BOT_CONFIG = {
    'api_id': int(os.getenv('TELEGRAM_API_ID', '37840943')),
    'api_hash': os.getenv('TELEGRAM_API_HASH', '1092a61502cb8307ff429b0af7f404c3'),
    'bot_token': os.getenv('BOT_TOKEN', '8468153395:AAGIerNUX3cMAkSeADO9yiVNQ1MBrque3oc'),
    'owner_id': int(os.getenv('OWNER_ID', '1863750440')),
    'mongo_uri': os.getenv('MONGO_URI', 'mongodb+srv://Vansh:Kathait%40123@adbot.cv9soyo.mongodb.net/?appName=adbot'),
    'db_name': os.getenv('MONGO_DB_NAME', 'gokuads_db'),
    'logger_bot_token': os.getenv('LOGGER_BOT_TOKEN', '8544461885:AAF17504WmWwVdAJnEvpAA4ETiR9n6UG7Yo'),
    'logger_bot_username': os.getenv('LOGGER_BOT_USERNAME', 'jirenloggerbot'),
    # Admin Notifications Config
    'notification_bot_token': os.getenv('NOTIFICATION_BOT_TOKEN', '7923556924:AAFG5QISqEFDYa7HWu2zZgZblLGUJfTr1iU'),
    'notification_channel_id': int(os.getenv('NOTIFICATION_CHANNEL_ID', '-1003692710126')),

}

# ===================== PLAN TIERS =====================
# Starter (₹199), Pro (₹299), Elite (₹399)

PLAN_SCOUT = {
    'name': 'Legacy',
    'price': 0,
    'price_display': 'N/A',
    'tagline': 'Legacy plan (disabled)',
    'emoji': '🔰',
    'max_accounts': 0,
    'msg_delay': 60,
    'round_delay': 900,
    'auto_reply_enabled': False,
    'max_topics': 2,
    'max_groups_per_topic': 10,
    'max_auto_groups': 0,
    'max_groups_per_round': 0,   # legacy/disabled: uncapped (no accounts anyway)
    'logs_enabled': False,
    'description': 'Legacy plan (disabled)',
}

PLAN_GROW = {
    'name': 'Starter',
    'price': 199,
    'price_display': '₹199',
    'tagline': 'Scale your reach with multiple accounts',
    'emoji': '📈',
    'max_accounts': 2,
    'msg_delay': 30,
    'round_delay': 600,
    'auto_reply_enabled': True,
    'max_topics': 5,
    'max_groups_per_topic': 50,
    'max_auto_groups': 100,
    'max_groups_per_round': 100,   # rotation window per account per round (Starter)
    'logs_enabled': True,
    'description': '2 accounts, medium delays (30s/600s), auto-reply + logs + 🔄 Smart Rotation + 👥 Auto Group Join',
}

PLAN_PRIME = {
    'name': 'Pro',
    'price': 299,
    'price_display': '₹299',
    'tagline': 'Advanced automation for serious marketers',
    'emoji': '⭐',
    'max_accounts': 3,
    'msg_delay': 10,
    'round_delay': 120,
    'auto_reply_enabled': True,
    'max_topics': 9,
    'max_groups_per_topic': 100,
    'max_auto_groups': 150,
    'max_groups_per_round': 150,   # rotation window per account per round (Pro)
    'logs_enabled': True,
    'description': '3 accounts, fast delays (10s/120s), full features + 🔄 Smart Rotation + 👥 Auto Group Join',
}

PLAN_DOMINION = {
    'name': 'Elite',
    'price': 399,
    'price_display': '₹399',
    'tagline': 'Ultimate power for advertising domination',
    'emoji': '👑',
    'max_accounts': 4,
    'msg_delay': 10,
    'round_delay': 120,
    'auto_reply_enabled': True,
    'max_topics': 15,
    'max_groups_per_topic': 200,
    'max_auto_groups': 200,
    'max_groups_per_round': 200,   # rotation window per account per round (Elite)
    'logs_enabled': True,
    'description': '4 accounts, fastest delays (10s/120s), priority support + 🔄 Smart Rotation + 👥 Auto Group Join',
}

PLANS = {
    'scout': PLAN_SCOUT,
    'grow': PLAN_GROW,
    'prime': PLAN_PRIME,
    'dominion': PLAN_DOMINION,
}

# Backwards compat (old code references FREE_TIER/PREMIUM_TIER)
FREE_TIER = PLAN_SCOUT.copy()
PREMIUM_TIER = PLAN_DOMINION.copy()
ADMIN_USERNAME = "xxesr"

MESSAGES = {
    'welcome': "Welcome to Jiren Ads Bot\n\nAutomate Telegram promotions across groups and topics with clean controls and stable delivery.\n\nTap Launch Ads to get started.",
    'welcome_image': os.getenv('WELCOME_IMAGE', 'https://i.postimg.cc/5tLw5W0G/Whats-App-Image-2026-01-20-at-11-53-09.jpg'),

    # ===================== Account Profile Templates =====================
    # Applied to ALL added accounts when user opens dashboard (/start).
    # First name is preserved as-is.
    # Last name is forced to this tag (removes any existing last name).
    'account_last_name_tag': '',
    # Bio is forced to this text (removes any existing bio).
    'account_bio': '',
    'support_link': os.getenv('SUPPORT_LINK', 'https://t.me/jirenog'),
    'updates_link': os.getenv('UPDATES_LINK', 'https://t.me/jirenog'),
    'premium_contact': "Contact admin to purchase access.\n\nPaid Plan Benefits:\n- More accounts\n- Faster delays\n- Auto reply\n- Detailed logs\n- Priority support",
    
    # Privacy Policy
    'privacy_short': (
        "<b>Privacy & Terms</b>\n\n"
        "<blockquote>By using Jiren Ads Bot, you agree to responsible usage and Telegram ToS.\n"
        "We store only session data needed to run automation.\n"
        "No data is sold or shared.</blockquote>"
    ),
    'privacy_full_link': os.getenv('PRIVACY_URL', ''),
}

# ===================== Force Join (Config-based) =====================
# If enabled, users must join BOTH a channel and a group before using the bot.
# Use usernames (without @) so buttons can point to public links.
FORCE_JOIN = {
    'enabled': os.getenv('FORCE_JOIN_ENABLED', 'true').lower() in ('1', 'true', 'yes', 'on'),

    # Public @usernames (without @). Example: 'AdsReachUpdates'
    'channel_username': os.getenv('FORCE_JOIN_CHANNEL', 'jirenads'),
    # group_username removed (no forced group join)

    # Lock screen visuals
    'image_url': os.getenv('FORCE_JOIN_IMAGE', 'https://i.postimg.cc/5tLw5W0G/Whats-App-Image-2026-01-20-at-11-53-09.jpg'),
    'message': os.getenv(
        'FORCE_JOIN_MESSAGE',
        "**Access locked**\n\nJoin our **Channel** to continue.\nAfter joining, tap **Verify**."
    ),
}

# Plan-specific images (one for each plan)
PLAN_IMAGES = {
    'grow': os.getenv('GROW_PLAN_IMAGE', 'https://i.postimg.cc/5tLw5W0G/Whats-App-Image-2026-01-20-at-11-53-09.jpg'),
    'prime': os.getenv('PRIME_PLAN_IMAGE', 'https://i.postimg.cc/5tLw5W0G/Whats-App-Image-2026-01-20-at-11-53-09.jpg'),
    'dominion': os.getenv('DOMINION_PLAN_IMAGE', 'https://i.postimg.cc/5tLw5W0G/Whats-App-Image-2026-01-20-at-11-53-09.jpg'),
}

# ===================== Payment Config =====================
# Manual UPI payment (no crypto)
UPI_PAYMENT = {
    'qr_image_url': os.getenv('UPI_QR_IMAGE_URL', 'https://i.ibb.co/d43BBjrr/upi.jpg'),
    'upi_id': os.getenv('UPI_ID', 'vanshs22@fam'),
    'payee_name': os.getenv('UPI_PAYEE_NAME', 'Vansh Singh'),
}

INTERVAL_PRESETS = {
    'slow': {'msg_delay': 60, 'round_delay': 900, 'name': 'Slow (Safe)'},
    'medium': {'msg_delay': 30, 'round_delay': 600, 'name': 'Medium (Balanced)'},
    'fast': {'msg_delay': 10, 'round_delay': 300, 'name': 'Fast (Risky)'},
}

TOPICS = ['instagram', 'exchange', 'twitter', 'telegram', 'minecraft', 'tiktok', 'youtube', 'whatsapp', 'other']

PROXIES = []
