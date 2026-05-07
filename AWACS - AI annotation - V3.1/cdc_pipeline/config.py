import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from cdc_pipeline directory
_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path)

# ---------------------------------------------------------------------------
# Connection mode: "msk" (direct to MSK) or "local" (localhost:9093)
# Override with env var CDC_MODE=local to use local Kafka for testing.
# ---------------------------------------------------------------------------
MODE = os.environ.get("CDC_MODE", "msk")

# ---------------------------------------------------------------------------
# Environment: "dev" or "prod"
# Controls which Kafka brokers, DB Fetch API, and DB Update API are used.
# ---------------------------------------------------------------------------
CDC_ENV = os.environ.get("CDC_ENV", "dev").strip().lower()
IS_PROD = (CDC_ENV == "prod")

# --- Dev MSK (via SSM tunnel) ---
DEV_MSK_BROKERS = os.environ.get("DEV_MSK_BROKERS", "")
DEV_MSK_USERNAME = os.environ.get("DEV_MSK_USERNAME", "")
DEV_MSK_PASSWORD = os.environ.get("DEV_MSK_PASSWORD", "")

# --- Prod MSK ---
PROD_MSK_BROKERS = os.environ.get("PROD_MSK_BROKERS", "")
PROD_MSK_USERNAME = os.environ.get("PROD_MSK_USERNAME", "")
PROD_MSK_PASSWORD = os.environ.get("PROD_MSK_PASSWORD", "")

# --- Local Kafka ---
LOCAL_BOOTSTRAP = "localhost:9093"

# --- Derived Kafka settings (based on CDC_ENV + MODE) ---
if MODE == "msk":
    if IS_PROD:
        KAFKA_BOOTSTRAP_SERVERS = PROD_MSK_BROKERS
        KAFKA_SECURITY = {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_plain_username": PROD_MSK_USERNAME,
            "sasl_plain_password": PROD_MSK_PASSWORD,
        }
    else:
        KAFKA_BOOTSTRAP_SERVERS = DEV_MSK_BROKERS
        KAFKA_SECURITY = {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_plain_username": DEV_MSK_USERNAME,
            "sasl_plain_password": DEV_MSK_PASSWORD,
        }
else:
    KAFKA_BOOTSTRAP_SERVERS = LOCAL_BOOTSTRAP
    KAFKA_SECURITY = {}

TOPICS = [
    "traderinteractive.ads.aggregate.diff",
]

# Use separate consumer groups for dev and prod to avoid offset conflicts.
# CDC_GROUP_ID_OVERRIDE allows local testing with prod creds without
# affecting the prod VM's committed offsets (e.g. set to
# "awacs-truck-filter-prod-local" on your laptop).
_default_group = "awacs-truck-filter-prod" if IS_PROD else "awacs-truck-filter"
GROUP_ID = os.environ.get("CDC_GROUP_ID_OVERRIDE", _default_group)

OUTPUT_FILE = os.environ.get("CDC_OUTPUT_FILE", "cdc_pipeline/filtered_ads.jsonl")
RAW_MESSAGES_FILE = os.environ.get("CDC_RAW_MESSAGES_FILE", "cdc_pipeline/raw_messages.jsonl")
SAVE_RAW_MESSAGES = os.environ.get("CDC_SAVE_RAW_MESSAGES", "false").strip().lower() == "true"
SHOW_SUMMARY = os.environ.get("CDC_SHOW_SUMMARY", "false").strip().lower() == "true"
SUMMARY_FILE = os.environ.get("CDC_SUMMARY_FILE", "cdc_pipeline/session_summary.json")
TRUCK_REALM_ID = 4

# Toggle photo_update collection on/off via env var.
# Set CDC_COLLECT_PHOTO_UPDATES=false to collect only new_ad events.
# Default: true (collect both new_ad and photo_update)
COLLECT_PHOTO_UPDATES = os.environ.get("CDC_COLLECT_PHOTO_UPDATES", "true").strip().lower() == "true"

# --- Auto mode: consumer timeout (minutes) ---
CDC_CONSUMER_TIMEOUT_MINUTES = int(os.environ.get("CDC_CONSUMER_TIMEOUT_MINUTES", "5"))

# --- Daemon mode paths ---
# Where rotated files land. run_eod.py reads the most recent rotation marker.
CDC_ROTATED_DIR = os.environ.get("CDC_ROTATED_DIR", "cdc_pipeline/rotated")
CDC_ROTATION_MARKER = os.environ.get("CDC_ROTATION_MARKER", "/run/cdc-rotated")
CDC_CONSUMER_PIDFILE = os.environ.get("CDC_CONSUMER_PIDFILE", "/run/cdc-consumer.pid")
CDC_ANNOTATE_LOCKFILE = os.environ.get("CDC_ANNOTATE_LOCKFILE", "/run/cdc-annotate.lock")

# Soft cap on the active filtered_ads.jsonl in daemon mode (bytes).
# Default 200 MB — at ~1-2 KB/line that's ~100k-200k ads, well above expected
# 10k/day. Hitting this means upstream is misbehaving; consumer halts writes.
CDC_DAEMON_FILE_SOFT_CAP_BYTES = int(os.environ.get("CDC_DAEMON_FILE_SOFT_CAP_BYTES", str(200 * 1024 * 1024)))

# Sanity cap on the rotated file size before run_eod.py will hand it to the
# annotation backend. Default 50 MB ≈ 50k ads, ~5x expected daily volume.
CDC_EOD_FILE_MAX_BYTES = int(os.environ.get("CDC_EOD_FILE_MAX_BYTES", str(50 * 1024 * 1024)))

# Minimum free percentage on /var/lib/cdc partition for run_eod.py to proceed.
CDC_EOD_MIN_DISK_FREE_PCT = int(os.environ.get("CDC_EOD_MIN_DISK_FREE_PCT", "15"))

# --- Pipeline integration ---
BACKEND_URL = os.environ.get("CDC_BACKEND_URL", "http://localhost:8000")

# --- Dev DB API (for CDC pipeline — used when CDC_ENV=dev) ---
DB_API_BASE_URL = os.environ.get("CDC_DB_API_BASE_URL", "")
DB_API_CLIENT_ID = os.environ.get("CDC_DB_API_CLIENT_ID", "")
DB_API_CLIENT_SECRET = os.environ.get("CDC_DB_API_CLIENT_SECRET", "")
DB_API_GRANT_TYPE = os.environ.get("CDC_DB_API_GRANT_TYPE", "client_credentials")

# --- Prod DB API (for CDC pipeline — used when CDC_ENV=prod) ---
PROD_DB_API_TOKEN_URL = os.environ.get("PROD_DB_API_TOKEN_URL", "https://nebulous-prod.traderonline.com/vLatest/token")
PROD_DB_API_BASE_URL = os.environ.get("PROD_DB_API_BASE_URL", "https://nebulous-prod.traderonline.com/vLatest")
PROD_DB_API_UPDATE_BASE_URL = os.environ.get("PROD_DB_API_UPDATE_BASE_URL", "https://nebulous-prod.traderonline.com/vLatest/trucks")
PROD_DB_API_CLIENT_ID = os.environ.get("PROD_DB_API_CLIENT_ID", "")
PROD_DB_API_CLIENT_SECRET = os.environ.get("PROD_DB_API_CLIENT_SECRET", "")
PROD_DB_API_GRANT_TYPE = os.environ.get("PROD_DB_API_GRANT_TYPE", "client_credentials")
