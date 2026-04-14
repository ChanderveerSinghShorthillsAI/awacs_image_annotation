import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from cdc_pipeline directory
_env_path = Path(__file__).parent / ".env"
load_dotenv(_env_path)

# ---------------------------------------------------------------------------
# Connection mode: "msk" (direct to dev MSK) or "local" (localhost:9093)
# Override with env var CDC_MODE=local to use local Kafka for testing.
# ---------------------------------------------------------------------------
MODE = os.environ.get("CDC_MODE", "msk")

# --- Dev MSK (via SSM tunnel) ---
# SSM tunnels map remote brokers to local loopback addresses:
#   broker-1 → 127.0.0.1:9096
#   broker-2 → 127.0.0.2:9096  (socat from localhost:9097)
#   broker-3 → 127.0.0.3:9096  (socat from localhost:9098)
DEV_MSK_BROKERS = os.environ.get("DEV_MSK_BROKERS", "")
MSK_USERNAME = os.environ.get("MSK_USERNAME", "")
MSK_PASSWORD = os.environ.get("MSK_PASSWORD", "")

# --- Local Kafka ---
LOCAL_BOOTSTRAP = "localhost:9093"

# --- Derived settings ---
if MODE == "msk":
    KAFKA_BOOTSTRAP_SERVERS = DEV_MSK_BROKERS
    KAFKA_SECURITY = {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_plain_username": MSK_USERNAME,
        "sasl_plain_password": MSK_PASSWORD,
    }
else:
    KAFKA_BOOTSTRAP_SERVERS = LOCAL_BOOTSTRAP
    KAFKA_SECURITY = {}

TOPICS = [
    "traderinteractive.ads.aggregate.diff",
]
GROUP_ID = "awacs-truck-filter"
OUTPUT_FILE = "cdc_pipeline/filtered_ads.jsonl"
TRUCK_REALM_ID = 4

# --- Pipeline integration ---
BACKEND_URL = os.environ.get("CDC_BACKEND_URL", "http://localhost:8000")

# --- Dev DB API (for CDC feature — separate from prod config.ini) ---
DB_API_BASE_URL = os.environ.get("CDC_DB_API_BASE_URL", "")
DB_API_CLIENT_ID = os.environ.get("CDC_DB_API_CLIENT_ID", "")
DB_API_CLIENT_SECRET = os.environ.get("CDC_DB_API_CLIENT_SECRET", "")
DB_API_GRANT_TYPE = os.environ.get("CDC_DB_API_GRANT_TYPE", "client_credentials")
