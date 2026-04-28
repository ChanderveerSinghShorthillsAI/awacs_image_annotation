import sys
import os

# Add project root to path so we can import from modules/ai_tool
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc")

from cdc_pipeline.consumer import run
from cdc_pipeline.config import CDC_CONSUMER_TIMEOUT_MINUTES

auto = "--auto" in sys.argv
debug = "--debug" in sys.argv
fresh = "--fresh" in sys.argv
daemon = "--daemon" in sys.argv

# Daemon mode is the long-running 24/7 deployment. It cannot coexist with
# --auto (auto runs annotation after a bounded consumer session) or --timeout
# (daemon mode never times out by design — rotation is signal-driven).
if daemon and (auto or "--timeout" in sys.argv):
    logger.error("--daemon is incompatible with --auto and --timeout")
    sys.exit(2)

# In auto mode, use env var timeout as default; --timeout N overrides it
timeout_minutes = None
if auto:
    timeout_minutes = CDC_CONSUMER_TIMEOUT_MINUTES
    if "--timeout" in sys.argv:
        try:
            idx = sys.argv.index("--timeout")
            timeout_minutes = int(sys.argv[idx + 1])
        except (IndexError, ValueError):
            logger.error("--timeout requires an integer argument (minutes)")
            sys.exit(1)

matched = run(debug=debug, fresh=fresh, timeout_minutes=timeout_minutes, daemon=daemon)

if auto:
    if matched and matched > 0:
        logger.info("=" * 60)
        logger.info("Auto mode: %d ads collected. Starting annotation pipeline...", matched)
        logger.info("=" * 60)
        from cdc_pipeline.run_annotation import main as run_annotation
        run_annotation()
    else:
        logger.info("=" * 60)
        logger.info("Auto mode: No ads collected. Skipping annotation.")
        logger.info("=" * 60)
