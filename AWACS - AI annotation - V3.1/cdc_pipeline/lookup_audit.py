"""
Look up CDC category change audit history for a given ad ID via Grafana Loki.

Usage:
  cd "AWACS - AI annotation - V3.1"
  python -m cdc_pipeline.lookup_audit                # interactive prompt
  python -m cdc_pipeline.lookup_audit 5039652358     # direct lookup
  python -m cdc_pipeline.lookup_audit 5039652358 20  # limit to 20 results
"""

import sys
import os

# Add modules to path so we can import config and audit logger
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.config_loader import config, load_config
from ai_tool import cdc_audit_logger
from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc.lookup_audit")


def main():
    load_config()

    if not config.enable_cdc_audit_log:
        logger.error("CDC audit logging is disabled in config.ini")
        logger.info("Set EnableCDCAuditLog = True in [Grafana_Loki] section")
        sys.exit(1)

    # Parse args
    if len(sys.argv) > 1:
        ad_id = sys.argv[1]
    else:
        ad_id = input("Enter Ad ID: ").strip()

    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    if not ad_id:
        logger.error("No ad ID provided.")
        sys.exit(1)

    # Initialize Loki connection (query-only, no flush thread needed)
    cdc_audit_logger.init_audit_logger(
        config.loki_push_url, config.loki_query_url,
        config.loki_user_id, config.loki_api_key,
    )

    logger.info("Querying Loki for ad ID: %s (limit=%d)...", ad_id, limit)
    results = cdc_audit_logger.query_ad_history(ad_id, limit=limit)

    if not results:
        logger.warning("No audit records found for ad ID: %s", ad_id)
        logger.info("Possible reasons:")
        logger.info("  - The ad was never updated through the CDC pipeline")
        logger.info("  - Audit records have expired (Loki retention is 14 days on free tier)")
        logger.info("  - Loki credentials may be incorrect")
        sys.exit(1)

    logger.info("Found %d category change(s) for ad ID: %s", len(results), ad_id)

    for i, rec in enumerate(results, 1):
        old_cats = [c for c in rec.get("old_categories", []) if c]
        new_cats = [c for c in rec.get("new_categories", []) if c]

        logger.info("=" * 70)
        if len(results) > 1:
            logger.info("  Change %d of %d", i, len(results))
        logger.info("=" * 70)
        logger.info("  Timestamp       : %s", rec.get('timestamp', 'N/A'))
        logger.info("  Environment     : %s", rec.get('environment', 'N/A'))
        logger.info("  Job ID          : %s", rec.get('job_id', 'N/A'))
        logger.info("  Old Categories  : %s", ', '.join(old_cats) if old_cats else '(none)')
        logger.info("  New Categories  : %s", ', '.join(new_cats) if new_cats else '(none)')
        if rec.get("old_patch_categories"):
            logger.info("  Old Patch Cats   : %s", rec['old_patch_categories'])
        logger.info("  Update Status   : %s", rec.get('update_status', 'N/A'))
        if rec.get("error_message"):
            logger.error("  Error           : %s", rec['error_message'])
        if rec.get("patch_action"):
            logger.info("  Patch Action    : %s", rec['patch_action'])
        if rec.get("patch_deleted") == "true":
            logger.info("  Patch Deleted   : Yes")
        logger.info("")

    cdc_audit_logger.close_audit_logger()


if __name__ == "__main__":
    main()
