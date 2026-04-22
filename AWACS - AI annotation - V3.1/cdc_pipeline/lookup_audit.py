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


def main():
    load_config()

    if not config.enable_cdc_audit_log:
        print("Error: CDC audit logging is disabled in config.ini")
        print("Set EnableCDCAuditLog = True in [Grafana_Loki] section")
        sys.exit(1)

    # Parse args
    if len(sys.argv) > 1:
        ad_id = sys.argv[1]
    else:
        ad_id = input("Enter Ad ID: ").strip()

    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50

    if not ad_id:
        print("No ad ID provided.")
        sys.exit(1)

    # Initialize Loki connection (query-only, no flush thread needed)
    cdc_audit_logger.init_audit_logger(
        config.loki_push_url, config.loki_query_url,
        config.loki_user_id, config.loki_api_key,
    )

    print(f"\nQuerying Loki for ad ID: {ad_id} (limit={limit})...\n")
    results = cdc_audit_logger.query_ad_history(ad_id, limit=limit)

    if not results:
        print(f"No audit records found for ad ID: {ad_id}")
        print("\nPossible reasons:")
        print("  - The ad was never updated through the CDC pipeline")
        print("  - Audit records have expired (Loki retention is 14 days on free tier)")
        print("  - Loki credentials may be incorrect")
        sys.exit(1)

    print(f"Found {len(results)} category change(s) for ad ID: {ad_id}\n")

    for i, rec in enumerate(results, 1):
        old_cats = [c for c in rec.get("old_categories", []) if c]
        new_cats = [c for c in rec.get("new_categories", []) if c]

        print(f"{'=' * 70}")
        if len(results) > 1:
            print(f"  Change {i} of {len(results)}")
        print(f"{'=' * 70}")
        print(f"  Timestamp       : {rec.get('timestamp', 'N/A')}")
        print(f"  Environment     : {rec.get('environment', 'N/A')}")
        print(f"  Job ID          : {rec.get('job_id', 'N/A')}")
        print(f"  Old Categories  : {', '.join(old_cats) if old_cats else '(none)'}")
        print(f"  New Categories  : {', '.join(new_cats) if new_cats else '(none)'}")
        if rec.get("old_patch_categories"):
            print(f"  Old Patch Cats   : {rec['old_patch_categories']}")
        print(f"  Update Status   : {rec.get('update_status', 'N/A')}")
        if rec.get("error_message"):
            print(f"  Error           : {rec['error_message']}")
        if rec.get("patch_action"):
            print(f"  Patch Action    : {rec['patch_action']}")
        if rec.get("patch_deleted") == "true":
            print(f"  Patch Deleted   : Yes")
        print()

    cdc_audit_logger.close_audit_logger()


if __name__ == "__main__":
    main()
