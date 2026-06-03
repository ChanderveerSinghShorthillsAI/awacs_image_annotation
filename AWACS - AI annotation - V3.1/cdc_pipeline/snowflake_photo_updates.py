"""Snowflake-based photo-update detector (production module).

Ground-truth photo-update detection for the EOD pipeline. Queries
``<DB>.ENTERPRISE.MARKETPLACE_LISTING_PERFORMANCE_DAILY`` for whole-truck ads
(``REALM_NAME = 'Commercial Truck'``) whose ``LISTING_PHOTO_COUNT`` changed
between two consecutive daily snapshots, and appends the matching ad IDs as
``photo_update`` records to the rotated ``filtered_ads.jsonl`` so the existing
annotation pipeline picks them up alongside the Kafka ``new_ad``s.

This sidesteps the Nebulous ``mediaApiId``-regeneration bug that floods the
Kafka ``photo_update`` signal with false positives. It is a temporary,
flag-gated measure: enable with ``CDC_COLLECT_PHOTO_UPDATES_SNOWFLAKE=true``
(while ``CDC_COLLECT_PHOTO_UPDATES=false``); revert by flipping both back once
the upstream bug is fixed.

Auth (chosen by which env vars are set, no hostname/EC2 detection):
  * VMs  — service account, key-pair: ``*_SNOWFLAKE_PRIVATE_KEY_PATH`` (+ passphrase).
  * Local — personal account, password: ``*_SNOWFLAKE_PASSWORD``.

Date window is arrears-aware: the warehouse loads one day behind, so a run on
day D queries ``(D - CDC_SNOWFLAKE_ARREARS_DAYS - 1) -> (D - CDC_SNOWFLAKE_ARREARS_DAYS)``.

CLI (for local testing / smoke tests)::

    python -m cdc_pipeline.snowflake_photo_updates --dry-run
    python -m cdc_pipeline.snowflake_photo_updates --dry-run --yesterday 2026-05-17 --today 2026-05-18
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc.snowflake")

from cdc_pipeline.config import (
    SNOWFLAKE,
    SNOWFLAKE_ARREARS_DAYS,
    SNOWFLAKE_DATE_TZ,
    SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
)


# The user-verified query: self-join the daily snapshot to its prior day and
# keep only ads whose photo count changed. The INNER JOIN drops new-today ads
# (no prior-day row) automatically, so Kafka remains the sole source of new_ads.
SQL_QUERY = """
WITH daily AS (
  SELECT DATE_FORMATTED, LISTING_AD_ID, LISTING_PHOTO_COUNT
  FROM {database}.{schema}.MARKETPLACE_LISTING_PERFORMANCE_DAILY
  WHERE DATE_FORMATTED BETWEEN %(yesterday)s AND %(today)s
    AND REALM_NAME = 'Commercial Truck'
),
paired AS (
  SELECT d2.DATE_FORMATTED AS day, d2.LISTING_AD_ID,
         d1.LISTING_PHOTO_COUNT AS yday_count,
         d2.LISTING_PHOTO_COUNT AS today_count
  FROM daily d2
  INNER JOIN daily d1
    ON d1.LISTING_AD_ID = d2.LISTING_AD_ID
   AND d1.DATE_FORMATTED = DATEADD(day, -1, d2.DATE_FORMATTED)
)
SELECT day, LISTING_AD_ID, yday_count, today_count,
       (today_count - yday_count) AS photo_change,
       CASE WHEN today_count > yday_count THEN 'added'
            WHEN today_count < yday_count THEN 'removed' END AS change_type
FROM paired
WHERE day = %(today)s AND today_count != yday_count
ORDER BY change_type, ABS(today_count - yday_count) DESC
"""


def _arrears_window():
    """Return (yesterday, today) ISO date strings for the arrears-shifted window.

    The warehouse is SNOWFLAKE_ARREARS_DAYS behind, so the freshest snapshot at
    run time is run_date - ARREARS_DAYS. We compare that day to the one before it.
    """
    run_date = datetime.now(ZoneInfo(SNOWFLAKE_DATE_TZ)).date()
    today = run_date - timedelta(days=SNOWFLAKE_ARREARS_DAYS)
    yesterday = today - timedelta(days=1)
    return yesterday.isoformat(), today.isoformat()


def _load_private_key_der(path, passphrase):
    """Load an (encrypted) PEM private key from disk and return PKCS8 DER bytes.

    Mirrors the verified VM workflow: decrypt with the passphrase, re-serialize
    unencrypted to DER, and hand that to the connector via ``private_key=``.
    """
    from cryptography.hazmat.primitives import serialization

    with open(os.path.expanduser(path), "rb") as f:
        key = serialization.load_pem_private_key(
            f.read(),
            password=(passphrase.encode() if passphrase else None),
        )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _connect():
    """Open a Snowflake connection for the active environment (from config.SNOWFLAKE).

    Auth priority: private key file (VMs) > password (local). Account/user/role/
    warehouse/database/schema come from the env-resolved DEV_/PROD_ block.
    """
    try:
        import snowflake.connector
    except ImportError:
        raise RuntimeError(
            "snowflake-connector-python is not installed. "
            "Run: pip install -r cdc_pipeline/requirements.txt"
        )

    cfg = SNOWFLAKE
    missing = [
        name for name in ("account", "user", "role", "warehouse", "database", "schema")
        if not cfg.get(name)
    ]
    if missing:
        raise RuntimeError(
            "Missing required Snowflake settings: " + ", ".join(missing) +
            ". Set the DEV_SNOWFLAKE_* / PROD_SNOWFLAKE_* block in your env file."
        )

    kwargs = dict(
        account=cfg["account"],
        user=cfg["user"],
        role=cfg["role"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        login_timeout=20,
        # network_timeout bounds socket retries; STATEMENT_TIMEOUT_IN_SECONDS is
        # the server-side cap that actually aborts a long-running query. Both are
        # set so a slow/hung warehouse can never stall the EOD annotation window.
        network_timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
        session_parameters={"STATEMENT_TIMEOUT_IN_SECONDS": SNOWFLAKE_QUERY_TIMEOUT_SECONDS},
        client_session_keep_alive=False,
    )

    if cfg.get("private_key_path"):
        logger.info("Snowflake auth: key-pair (%s as %s)", cfg["account"], cfg["user"])
        kwargs["private_key"] = _load_private_key_der(
            cfg["private_key_path"], cfg.get("private_key_passphrase"))
    elif cfg.get("password"):
        logger.info("Snowflake auth: password (%s as %s)", cfg["account"], cfg["user"])
        kwargs["password"] = cfg["password"]
    else:
        raise RuntimeError(
            "No Snowflake auth configured. Set either *_SNOWFLAKE_PRIVATE_KEY_PATH "
            "(VM/service account) or *_SNOWFLAKE_PASSWORD (local/personal account)."
        )

    return snowflake.connector.connect(**kwargs)


def fetch_changed_photos(yesterday, today):
    """Run the photo-change query for the given date pair; return list of row dicts."""
    conn = _connect()
    try:
        sql = SQL_QUERY.format(
            database=SNOWFLAKE["database"], schema=SNOWFLAKE["schema"])
        cursor = conn.cursor()
        try:
            logger.info("Querying Snowflake for photo changes %s -> %s ...", yesterday, today)
            cursor.execute(sql, {"yesterday": yesterday, "today": today})
            cols = [c[0] for c in cursor.description]
            rows = [dict(zip(cols, r)) for r in cursor.fetchall()]
            logger.info("Snowflake returned %d changed-photo ads", len(rows))
            return rows
        finally:
            cursor.close()
    finally:
        conn.close()


def to_filtered_record(row):
    """Map a Snowflake row to the consumer's filtered_ads.jsonl schema.

    Minimal mapping: the annotation path only needs adId + filter_reason; the
    remaining consumer fields are filled with null/empty so the record shape
    matches what extract_summary() produces. `source`/`change_type` are extra
    audit fields the reader ignores.
    """
    day = row.get("DAY")
    # Snowflake NUMBER columns come back as decimal.Decimal, which json.dumps
    # can't serialize. Coerce to int so photoCount matches the Kafka path's type.
    # Guard against non-finite Decimals (NaN/Inf) which would raise on int().
    today_count = row.get("TODAY_COUNT")
    try:
        photo_count = int(today_count) if today_count is not None else None
    except (ValueError, OverflowError):
        photo_count = None
    return {
        "adId": str(row.get("LISTING_AD_ID")),
        "filter_reason": "photo_update",
        "timestamp": day.isoformat() if hasattr(day, "isoformat") else day,
        "photoCount": photo_count,
        "realm": "TRUCK",
        "classId": None,
        "makeDisplayName": None,
        "modelDisplayName": None,
        "categories": [],
        "received_at": datetime.now(timezone.utc).isoformat(),
        "source": "snowflake",
        "change_type": row.get("CHANGE_TYPE"),
    }


def append_to_jsonl(records, path):
    """Append records to a JSONL file, one JSON object per line. Returns count written."""
    n = 0
    with open(path, "a", encoding="utf-8") as f:
        for rec in records:
            # default=str is a safety net for any stray Decimal/date that slips
            # through (Snowflake NUMBER/DATE types are not natively JSON-serializable).
            f.write(json.dumps(rec, default=str) + "\n")
            n += 1
    return n


def collect_and_append(rotated_path):
    """Query Snowflake for the arrears window and append photo_update records.

    Never raises: any failure (driver missing, auth, timeout, table not loaded)
    is logged and swallowed, returning 0, so the EOD run is never blocked by
    Snowflake. Returns the number of records appended.
    """
    try:
        yesterday, today = _arrears_window()
        rows = fetch_changed_photos(yesterday, today)
        if not rows:
            logger.info("No Snowflake photo changes for %s -> %s", yesterday, today)
            return 0
        # Map per-row so one malformed row can't drop the whole night's batch.
        records = []
        skipped = 0
        for r in rows:
            try:
                records.append(to_filtered_record(r))
            except Exception as e:
                skipped += 1
                logger.warning("Skipping unmappable Snowflake row %s: %s",
                               r.get("LISTING_AD_ID"), e)
        if skipped:
            logger.warning("Skipped %d unmappable Snowflake rows", skipped)
        written = append_to_jsonl(records, rotated_path)
        logger.info("Appended %d Snowflake photo_update ads to %s", written, rotated_path)
        return written
    except Exception as e:
        logger.warning("Snowflake photo-update collection failed (non-fatal): %s", e)
        return 0


def main():
    p = argparse.ArgumentParser(
        description="Query Snowflake for truck ads whose photo count changed.")
    p.add_argument("--yesterday", default=None,
                   help="Earlier date (YYYY-MM-DD). Default: arrears-computed.")
    p.add_argument("--today", default=None,
                   help="Later date (YYYY-MM-DD). Default: arrears-computed.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print the records that would be appended (no file written).")
    p.add_argument("--out", default=None,
                   help="Append records to this JSONL file (like the EOD path).")
    args = p.parse_args()

    if args.yesterday and args.today:
        yesterday, today = args.yesterday, args.today
    else:
        yesterday, today = _arrears_window()
    logger.info("Date window: %s -> %s", yesterday, today)

    rows = fetch_changed_photos(yesterday, today)
    records = [to_filtered_record(r) for r in rows]

    by_type = {}
    for r in records:
        by_type[r["change_type"]] = by_type.get(r["change_type"], 0) + 1
    logger.info("Total changed-photo ads: %d  (%s)", len(records), by_type)

    if args.dry_run:
        for rec in records[:20]:
            print(json.dumps(rec, default=str))
        if len(records) > 20:
            print(f"... ({len(records) - 20} more)")
    if args.out:
        written = append_to_jsonl(records, args.out)
        logger.info("Appended %d records to %s", written, args.out)


if __name__ == "__main__":
    main()
