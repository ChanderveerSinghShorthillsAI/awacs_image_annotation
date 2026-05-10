"""
CDC Category Mode Tracker — Turso HTTP API Integration

Tracks which categories are assigned to 'full_auto' or 'human_review' mode.
Used by the CDC pipeline to route ads: if any predicted category is in the
human_review list (or is unknown and DefaultCdcMode = human_review), the
entire ad is held for human review instead of being auto-updated in the DB.

Uses Turso's HTTP pipeline API (https://docs.turso.tech/sdk/http/reference)
with the `requests` library — same pattern as ad_tracker.py.
"""

import requests
from datetime import datetime, timezone

from .awacs_logger import setup_logger

logger = setup_logger("awacs.review_tracker")

# Singleton config — shared state, set once at init
_api_url = None
_auth_token = None


def _execute_sql(statements: list) -> list:
    """
    Execute one or more SQL statements via Turso HTTP pipeline API.

    Each statement is a dict with 'sql' and optional 'args'.
    Returns list of result objects from Turso.
    """
    if not _api_url or not _auth_token:
        return []

    pipeline_requests = []
    for stmt in statements:
        req = {
            "type": "execute",
            "stmt": {"sql": stmt["sql"]}
        }
        if "args" in stmt:
            typed_args = []
            for arg in stmt["args"]:
                if isinstance(arg, int):
                    typed_args.append({"type": "integer", "value": str(arg)})
                elif arg is None:
                    typed_args.append({"type": "null"})
                else:
                    typed_args.append({"type": "text", "value": str(arg)})
            req["stmt"]["args"] = typed_args
        pipeline_requests.append(req)

    pipeline_requests.append({"type": "close"})

    response = requests.post(
        f"{_api_url}/v2/pipeline",
        headers={
            "Authorization": f"Bearer {_auth_token}",
            "Content-Type": "application/json"
        },
        json={"requests": pipeline_requests},
        timeout=30
    )
    response.raise_for_status()

    data = response.json()
    return data.get("results", [])


def init_review_tracker(url: str, token: str):
    """
    Initialize the Turso HTTP API connection and create the cdc_mode_config
    table if it doesn't already exist.

    Called once at backend startup when EnableHumanInLoop = True.
    Converts libsql:// URL to https:// for HTTP API access.
    """
    global _api_url, _auth_token

    try:
        api_url = url.replace("libsql://", "https://").rstrip("/")
        _api_url = api_url
        _auth_token = token

        results = _execute_sql([
            {
                "sql": """
                    CREATE TABLE IF NOT EXISTS cdc_mode_config (
                        category_name TEXT PRIMARY KEY,
                        mode          TEXT NOT NULL,
                        updated_at    TEXT NOT NULL,
                        updated_by    TEXT NOT NULL DEFAULT 'system'
                    )
                """
            },
            {"sql": "SELECT COUNT(*) FROM cdc_mode_config"}
        ])

        total = 0
        if len(results) >= 2:
            result = results[1].get("response", {}).get("result", {})
            rows = result.get("rows", [])
            if rows:
                total = int(rows[0][0].get("value", 0))

        logger.info("   🔀 Review Tracker: Turso DB connected successfully (HTTP API)")
        logger.info("   🔀 Review Tracker: %d categories currently configured", total)

    except Exception as e:
        logger.error("   ❌ Review Tracker: Failed to connect to Turso DB: %s", e)
        logger.warning("   ⚠️ Review Tracker: Human-in-Loop routing will use DefaultCdcMode fallback only")
        _api_url = None
        _auth_token = None
        raise


def get_category_mode_map() -> dict:
    """
    Fetch all category-mode assignments from Turso.

    Returns {category_name (lowercase): 'full_auto' | 'human_review'}.
    Returns empty dict on error (caller falls back to DefaultCdcMode for all categories).
    """
    if not _api_url:
        return {}

    try:
        results = _execute_sql([
            {"sql": "SELECT category_name, mode FROM cdc_mode_config"}
        ])

        if not results:
            return {}

        result = results[0].get("response", {}).get("result", {})
        rows = result.get("rows", [])

        mode_map = {}
        for row in rows:
            cat = row[0].get("value", "")
            mode = row[1].get("value", "full_auto")
            if cat:
                mode_map[cat.strip().lower()] = mode

        return mode_map

    except Exception as e:
        logger.warning("   ⚠️ Review Tracker: Error fetching category modes: %s", e)
        logger.warning("   ⚠️ Review Tracker: Falling back to DefaultCdcMode for all categories")
        return {}


def get_all_category_modes() -> list:
    """
    Fetch all category-mode rows for the Category Manager UI.

    Returns list of dicts: [{category_name, mode, updated_at, updated_by}, ...]
    sorted by category_name.
    """
    if not _api_url:
        return []

    try:
        results = _execute_sql([
            {"sql": "SELECT category_name, mode, updated_at, updated_by FROM cdc_mode_config ORDER BY category_name"}
        ])

        if not results:
            return []

        result = results[0].get("response", {}).get("result", {})
        rows = result.get("rows", [])

        categories = []
        for row in rows:
            categories.append({
                "category_name": row[0].get("value", ""),
                "mode":          row[1].get("value", "full_auto"),
                "updated_at":    row[2].get("value", ""),
                "updated_by":    row[3].get("value", "system"),
            })

        return categories

    except Exception as e:
        logger.warning("   ⚠️ Review Tracker: Error listing category modes: %s", e)
        return []


def set_category_mode(name: str, mode: str, updated_by: str = "api"):
    """
    Insert or update a category's mode assignment.

    mode must be 'full_auto' or 'human_review'.
    """
    if not _api_url:
        logger.warning("   ⚠️ Review Tracker: Not initialized, cannot set category mode")
        return

    if mode not in ("full_auto", "human_review"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'full_auto' or 'human_review'.")

    now_utc = datetime.now(timezone.utc).isoformat()
    _execute_sql([
        {
            "sql": """
                INSERT INTO cdc_mode_config (category_name, mode, updated_at, updated_by)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(category_name) DO UPDATE SET
                    mode       = excluded.mode,
                    updated_at = excluded.updated_at,
                    updated_by = excluded.updated_by
            """,
            "args": [name.strip().lower(), mode, now_utc, updated_by]
        }
    ])


def delete_category_override(name: str):
    """
    Remove a category's explicit mode assignment.

    After deletion the category is no longer in either list, so it will
    be routed by DefaultCdcMode during the next CDC run.
    """
    if not _api_url:
        return

    _execute_sql([
        {
            "sql": "DELETE FROM cdc_mode_config WHERE category_name = ?",
            "args": [name.strip().lower()]
        }
    ])


def close_tracker():
    """Clean shutdown — clear credentials."""
    global _api_url, _auth_token
    _api_url = None
    _auth_token = None
    logger.info("   🔀 Review Tracker: Turso DB connection closed")
