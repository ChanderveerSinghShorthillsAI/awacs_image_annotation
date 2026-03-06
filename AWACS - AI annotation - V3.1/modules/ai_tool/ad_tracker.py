"""
Ad Annotation Tracker — Turso HTTP API Integration

Tracks how many times each ad has been successfully annotated.
Ads exceeding the configurable max are filtered out before processing.

Uses Turso's HTTP pipeline API (https://docs.turso.tech/sdk/http/reference)
with the `requests` library — NO native SDK needed, NO Rust compilation.

PRIMARY KEY on ad_id provides automatic B-tree indexing for fast lookups.
"""

import requests
from datetime import datetime, timezone

# Singleton config
_api_url = None
_auth_token = None


def _execute_sql(statements: list) -> list:
    """
    Execute one or more SQL statements via Turso HTTP pipeline API.
    
    Each statement is a dict with 'sql' and optional 'args'.
    Returns list of result objects from Turso.
    
    Turso HTTP API: POST {db-url}/v2/pipeline
    """
    if not _api_url or not _auth_token:
        return []
    
    # Build pipeline request
    pipeline_requests = []
    for stmt in statements:
        req = {
            "type": "execute",
            "stmt": {"sql": stmt["sql"]}
        }
        if "args" in stmt:
            # Convert args to Turso typed format
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
    
    # Always close the stream at the end
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


def init_tracker(url: str, token: str):
    """
    Initialize the Turso HTTP API connection and create table if needed.
    Called once at server startup when EnableAdAnnotationLimit = True.
    
    Converts libsql:// URL to https:// for HTTP API access.
    """
    global _api_url, _auth_token
    
    try:
        # Convert libsql:// to https:// for HTTP API
        api_url = url.replace("libsql://", "https://").rstrip("/")
        _api_url = api_url
        _auth_token = token
        
        # Auto-create table with PRIMARY KEY index on ad_id
        results = _execute_sql([
            {
                "sql": """
                    CREATE TABLE IF NOT EXISTS ad_annotations (
                        ad_id TEXT PRIMARY KEY,
                        annotation_count INTEGER DEFAULT 0,
                        last_annotated_at TEXT
                    )
                """
            },
            {"sql": "SELECT COUNT(*) FROM ad_annotations"}
        ])
        
        # Parse count from second result
        total_tracked = 0
        if len(results) >= 2:
            result = results[1].get("response", {}).get("result", {})
            rows = result.get("rows", [])
            if rows:
                total_tracked = int(rows[0][0].get("value", 0))
        
        print(f"   📊 Ad Tracker: Turso DB connected successfully (HTTP API)")
        print(f"   📊 Ad Tracker: {total_tracked} ads currently tracked in database")
        
    except Exception as e:
        print(f"   ❌ Ad Tracker: Failed to connect to Turso DB: {e}")
        print(f"   ⚠️ Ad Tracker: Feature will be DISABLED for this session")
        _api_url = None
        _auth_token = None
        raise


def filter_over_limit_ads(ad_ids: list, max_count: int) -> set:
    """
    Check which ads have already been annotated >= max_count times.
    
    Returns a set of ad_id strings that should be SKIPPED.
    Processes in chunks of 500 to stay under SQLite's 999 parameter limit.
    The PRIMARY KEY index on ad_id ensures O(log n) lookups.
    """
    if not _api_url or not ad_ids:
        return set()
    
    CHUNK_SIZE = 500  # Stay well under SQLite's 999 param limit
    
    try:
        over_limit = set()
        
        for i in range(0, len(ad_ids), CHUNK_SIZE):
            chunk = ad_ids[i:i + CHUNK_SIZE]
            placeholders = ",".join(["?" for _ in chunk])
            query = f"SELECT ad_id, annotation_count FROM ad_annotations WHERE ad_id IN ({placeholders}) AND annotation_count >= ?"
            args = list(chunk) + [max_count]
            
            results = _execute_sql([{"sql": query, "args": args}])
            
            if results:
                result = results[0].get("response", {}).get("result", {})
                rows = result.get("rows", [])
                for row in rows:
                    ad_id = row[0].get("value", "")
                    over_limit.add(str(ad_id))
        
        return over_limit
        
    except Exception as e:
        print(f"   ⚠️ Ad Tracker: Error checking annotation counts: {e}")
        print(f"   ⚠️ Ad Tracker: Allowing all ads through (fail-open)")
        return set()


def increment_annotation_counts(ad_ids: list):
    """
    Increment annotation count for successfully processed ads.
    
    Processes in chunks of 500 to keep pipeline payloads manageable.
    New ads get count=1, existing ads get count+1.
    """
    if not _api_url or not ad_ids:
        return
    
    CHUNK_SIZE = 500  # Limit pipeline size per HTTP request
    
    try:
        now_utc = datetime.now(timezone.utc).isoformat()
        
        for i in range(0, len(ad_ids), CHUNK_SIZE):
            chunk = ad_ids[i:i + CHUNK_SIZE]
            statements = []
            for ad_id in chunk:
                statements.append({
                    "sql": """
                        INSERT INTO ad_annotations (ad_id, annotation_count, last_annotated_at)
                        VALUES (?, 1, ?)
                        ON CONFLICT(ad_id) DO UPDATE SET
                            annotation_count = annotation_count + 1,
                            last_annotated_at = ?
                    """,
                    "args": [str(ad_id), now_utc, now_utc]
                })
            _execute_sql(statements)
        
    except Exception as e:
        print(f"   ⚠️ Ad Tracker: Error updating annotation counts: {e}")
        print(f"   ⚠️ Ad Tracker: Counts may not be updated for this batch")


def close_tracker():
    """Clean shutdown — clear credentials."""
    global _api_url, _auth_token
    _api_url = None
    _auth_token = None
    print("   📊 Ad Tracker: Turso DB connection closed")
