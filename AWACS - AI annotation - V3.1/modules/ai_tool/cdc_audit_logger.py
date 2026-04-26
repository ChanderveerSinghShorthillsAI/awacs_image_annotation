"""
CDC Audit Logger — Grafana Cloud Loki Integration

Logs old vs new category changes during CDC pipeline DB updates to Grafana Cloud Loki.
When a dealer asks "why were my categories changed?", query by Ad ID to see the full history.

Uses Loki's HTTP push API (POST /loki/api/v1/push) with structured metadata.
Batches records and flushes in a background thread for zero pipeline impact.

Fail-open: if Loki is unreachable, prints a warning and continues — never crashes the pipeline.
"""

import time
import threading
import queue
from datetime import datetime, timezone

import requests

from .awacs_logger import setup_logger

logger = setup_logger("awacs.cdc_audit_logger")


# ── Singleton state ──
_push_url = None
_query_url = None
_user_id = None
_api_key = None

_batch_queue = queue.Queue()
_flush_thread = None
_shutdown_event = threading.Event()

BATCH_SIZE = 50          # Records per HTTP POST
FLUSH_INTERVAL_SEC = 5   # Max seconds before flushing a partial batch
APP_LABEL = "awacs-cdc"  # Loki stream label


# ── Init / Shutdown ──

def init_audit_logger(push_url: str, query_url: str, user_id: str, api_key: str):
    """
    Initialize Loki connection and start the background flush thread.
    Called once at server startup when EnableCDCAuditLog = True.
    """
    global _push_url, _query_url, _user_id, _api_key, _flush_thread

    _push_url = push_url.rstrip("/")
    _query_url = query_url.rstrip("/")
    _user_id = user_id
    _api_key = api_key

    # Validate connectivity with a lightweight query
    try:
        resp = requests.get(
            _query_url,
            params={"query": f'{{app="{APP_LABEL}"}}', "limit": "1"},
            auth=(_user_id, _api_key),
            timeout=10,
        )
        if resp.status_code in (200, 204):
            logger.info("   [Audit] Grafana Loki connected successfully")
        else:
            logger.warning("   [Audit] Loki returned status %s — logging may not work", resp.status_code)
    except Exception as e:
        logger.warning("   [Audit] Warning: Could not reach Loki (%s) — will retry on first push", e)

    # Start background flush thread
    _shutdown_event.clear()
    _flush_thread = threading.Thread(target=_flush_loop, daemon=True, name="loki-audit-flush")
    _flush_thread.start()
    logger.info("   [Audit] Background flush thread started (batch=%d, interval=%ds)", BATCH_SIZE, FLUSH_INTERVAL_SEC)


def close_audit_logger():
    """Flush remaining records and stop the background thread."""
    global _push_url, _query_url, _flush_thread

    _shutdown_event.set()
    if _flush_thread and _flush_thread.is_alive():
        _flush_thread.join(timeout=15)

    # Final flush of anything left in the queue
    _flush_batch(_drain_queue())

    _push_url = None
    _query_url = None
    _flush_thread = None
    logger.info("   [Audit] Loki audit logger closed")


# ── Public API: Log a category change ──

def log_category_change(
    ad_id: str,
    job_id: str,
    environment: str,
    old_breadcrumbs: list,
    new_annotated: list,
    old_patch_categories: list,
    success: bool,
    error: str = "",
    patch_action: str = "",
    patch_deleted: bool = False,
):
    """
    Queue a category change audit record for Loki.

    Non-blocking: record goes to an in-memory queue, flushed by the background thread.
    """
    if not _push_url:
        return

    # Pad lists to 3 elements
    old = (old_breadcrumbs + ["", "", ""])[:3]
    new = (new_annotated + ["", "", ""])[:3]

    now_ns = str(int(time.time() * 1_000_000_000))

    log_line = (
        f"Category update: ad={ad_id} "
        f"old=[{', '.join(old)}] "
        f"new=[{', '.join(new)}] "
        f"status={'success' if success else 'failed'}"
    )

    metadata = {
        "ad_id": str(ad_id),
        "job_id": str(job_id),
        "old_top1": str(old[0]),
        "old_top2": str(old[1]),
        "old_top3": str(old[2]),
        "new_top1": str(new[0]),
        "new_top2": str(new[1]),
        "new_top3": str(new[2]),
        "old_patch_categories": ", ".join(str(c) for c in old_patch_categories) if old_patch_categories else "",
        "update_status": "success" if success else "failed",
        "error_message": str(error) if error else "",
        "patch_action": str(patch_action) if patch_action else "",
        "patch_deleted": "true" if patch_deleted else "false",
    }

    record = {
        "timestamp_ns": now_ns,
        "environment": environment,
        "log_line": log_line,
        "metadata": metadata,
    }

    _batch_queue.put(record)


def flush():
    """Manually flush all pending records. Call at the end of a pipeline run."""
    records = _drain_queue()
    if records:
        _flush_batch(records)


# ── Public API: Query audit history ──

def query_ad_history(ad_id: str, limit: int = 50) -> list:
    """
    Query Loki for all category change records for a given Ad ID.

    Returns a list of dicts with old/new categories, timestamps, and status.
    """
    if not _query_url or not _user_id:
        return []

    try:
        logql = f'{{app="{APP_LABEL}"}} | ad_id="{ad_id}"'
        resp = requests.get(
            _query_url,
            params={
                "query": logql,
                "limit": str(limit),
                "direction": "backward",
            },
            auth=(_user_id, _api_key),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        results = []
        for stream in data.get("data", {}).get("result", []):
            stream_labels = stream.get("stream", {})
            for value in stream.get("values", []):
                timestamp_ns = value[0]
                log_line = value[1] if len(value) > 1 else ""
                metadata = value[2] if len(value) > 2 else {}

                ts_sec = int(timestamp_ns) / 1_000_000_000
                dt = datetime.fromtimestamp(ts_sec, tz=timezone.utc)

                results.append({
                    "timestamp": dt.isoformat(),
                    "ad_id": metadata.get("ad_id", stream_labels.get("ad_id", "")),
                    "job_id": metadata.get("job_id", ""),
                    "environment": stream_labels.get("environment", ""),
                    "old_categories": [
                        metadata.get("old_top1", ""),
                        metadata.get("old_top2", ""),
                        metadata.get("old_top3", ""),
                    ],
                    "new_categories": [
                        metadata.get("new_top1", ""),
                        metadata.get("new_top2", ""),
                        metadata.get("new_top3", ""),
                    ],
                    "old_patch_categories": metadata.get("old_patch_categories", ""),
                    "update_status": metadata.get("update_status", ""),
                    "error_message": metadata.get("error_message", ""),
                    "patch_action": metadata.get("patch_action", ""),
                    "patch_deleted": metadata.get("patch_deleted", ""),
                    "log_line": log_line,
                })

        return results

    except Exception as e:
        logger.error("   [Audit] Error querying Loki for ad %s: %s", ad_id, e)
        return []


# ── Internal: Background flush loop ──

def _flush_loop():
    """Background thread: flushes the batch queue every FLUSH_INTERVAL_SEC or when batch is full."""
    while not _shutdown_event.is_set():
        records = []
        deadline = time.time() + FLUSH_INTERVAL_SEC

        while time.time() < deadline and len(records) < BATCH_SIZE:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                record = _batch_queue.get(timeout=min(remaining, 1.0))
                records.append(record)
            except queue.Empty:
                if _shutdown_event.is_set():
                    break
                continue

        if records:
            _flush_batch(records)

    # Drain anything left on shutdown
    leftover = _drain_queue()
    if leftover:
        _flush_batch(leftover)


def _drain_queue() -> list:
    """Pull all records currently in the queue."""
    records = []
    while True:
        try:
            records.append(_batch_queue.get_nowait())
        except queue.Empty:
            break
    return records


def _flush_batch(records: list):
    """Send a batch of records to Loki in a single HTTP POST."""
    if not records or not _push_url:
        return

    # Group by environment for separate Loki streams
    streams_by_env = {}
    for rec in records:
        env = rec["environment"]
        if env not in streams_by_env:
            streams_by_env[env] = []
        streams_by_env[env].append(rec)

    streams = []
    for env, env_records in streams_by_env.items():
        values = []
        for rec in env_records:
            values.append([
                rec["timestamp_ns"],
                rec["log_line"],
                rec["metadata"],
            ])
        streams.append({
            "stream": {
                "app": APP_LABEL,
                "environment": env,
            },
            "values": values,
        })

    payload = {"streams": streams}

    try:
        resp = requests.post(
            _push_url,
            auth=(_user_id, _api_key),
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code not in (200, 204):
            logger.warning("   [Audit] Loki push returned %s: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("   [Audit] Warning: Failed to push %d audit records to Loki: %s", len(records), e)
