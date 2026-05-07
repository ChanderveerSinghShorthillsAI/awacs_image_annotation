"""
Trigger AI annotation for CDC-filtered truck ads.

Reads unique ad IDs from filtered_ads.jsonl and sends them to the
FastAPI backend which runs the full DB Fetch + AI Annotation pipeline.

Supports both dev and prod environments via CDC_ENV flag in .env.

Usage:
    cd "AWACS - AI annotation - V3.1"
    python -m cdc_pipeline.run_annotation

Prerequisites:
    - Backend must be running (cd backend && python main.py)
    - filtered_ads.jsonl must exist (run CDC consumer first)
"""

import json
import os
import sys
import time

import requests

# Add project root to path so we can import from modules/ai_tool
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc.annotation")

from cdc_pipeline.config import (
    BACKEND_URL,
    CDC_ENV,
    IS_PROD,
    # Dev credentials
    DB_API_BASE_URL,
    DB_API_CLIENT_ID,
    DB_API_CLIENT_SECRET,
    DB_API_GRANT_TYPE,
    # Prod credentials
    PROD_DB_API_TOKEN_URL,
    PROD_DB_API_BASE_URL,
    PROD_DB_API_UPDATE_BASE_URL,
    PROD_DB_API_CLIENT_ID,
    PROD_DB_API_CLIENT_SECRET,
    PROD_DB_API_GRANT_TYPE,
    OUTPUT_FILE,
)


def read_unique_ad_ids(filepath: str) -> list[str]:
    """Read filtered_ads.jsonl and return deduplicated ad IDs (preserving order).

    Handles both strict JSONL (one JSON object per line) and pretty-printed
    JSON (multi-line objects, or a top-level JSON array).
    """
    seen = set()
    ad_ids = []
    try:
        with open(filepath, encoding="utf-8") as f:
            content = f.read().strip()
    except FileNotFoundError:
        logger.error("File not found: %s", filepath)
        sys.exit(1)

    if not content:
        return ad_ids

    # Try line-by-line JSONL first (most common / expected format)
    lines = content.splitlines()
    jsonl_ok = False
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            ad_id = str(record.get("adId", "")).strip()
            if ad_id and ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)
            jsonl_ok = True
        except json.JSONDecodeError:
            continue

    if jsonl_ok and ad_ids:
        return ad_ids

    # Fallback: try parsing as a single JSON array or comma-separated objects
    # Wrap with brackets if needed (handles pretty-printed objects separated by commas)
    ad_ids = []
    seen = set()
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # Try wrapping comma-separated objects in an array
        try:
            data = json.loads(f"[{content}]")
        except json.JSONDecodeError:
            logger.warning("Could not parse %s as JSONL or JSON", filepath)
            return ad_ids

    # data could be a single dict or a list
    if isinstance(data, dict):
        data = [data]

    for record in data:
        if isinstance(record, dict):
            ad_id = str(record.get("adId", "")).strip()
            if ad_id and ad_id not in seen:
                seen.add(ad_id)
                ad_ids.append(ad_id)

    return ad_ids


def trigger_pipeline(ad_ids: list[str]) -> str:
    """POST ad IDs + DB API credentials to the backend CDC trigger endpoint."""
    url = f"{BACKEND_URL}/api/cdc-trigger"
    logger.info("Sending %d ad IDs to %s...", len(ad_ids), url)
    logger.info("Environment: %s", CDC_ENV.upper())

    if IS_PROD:
        # ── Prod mode ──
        client_id = PROD_DB_API_CLIENT_ID
        client_secret = PROD_DB_API_CLIENT_SECRET
        grant_type = PROD_DB_API_GRANT_TYPE
        base_url = PROD_DB_API_BASE_URL
        token_url = PROD_DB_API_TOKEN_URL
        update_base_url = PROD_DB_API_UPDATE_BASE_URL

        logger.info("Using PROD DB API: %s", base_url)
        logger.info("Using PROD Token URL: %s", token_url)
        logger.info("Using PROD Update URL: %s", update_base_url)

        if not client_id or not client_secret:
            logger.error("Prod DB API credentials not set in cdc_pipeline/.env")
            logger.error("Set PROD_DB_API_CLIENT_ID and PROD_DB_API_CLIENT_SECRET")
            sys.exit(1)

        payload = {
            "ad_ids": ad_ids,
            "cdc_env": "prod",
            "db_api_base_url": base_url,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
            "token_url": token_url,
            "update_base_url": update_base_url,
        }
    else:
        # ── Dev mode (existing behavior) ──
        client_id = DB_API_CLIENT_ID
        client_secret = DB_API_CLIENT_SECRET
        grant_type = DB_API_GRANT_TYPE
        base_url = DB_API_BASE_URL

        logger.info("Using DEV DB API: %s", base_url)

        if not client_id or not client_secret:
            logger.error("Dev DB API credentials not set in cdc_pipeline/.env")
            logger.error("Set CDC_DB_API_CLIENT_ID and CDC_DB_API_CLIENT_SECRET")
            sys.exit(1)

        payload = {
            "ad_ids": ad_ids,
            "cdc_env": "dev",
            "db_api_base_url": base_url,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type,
        }

    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
    except requests.ConnectionError:
        logger.error("Cannot connect to backend at %s", BACKEND_URL)
        logger.error("Make sure the backend is running: cd backend && python main.py")
        sys.exit(1)
    except requests.HTTPError as e:
        logger.error("Error from backend: %d - %s", e.response.status_code, e.response.text)
        sys.exit(1)

    result = resp.json()
    job_id = result["job_id"]
    logger.info("Pipeline triggered: job_id=%s, %d ads", job_id, result["total_ads"])
    logger.info("Output will be saved to: cdc_ai_output_excels/")
    return job_id


def poll_status(job_id: str):
    """Poll the backend for job status until completion or failure."""
    url = f"{BACKEND_URL}/api/cdc-trigger/{job_id}/status"
    logger.info("Polling job status (Ctrl+C to stop polling — job continues in backend)...")

    while True:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "unknown")

            if status == "completed":
                logger.info("Job %s COMPLETED!", job_id)
                if data.get("output_file"):
                    logger.info("Output: cdc_ai_output_excels/%s", data["output_file"])
                # Show DB update result if present
                db_update = data.get("db_update_result")
                if db_update:
                    logger.info(
                        "DB Update: %d updated, %d failed, %d skipped",
                        db_update.get("success_count", 0),
                        db_update.get("failed_count", 0),
                        db_update.get("skipped_count", 0),
                    )
                    if db_update.get("report_filename"):
                        logger.info("DB Update Report: cdc_ai_output_excels/%s", db_update["report_filename"])
                    if db_update.get("patch_report_filename"):
                        logger.info("Patch Summary: cdc_ai_output_excels/%s", db_update["patch_report_filename"])
                return
            elif status == "failed":
                logger.error("Job %s FAILED: %s", job_id, data.get("error", "unknown error"))
                raise RuntimeError(f"Job {job_id} failed: {data.get('error', 'unknown error')}")
            else:
                logger.info("Status: %s | Ads: %s", status, data.get("total_ads", "?"))

        except requests.RequestException:
            logger.warning("Polling error (retrying)...")

        time.sleep(5)

def clear_processed_file(filepath: str):
    """Truncate the processed jsonl file so the next run starts fresh.
    No backup/archive is created to avoid disk bloat."""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            pass  # empty
        logger.info("Cleared %s for next run", os.path.basename(filepath))
    except Exception as e:
        logger.warning("Could not clear %s: %s", filepath, e)


def annotate_file(input_path: str, clear_on_success: bool = True) -> bool:
    """Run the full annotate flow against a specific JSONL file.

    Returns True on completion, raises on failure (so callers like run_eod can
    decide whether to delete the rotated file). Does not call sys.exit on
    backend errors — the underlying helpers still do, which is fine for the
    CLI path; run_eod wraps this in a try/except.
    """
    logger.info("=" * 60)
    logger.info("CDC Pipeline -> AI Annotation [%s]", CDC_ENV.upper())
    logger.info("Input: %s", input_path)
    logger.info("=" * 60)

    ad_ids = read_unique_ad_ids(input_path)
    logger.info("Found %d unique ad IDs from %s", len(ad_ids), input_path)

    if not ad_ids:
        logger.info("No ads to process.")
        return True

    logger.info("Ad IDs: %s%s", ad_ids[:10], "..." if len(ad_ids) > 10 else "")
    logger.info("-" * 60)

    job_id = trigger_pipeline(ad_ids)

    try:
        poll_status(job_id)
    except KeyboardInterrupt:
        logger.info("Stopped polling. Job %s is still running in the backend.", job_id)
        logger.info("Check status: curl %s/api/cdc-trigger/%s/status", BACKEND_URL, job_id)
        logger.info("Note: %s was NOT cleared (job may still be running).", input_path)
        raise

    if clear_on_success:
        clear_processed_file(input_path)
    return True


def main():
    """CLI entrypoint — operates on the default OUTPUT_FILE.

    Daemon-mode rotated files are processed by cdc_pipeline.run_eod, which
    calls annotate_file() directly with the rotated path.
    """
    annotate_file(OUTPUT_FILE, clear_on_success=True)


if __name__ == "__main__":
    main()
