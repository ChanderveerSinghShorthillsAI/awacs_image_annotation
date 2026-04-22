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
        print(f"File not found: {filepath}")
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
            print(f"Warning: Could not parse {filepath} as JSONL or JSON")
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
    print(f"Sending {len(ad_ids)} ad IDs to {url}...")
    print(f"Environment: {CDC_ENV.upper()}")

    if IS_PROD:
        # ── Prod mode ──
        client_id = PROD_DB_API_CLIENT_ID
        client_secret = PROD_DB_API_CLIENT_SECRET
        grant_type = PROD_DB_API_GRANT_TYPE
        base_url = PROD_DB_API_BASE_URL
        token_url = PROD_DB_API_TOKEN_URL
        update_base_url = PROD_DB_API_UPDATE_BASE_URL

        print(f"Using PROD DB API: {base_url}")
        print(f"Using PROD Token URL: {token_url}")
        print(f"Using PROD Update URL: {update_base_url}")

        if not client_id or not client_secret:
            print("\nError: Prod DB API credentials not set in cdc_pipeline/.env")
            print("Set PROD_DB_API_CLIENT_ID and PROD_DB_API_CLIENT_SECRET")
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

        print(f"Using DEV DB API: {base_url}")

        if not client_id or not client_secret:
            print("\nError: Dev DB API credentials not set in cdc_pipeline/.env")
            print("Set CDC_DB_API_CLIENT_ID and CDC_DB_API_CLIENT_SECRET")
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
        print(f"\nError: Cannot connect to backend at {BACKEND_URL}")
        print("Make sure the backend is running: cd backend && python main.py")
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"\nError from backend: {e.response.status_code} - {e.response.text}")
        sys.exit(1)

    result = resp.json()
    job_id = result["job_id"]
    print(f"Pipeline triggered: job_id={job_id}, {result['total_ads']} ads")
    print(f"Output will be saved to: cdc_ai_output_excels/")
    return job_id


def poll_status(job_id: str):
    """Poll the backend for job status until completion or failure."""
    url = f"{BACKEND_URL}/api/cdc-trigger/{job_id}/status"
    print(f"\nPolling job status (Ctrl+C to stop polling — job continues in backend)...\n")

    while True:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "unknown")

            if status == "completed":
                print(f"\nJob {job_id} COMPLETED!")
                if data.get("output_file"):
                    print(f"Output: cdc_ai_output_excels/{data['output_file']}")
                # Show DB update result if present
                db_update = data.get("db_update_result")
                if db_update:
                    print(f"\nDB Update: {db_update.get('success_count', 0)} updated, "
                          f"{db_update.get('failed_count', 0)} failed, "
                          f"{db_update.get('skipped_count', 0)} skipped")
                    if db_update.get("report_filename"):
                        print(f"DB Update Report: cdc_ai_output_excels/{db_update['report_filename']}")
                    if db_update.get("patch_report_filename"):
                        print(f"Patch Summary: cdc_ai_output_excels/{db_update['patch_report_filename']}")
                return
            elif status == "failed":
                print(f"\nJob {job_id} FAILED: {data.get('error', 'unknown error')}")
                sys.exit(1)
            else:
                sys.stdout.write(f"\r  Status: {status} | Ads: {data.get('total_ads', '?')}  ")
                sys.stdout.flush()

        except requests.RequestException:
            sys.stdout.write(f"\r  Status: polling error (retrying)...  ")
            sys.stdout.flush()

        time.sleep(5)

def clear_processed_file(filepath: str):
    """Truncate the processed jsonl file so the next run starts fresh.
    No backup/archive is created to avoid disk bloat."""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            pass  # empty
        print(f"Cleared {os.path.basename(filepath)} for next run")
    except Exception as e:
        print(f"Warning: Could not clear {filepath}: {e}")


def main():
    print("=" * 60)
    print(f"CDC Pipeline -> AI Annotation [{CDC_ENV.upper()}]")
    print("=" * 60)

    # Step 1: Read unique ad IDs
    ad_ids = read_unique_ad_ids(OUTPUT_FILE)
    print(f"Found {len(ad_ids)} unique ad IDs from {OUTPUT_FILE}")

    if not ad_ids:
        print("No ads to process. Run the CDC consumer first.")
        return

    print(f"Ad IDs: {ad_ids[:10]}{'...' if len(ad_ids) > 10 else ''}")
    print("-" * 60)

    # Step 2: Trigger pipeline
    job_id = trigger_pipeline(ad_ids)

    # Step 3: Poll until done
    try:
        poll_status(job_id)
        # Pipeline completed successfully — archive and clear the jsonl file
        # so the next run doesn't reprocess these ads
        clear_processed_file(OUTPUT_FILE)
    except KeyboardInterrupt:
        print(f"\n\nStopped polling. Job {job_id} is still running in the backend.")
        print(f"Check status: curl {BACKEND_URL}/api/cdc-trigger/{job_id}/status")
        print(f"Note: {OUTPUT_FILE} was NOT cleared (job may still be running).")


if __name__ == "__main__":
    main()
