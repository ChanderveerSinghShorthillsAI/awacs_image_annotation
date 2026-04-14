"""
Trigger AI annotation for CDC-filtered truck ads.

Reads unique ad IDs from filtered_ads.jsonl and sends them to the
FastAPI backend which runs the full DB Fetch + AI Annotation pipeline.

Usage:
    cd "AWACS - AI annotation - V3.1"
    python -m cdc_pipeline.run_annotation

Prerequisites:
    - Backend must be running (cd backend && python main.py)
    - filtered_ads.jsonl must exist (run CDC consumer first)
"""

import json
import sys
import time

import requests

from cdc_pipeline.config import (
    BACKEND_URL,
    DB_API_BASE_URL,
    DB_API_CLIENT_ID,
    DB_API_CLIENT_SECRET,
    DB_API_GRANT_TYPE,
    OUTPUT_FILE,
)


def read_unique_ad_ids(filepath: str) -> list[str]:
    """Read filtered_ads.jsonl and return deduplicated ad IDs (preserving order)."""
    seen = set()
    ad_ids = []
    try:
        with open(filepath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    ad_id = str(record.get("adId", "")).strip()
                    if ad_id and ad_id not in seen:
                        seen.add(ad_id)
                        ad_ids.append(ad_id)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        sys.exit(1)
    return ad_ids


def trigger_pipeline(ad_ids: list[str]) -> str:
    """POST ad IDs + dev DB API credentials to the backend CDC trigger endpoint."""
    url = f"{BACKEND_URL}/api/cdc-trigger"
    print(f"Sending {len(ad_ids)} ad IDs to {url}...")
    print(f"Using dev DB API: {DB_API_BASE_URL}")

    if not DB_API_CLIENT_ID or not DB_API_CLIENT_SECRET:
        print("\nError: Dev DB API credentials not set in cdc_pipeline/.env")
        print("Set CDC_DB_API_CLIENT_ID and CDC_DB_API_CLIENT_SECRET")
        sys.exit(1)

    payload = {
        "ad_ids": ad_ids,
        "db_api_base_url": DB_API_BASE_URL,
        "client_id": DB_API_CLIENT_ID,
        "client_secret": DB_API_CLIENT_SECRET,
        "grant_type": DB_API_GRANT_TYPE,
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


def main():
    print("=" * 60)
    print("CDC Pipeline -> AI Annotation")
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
    except KeyboardInterrupt:
        print(f"\n\nStopped polling. Job {job_id} is still running in the backend.")
        print(f"Check status: curl {BACKEND_URL}/api/cdc-trigger/{job_id}/status")


if __name__ == "__main__":
    main()
