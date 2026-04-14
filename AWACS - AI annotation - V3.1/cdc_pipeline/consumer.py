"""
Kafka consumer that filters truck ads from the CDC aggregate diff topic.

Captures:
  - New truck ads (all diff ops are "add")
  - Existing truck ads with photo updates (any diff op touches /photos)

Usage:
  cd "AWACS - AI annotation - V3.1"
  python -m cdc_pipeline                  # normal mode (resumes from last offset)
  python -m cdc_pipeline --fresh          # skip backlog, only new messages
  python -m cdc_pipeline --debug          # verbose: prints first non-truck message
  python -m cdc_pipeline --fresh --debug  # both
"""

import json
import signal
import sys
from datetime import datetime, timezone

from kafka import KafkaConsumer

from cdc_pipeline.config import (
    GROUP_ID,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY,
    MODE,
    OUTPUT_FILE,
    TOPICS,
)
from cdc_pipeline.filter import classify_message, is_truck_ad

def extract_summary(message: dict, filter_reason: str) -> dict:
    """Extract only the essential fields from a matched message."""
    categories = []
    for cat in (message.get("categories") or {}).values():
        name = None
        if isinstance(cat, dict):
            # Try nested category.name first, then top-level name
            cat_obj = cat.get("category")
            if isinstance(cat_obj, dict):
                name = cat_obj.get("name")
            if not name:
                name = cat.get("name")
        if name:
            categories.append(name)

    diff = message.get("diff", {})
    diff_timestamp = diff.get("timestamp") if diff else None

    return {
        "adId": message.get("adId"),
        "filter_reason": filter_reason,
        "timestamp": diff_timestamp,
        "photoCount": message.get("photoCount", 0),
        "realm": "TRUCK",
        "makeDisplayName": message.get("makeDisplayName"),
        "modelDisplayName": message.get("modelDisplayName"),
        "categories": categories,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def run(debug: bool = False, fresh: bool = False):
    print(f"Mode: {MODE}")
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    print(f"Topics: {TOPICS}")
    print(f"Consumer group: {GROUP_ID}")
    if fresh:
        print(">>> FRESH mode: skipping backlog, listening for new messages only")
    print(f"Output: {OUTPUT_FILE}")
    print("-" * 60)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        **KAFKA_SECURITY,
    )

    if fresh:
        # Force seek to end of all assigned partitions, ignoring committed offsets
        consumer.poll(timeout_ms=5000)  # triggers partition assignment
        consumer.seek_to_end()
        consumer.commit()  # persist so the skip sticks
        print(">>> Seeked to end of all partitions. Backlog skipped.")

    total = 0
    trucks = 0
    matched = 0
    skipped = 0
    running = True

    def shutdown(signum, frame):
        nonlocal running
        print(f"\n{'=' * 60}")
        print("Shutting down...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Listening for messages... (Ctrl+C to stop)\n")

    outfile = open(OUTPUT_FILE, "a", encoding="utf-8")

    try:
        while running:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for record in messages:
                    if not running:
                        break

                    total += 1
                    raw = record.value
                    if not raw:
                        continue
                    try:
                        message = json.loads(raw.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        skipped += 1
                        continue
                    if not isinstance(message, dict):
                        continue

                    # Check realm for truck count
                    truck = is_truck_ad(message)
                    if truck:
                        trucks += 1

                    if debug:
                        # Dump full message to file for inspection
                        debug_path = "cdc_pipeline/debug_messages.jsonl"
                        with open(debug_path, "a", encoding="utf-8") as df:
                            debug_envelope = {
                                "_topic": record.topic,
                                "_partition": record.partition,
                                "_offset": record.offset,
                                "message": message,
                            }
                            df.write(json.dumps(debug_envelope, indent=2, default=str) + "\n---\n")
                        keys = list(message.keys())
                        print(f"\n  [DEBUG] Topic={record.topic} | Ad {message.get('adId', '?')} | top-level keys={keys}")
                        print(f"  [DEBUG] realm={message.get('realm')} | adDetail={type(message.get('adDetail')).__name__} | photos={type(message.get('photos')).__name__}")
                    if not truck:
                        if debug:
                            print(f"  [DEBUG] ^ Not a truck. Dumped to cdc_pipeline/debug_messages.jsonl")

                    reason = classify_message(message)
                    if debug and truck and not reason:
                        diff_ops = message.get("diff", {}).get("operations", [])
                        paths = [op.get("path", "") for op in diff_ops]
                        print(f"\n  [DEBUG] Truck ad {message.get('adId')} not matched | diff paths={paths}")

                    if reason:
                        matched += 1
                        summary = extract_summary(message, reason)
                        outfile.write(json.dumps(summary) + "\n")
                        outfile.flush()

                        ad_id = summary["adId"]
                        make = summary["makeDisplayName"] or "?"
                        model = summary["modelDisplayName"] or "?"
                        cats = ", ".join(summary["categories"]) or "?"
                        print(
                            f"  [{reason:^12}] Ad {ad_id} | {make} {model} | {cats} | photos={summary['photoCount']}"
                        )

                    # Update status line
                    sys.stdout.write(
                        f"\r  Processed: {total} | Skipped: {skipped} | Trucks: {trucks} | Matched: {matched}"
                    )
                    sys.stdout.flush()

    finally:
        outfile.close()
        consumer.close()
        print(f"\n{'=' * 60}")
        print(f"Done. Processed {total} messages, {skipped} skipped, {trucks} trucks, {matched} matched.")
        print(f"Filtered ads saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
