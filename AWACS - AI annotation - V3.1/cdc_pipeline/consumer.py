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
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

# How long (seconds) to remember an ad after first seeing it.
# Messages for the same ad arriving within this window inherit the
# original classification instead of being re-classified independently.
_AD_MEMORY_TTL = 900  # 15 minutes

from cdc_pipeline.config import (
    CDC_ENV,
    GROUP_ID,
    IS_PROD,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY,
    MODE,
    OUTPUT_FILE,
    RAW_MESSAGES_FILE,
    SAVE_RAW_MESSAGES,
    SHOW_SUMMARY,
    SUMMARY_FILE,
    TOPICS,
)
from cdc_pipeline.filter import classify_message, has_valid_class_id, is_truck_ad

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

    # Extract class info
    cls = message.get("class")
    class_id = cls.get("id") if isinstance(cls, dict) else None

    return {
        "adId": message.get("adId"),
        "filter_reason": filter_reason,
        "timestamp": diff_timestamp,
        "photoCount": message.get("photoCount", 0),
        "realm": "TRUCK",
        "classId": class_id,
        "makeDisplayName": message.get("makeDisplayName"),
        "modelDisplayName": message.get("modelDisplayName"),
        "categories": categories,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def run(debug: bool = False, fresh: bool = False):
    print(f"Mode: {MODE}")
    print(f"Environment: {CDC_ENV.upper()} {'⚠️  PRODUCTION' if IS_PROD else '(dev)'}")
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
    suppressed = 0
    running = True

    # Track recently seen ads: {ad_id: {"reason": str, "first_seen": float}}
    # If an ad was first classified as "new_ad", subsequent photo_update
    # messages are suppressed — they are just photos being uploaded to the
    # brand-new ad, not genuine photo updates to an existing listing.
    seen_ads: dict[str, dict] = {}

    # For the shutdown summary table (only populated when SHOW_SUMMARY=true).
    # {ad_id: {"reason": str, "make": str, "model": str, "classId": ...,
    #          "categories": str, "photoCount": int, "received_at": str}}
    summary_ads: dict[str, dict] = {}

    def shutdown(signum, frame):
        nonlocal running
        print(f"\n{'=' * 60}")
        print("Shutting down...")
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Listening for messages... (Ctrl+C to stop)\n")

    outfile = open(OUTPUT_FILE, "a", encoding="utf-8")
    rawfile = open(RAW_MESSAGES_FILE, "a", encoding="utf-8") if SAVE_RAW_MESSAGES else None
    if SAVE_RAW_MESSAGES:
        print(f"Raw message logging: ENABLED -> {RAW_MESSAGES_FILE}")

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
                        cls = message.get("class")
                        cls_id = cls.get("id") if isinstance(cls, dict) else None
                        valid_cls = has_valid_class_id(message)
                        print(f"\n  [DEBUG] Truck ad {message.get('adId')} not matched | class={cls_id} valid_class={valid_cls} | diff paths={paths}")

                    if reason:
                        ad_id = str(message.get("adId", ""))
                        now = time.monotonic()

                        # Evict stale entries from seen_ads
                        stale = [k for k, v in seen_ads.items()
                                 if now - v["first_seen"] > _AD_MEMORY_TTL]
                        for k in stale:
                            del seen_ads[k]

                        # Determine effective reason using memory of prior
                        # messages for this ad.
                        prior = seen_ads.get(ad_id)
                        if prior is not None:
                            # We've seen this ad before within the TTL window.
                            if prior["reason"] == "new_ad" and reason == "photo_update":
                                # Photos being uploaded to a brand-new ad —
                                # suppress from filtered_ads.jsonl.
                                suppressed += 1
                                if debug:
                                    print(f"\n  [DEBUG] Suppressed photo_update for new ad {ad_id} (first seen as new_ad)")

                                # Still log to raw_messages for audit trail
                                if rawfile:
                                    raw_envelope = {
                                        "adId": ad_id,
                                        "filter_reason": "suppressed_new_ad_photo",
                                        "original_reason": reason,
                                        "received_at": datetime.now(timezone.utc).isoformat(),
                                        "_topic": record.topic,
                                        "_partition": record.partition,
                                        "_offset": record.offset,
                                        "message": message,
                                    }
                                    rawfile.write(json.dumps(raw_envelope, default=str) + "\n")
                                    rawfile.flush()

                                # Update status line and continue to next message
                                sys.stdout.write(
                                    f"\r  Processed: {total} | Skipped: {skipped} | Trucks: {trucks} | Matched: {matched} | Suppressed: {suppressed}"
                                )
                                sys.stdout.flush()
                                continue

                            # Same ad seen again with same or different reason
                            # (e.g. two photo_update messages for a genuinely
                            # existing ad) — let it through normally.
                        else:
                            # First time seeing this ad — record it.
                            seen_ads[ad_id] = {"reason": reason, "first_seen": now}

                        matched += 1
                        summary = extract_summary(message, reason)
                        outfile.write(json.dumps(summary) + "\n")
                        outfile.flush()

                        if rawfile:
                            raw_envelope = {
                                "adId": ad_id,
                                "filter_reason": reason,
                                "received_at": datetime.now(timezone.utc).isoformat(),
                                "_topic": record.topic,
                                "_partition": record.partition,
                                "_offset": record.offset,
                                "message": message,
                            }
                            rawfile.write(json.dumps(raw_envelope, default=str) + "\n")
                            rawfile.flush()

                        make = summary["makeDisplayName"] or "?"
                        model = summary["modelDisplayName"] or "?"
                        cats = ", ".join(summary["categories"]) or "?"
                        cls_id = summary.get("classId", "?")
                        print(
                            f"  [{reason:^12}] Ad {ad_id} | {make} {model} | class={cls_id} | {cats} | photos={summary['photoCount']}"
                        )

                        # Track for shutdown summary
                        if SHOW_SUMMARY:
                            if ad_id not in summary_ads:
                                summary_ads[ad_id] = {
                                    "reason": reason,
                                    "make": make,
                                    "model": model,
                                    "classId": cls_id,
                                    "categories": cats,
                                    "photoCount": summary["photoCount"],
                                    "received_at": summary["received_at"],
                                }
                            else:
                                # Update photoCount to latest value
                                summary_ads[ad_id]["photoCount"] = summary["photoCount"]

                    # Update status line
                    sys.stdout.write(
                        f"\r  Processed: {total} | Skipped: {skipped} | Trucks: {trucks} | Matched: {matched} | Suppressed: {suppressed}"
                    )
                    sys.stdout.flush()

    finally:
        outfile.close()
        if rawfile:
            rawfile.close()
        consumer.close()
        print(f"\n{'=' * 60}")
        print(f"Done. Processed {total} messages, {skipped} skipped, {trucks} trucks, {matched} matched, {suppressed} suppressed.")
        print(f"Filtered ads saved to: {OUTPUT_FILE}")
        if SAVE_RAW_MESSAGES:
            print(f"Raw messages saved to: {RAW_MESSAGES_FILE}")

        # ── Session summary JSON ──
        if SHOW_SUMMARY and summary_ads:
            new_ads = [
                {"adId": k, **v} for k, v in summary_ads.items() if v["reason"] == "new_ad"
            ]
            photo_updates = [
                {"adId": k, **v} for k, v in summary_ads.items() if v["reason"] == "photo_update"
            ]

            summary_json = {
                "session_ended_at": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    "total_messages_processed": total,
                    "total_skipped": skipped,
                    "total_trucks_seen": trucks,
                    "total_matched": matched,
                    "total_suppressed": suppressed,
                    "unique_ads_matched": len(summary_ads),
                    "newly_created_ads": len(new_ads),
                    "ads_with_photo_updates": len(photo_updates),
                },
                "newly_created_ads": new_ads,
                "ads_with_photo_updates": photo_updates,
            }

            with open(SUMMARY_FILE, "w", encoding="utf-8") as sf:
                json.dump(summary_json, sf, indent=2, default=str)

            print(f"Session summary saved to: {SUMMARY_FILE}")


if __name__ == "__main__":
    run()
