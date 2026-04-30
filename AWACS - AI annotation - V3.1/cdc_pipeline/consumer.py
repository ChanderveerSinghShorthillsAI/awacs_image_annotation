"""
Kafka consumer that filters truck ads from the CDC aggregate diff topic.

Captures:
  - New truck ads (all diff ops are "add")
  - Existing truck ads with photo updates (any diff op touches /photos)

Usage:
  cd "AWACS - AI annotation - V3.1"
  python -m cdc_pipeline                  # normal mode (resumes from last offset)
  python -m cdc_pipeline --fresh          # skip backlog, only new messages
  python -m cdc_pipeline --debug          # verbose: logs first non-truck message
  python -m cdc_pipeline --fresh --debug  # both
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

# Add project root to path so we can import from modules/ai_tool
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc.consumer")

# How long (seconds) to remember an ad after first seeing it.
# Messages for the same ad arriving within this window inherit the
# original classification instead of being re-classified independently.
_AD_MEMORY_TTL = 900  # 15 minutes

from cdc_pipeline.config import (
    CDC_ANNOTATE_LOCKFILE,  # noqa: F401  (re-exported for run_eod.py convenience)
    CDC_CONSUMER_PIDFILE,
    CDC_DAEMON_FILE_SOFT_CAP_BYTES,
    CDC_ENV,
    CDC_ROTATED_DIR,
    CDC_ROTATION_MARKER,
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


def run(debug: bool = False, fresh: bool = False, timeout_minutes: int | None = None,
        daemon: bool = False) -> int:
    logger.info("Mode: %s", MODE)
    logger.info("Environment: %s %s", CDC_ENV.upper(), "⚠️  PRODUCTION" if IS_PROD else "(dev)")
    logger.info("Connecting to Kafka at %s...", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Topics: %s", TOPICS)
    logger.info("Consumer group: %s", GROUP_ID)
    if fresh:
        logger.info(">>> FRESH mode: skipping backlog, listening for new messages only")
    if daemon:
        logger.info(">>> DAEMON mode: 24/7 long-running. SIGUSR1 rotates output file.")
    logger.info("Output: %s", OUTPUT_FILE)
    if timeout_minutes is not None:
        logger.info("Auto mode: consumer will stop after %d minute(s)", timeout_minutes)
    logger.info("-" * 60)

    # In daemon mode, drop a PID file so run_eod can SIGUSR1 us.
    if daemon:
        try:
            os.makedirs(os.path.dirname(CDC_CONSUMER_PIDFILE), exist_ok=True)
            with open(CDC_CONSUMER_PIDFILE, "w", encoding="utf-8") as pf:
                pf.write(str(os.getpid()))
        except OSError as e:
            logger.error("Cannot write PID file %s: %s", CDC_CONSUMER_PIDFILE, e)
            # Non-fatal in dev (e.g. /run not writable on a laptop), but warn loudly.

    # All signal handlers registered before the Kafka retry loop so signals
    # arriving during backoff sleeps are handled correctly instead of using
    # Python's default handlers (which would crash the process on SIGUSR1).
    running = True
    rotate_pending = False

    def shutdown(signum, frame):
        nonlocal running
        logger.info("=" * 60)
        logger.info("Shutting down...")
        running = False

    def request_rotation(signum, frame):
        nonlocal rotate_pending
        logger.info("SIGUSR1 received — rotation requested")
        rotate_pending = True

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    if daemon:
        signal.signal(signal.SIGUSR1, request_rotation)

    # Bounded retry on Kafka connect: 5s/15s/60s/180s, then re-raise.
    # Daemon mode rides over short broker hiccups without a process restart;
    # one-shot modes still benefit from a single retry instead of crashing on
    # a transient SASL refresh.
    _kafka_backoffs = [5, 15, 60, 180]
    consumer = None
    for attempt, delay in enumerate([0] + _kafka_backoffs):
        if delay:
            logger.warning("Kafka connect retry in %ds (attempt %d/%d)...",
                           delay, attempt, len(_kafka_backoffs))
            # Sleep in 1-second increments so SIGTERM exits promptly
            for _ in range(delay):
                if not running:
                    logger.info("Shutdown requested during retry backoff — exiting.")
                    if daemon:
                        try:
                            os.remove(CDC_CONSUMER_PIDFILE)
                        except OSError:
                            pass
                    return 0
                time.sleep(1)
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                **KAFKA_SECURITY,
            )
            break
        except Exception as e:
            if attempt >= len(_kafka_backoffs):
                logger.error("Kafka connect failed after %d retries: %s",
                             len(_kafka_backoffs), e)
                raise
            logger.warning("Kafka connect failed: %s", e)

    if fresh:
        # Force seek to end of all assigned partitions, ignoring committed offsets
        consumer.poll(timeout_ms=5000)  # triggers partition assignment
        consumer.seek_to_end()
        consumer.commit()  # persist so the skip sticks
        logger.info(">>> Seeked to end of all partitions. Backlog skipped.")

    total = 0
    trucks = 0
    matched = 0
    skipped = 0
    suppressed = 0

    # Track recently seen ads: {ad_id: {"reason": str, "first_seen": float}}
    # If an ad was first classified as "new_ad", subsequent photo_update
    # messages are suppressed — they are just photos being uploaded to the
    # brand-new ad, not genuine photo updates to an existing listing.
    seen_ads: dict[str, dict] = {}

    # For the shutdown summary table (only populated when SHOW_SUMMARY=true).
    # {ad_id: {"reason": str, "make": str, "model": str, "classId": ...,
    #          "categories": str, "photoCount": int, "received_at": str}}
    summary_ads: dict[str, dict] = {}

    # Periodic status logging interval (every N messages)
    _STATUS_LOG_INTERVAL = 100

    start_time = time.monotonic()
    logger.info("Listening for messages... (Ctrl+C to stop)")

    # Daemon mode opens in append mode so a consumer restart doesn't wipe
    # un-rotated ads. One-shot modes truncate (existing behavior) so each
    # session starts fresh.
    _open_mode = "a" if daemon else "w"
    outfile = open(OUTPUT_FILE, _open_mode, encoding="utf-8")
    rawfile = open(RAW_MESSAGES_FILE, "a", encoding="utf-8") if SAVE_RAW_MESSAGES else None
    if SAVE_RAW_MESSAGES:
        logger.info("Raw message logging: ENABLED -> %s", RAW_MESSAGES_FILE)

    # write_disabled trips when the active file exceeds the soft cap.
    write_disabled = False

    poll_failures = 0
    _POLL_BACKOFFS = [5, 15, 60, 180]  # seconds; 4 retries then re-raise

    try:
        while running:
            # Check if timeout has been reached (auto mode only)
            if timeout_minutes is not None:
                elapsed = time.monotonic() - start_time
                if elapsed >= timeout_minutes * 60:
                    logger.info("=" * 60)
                    logger.info("Timeout reached (%d min). Stopping consumer...", timeout_minutes)
                    running = False
                    break

            # Daemon-mode rotation: rename current file, reopen fresh handle,
            # write marker file with the rotated path so run_eod.py can find it.
            if daemon and rotate_pending:
                rotate_pending = False
                try:
                    outfile.flush()
                    outfile.close()
                    os.makedirs(CDC_ROTATED_DIR, exist_ok=True)
                    rotated_name = "filtered_ads.{}.jsonl".format(
                        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ"))
                    rotated_path = os.path.join(CDC_ROTATED_DIR, rotated_name)
                    os.rename(OUTPUT_FILE, rotated_path)
                    outfile = open(OUTPUT_FILE, "a", encoding="utf-8")
                    write_disabled = False  # fresh file — clear any prior cap trip
                    try:
                        os.makedirs(os.path.dirname(CDC_ROTATION_MARKER) or ".",
                                    exist_ok=True)
                        with open(CDC_ROTATION_MARKER, "w", encoding="utf-8") as mf:
                            mf.write(rotated_path + "\n")
                    except OSError as e:
                        # Marker is best-effort; run_eod will time out and alert.
                        logger.error("Could not write rotation marker %s: %s",
                                     CDC_ROTATION_MARKER, e)
                    logger.info("Rotated -> %s", rotated_path)
                except OSError as e:
                    # Rotation failed (e.g. cross-device rename, permission).
                    # Keep running on the old file rather than crashing the daemon.
                    logger.error("Rotation failed: %s — continuing on existing file", e)
                    try:
                        outfile = open(OUTPUT_FILE, "a", encoding="utf-8")
                    except OSError as e2:
                        logger.error("Cannot reopen %s after failed rotation: %s",
                                     OUTPUT_FILE, e2)
                        running = False
                        break

            # Daemon-mode soft cap: stop writing if the active file exceeds the
            # configured byte cap. We keep polling so Kafka offsets advance.
            if daemon and not write_disabled:
                try:
                    if os.path.getsize(OUTPUT_FILE) > CDC_DAEMON_FILE_SOFT_CAP_BYTES:
                        logger.error(
                            "Soft cap %d bytes exceeded for %s — halting writes "
                            "until rotation. Investigate upstream.",
                            CDC_DAEMON_FILE_SOFT_CAP_BYTES, OUTPUT_FILE)
                        write_disabled = True
                except OSError:
                    pass

            try:
                records = consumer.poll(timeout_ms=1000)
                poll_failures = 0  # reset on any successful poll
            except Exception as e:
                if poll_failures >= len(_POLL_BACKOFFS):
                    logger.error("Kafka poll failed after %d retries: %s",
                                 len(_POLL_BACKOFFS), e)
                    raise
                delay = _POLL_BACKOFFS[poll_failures]
                poll_failures += 1
                logger.warning("Kafka poll error (%d/%d), backing off %ds: %s",
                               poll_failures, len(_POLL_BACKOFFS), delay, e)
                time.sleep(delay)
                continue
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
                        logger.debug("[DEBUG] Topic=%s | Ad %s | top-level keys=%s", record.topic, message.get('adId', '?'), keys)
                        logger.debug("[DEBUG] realm=%s | adDetail=%s | photos=%s", message.get('realm'), type(message.get('adDetail')).__name__, type(message.get('photos')).__name__)
                    if not truck:
                        if debug:
                            logger.debug("[DEBUG] ^ Not a truck. Dumped to cdc_pipeline/debug_messages.jsonl")

                    reason = classify_message(message)
                    if debug and truck and not reason:
                        diff_ops = message.get("diff", {}).get("operations", [])
                        paths = [op.get("path", "") for op in diff_ops]
                        cls = message.get("class")
                        cls_id = cls.get("id") if isinstance(cls, dict) else None
                        valid_cls = has_valid_class_id(message)
                        logger.debug("[DEBUG] Truck ad %s not matched | class=%s valid_class=%s | diff paths=%s", message.get('adId'), cls_id, valid_cls, paths)

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
                                    logger.debug("Suppressed photo_update for new ad %s (first seen as new_ad)", ad_id)

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

                                # Periodic status log
                                if total % _STATUS_LOG_INTERVAL == 0:
                                    logger.info("Processed: %d | Skipped: %d | Trucks: %d | Matched: %d | Suppressed: %d", total, skipped, trucks, matched, suppressed)
                                continue

                            # Same ad seen again with same or different reason
                            # (e.g. two photo_update messages for a genuinely
                            # existing ad) — let it through normally.
                        else:
                            # First time seeing this ad — record it.
                            seen_ads[ad_id] = {"reason": reason, "first_seen": now}

                        matched += 1
                        summary = extract_summary(message, reason)
                        if not write_disabled:
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
                        logger.info(
                            "[%s] Ad %s | %s %s | class=%s | %s | photos=%d",
                            reason, ad_id, make, model, cls_id, cats, summary["photoCount"],
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

                    # Periodic status log (instead of \r overwrite)
                    if total % _STATUS_LOG_INTERVAL == 0:
                        logger.info("Processed: %d | Skipped: %d | Trucks: %d | Matched: %d | Suppressed: %d", total, skipped, trucks, matched, suppressed)

    finally:
        try:
            outfile.close()
        except Exception:
            pass
        if rawfile:
            try:
                rawfile.close()
            except Exception:
                pass
        try:
            consumer.close()
        except Exception:
            pass
        if daemon:
            try:
                os.remove(CDC_CONSUMER_PIDFILE)
            except OSError:
                pass
        logger.info("=" * 60)
        logger.info("Done. Processed %d messages, %d skipped, %d trucks, %d matched, %d suppressed.", total, skipped, trucks, matched, suppressed)
        logger.info("Filtered ads saved to: %s", OUTPUT_FILE)
        if SAVE_RAW_MESSAGES:
            logger.info("Raw messages saved to: %s", RAW_MESSAGES_FILE)

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

            logger.info("Session summary saved to: %s", SUMMARY_FILE)

        return matched


if __name__ == "__main__":
    run()
