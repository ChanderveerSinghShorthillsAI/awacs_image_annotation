"""End-of-day annotation trigger for the 24/7 daemon deployment.

Flow:
    1. Acquire single-flight lock (refuse to run if another EOD is in progress).
    2. Pre-flight: check disk free, sanity-cap on the active file size.
    3. Send SIGUSR1 to the running consumer (PID from /run/cdc-consumer.pid).
    4. Wait for the rotation marker file; read the rotated path from it.
    5. Sanity-check the rotated file size.
    6. Hand it to run_annotation.annotate_file().
    7. On success: delete the rotated file, release lock, exit 0.
    8. On any failure: release lock (so manual rerun isn't blocked), keep the
       rotated file on disk, exit non-zero so systemd marks the unit failed.

Invoked by the cdc-annotate.service systemd unit. Not intended for interactive
use, but `python -m cdc_pipeline.run_eod` works for local testing.
"""

import os
import shutil
import signal
import sys
import time

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_PATH = os.path.join(PROJECT_ROOT, "modules")
if MODULES_PATH not in sys.path:
    sys.path.insert(0, MODULES_PATH)

from ai_tool.awacs_logger import setup_logger

logger = setup_logger("awacs.cdc.eod")

from cdc_pipeline.config import (
    CDC_ANNOTATE_LOCKFILE,
    CDC_CONSUMER_PIDFILE,
    CDC_EOD_FILE_MAX_BYTES,
    CDC_EOD_MIN_DISK_FREE_PCT,
    CDC_ROTATION_MARKER,
    COLLECT_PHOTO_UPDATES_SNOWFLAKE,
    OUTPUT_FILE,
)
from cdc_pipeline.run_annotation import annotate_file


# Must be longer than the longest Kafka retry backoff (180s) so rotation
# succeeds even if the consumer is mid-backoff when SIGUSR1 arrives.
_ROTATION_WAIT_SECONDS = 300


def _acquire_lock() -> bool:
    """Atomically create the lock file. Returns True on acquire, False if held."""
    try:
        os.makedirs(os.path.dirname(CDC_ANNOTATE_LOCKFILE) or ".", exist_ok=True)
    except OSError:
        pass
    try:
        # O_EXCL guarantees we only succeed if the file did not exist.
        fd = os.open(CDC_ANNOTATE_LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def _release_lock() -> None:
    try:
        os.remove(CDC_ANNOTATE_LOCKFILE)
    except OSError:
        pass


def _disk_free_pct(path: str) -> int:
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return 100  # if we can't stat, don't block on it
    return int(usage.free * 100 / usage.total) if usage.total else 100


def _read_consumer_pid() -> int:
    with open(CDC_CONSUMER_PIDFILE, "r", encoding="utf-8") as f:
        return int(f.read().strip())


def _trigger_rotation_and_wait() -> str:
    """Send SIGUSR1 to the consumer, wait for the marker, return rotated path."""
    pid = _read_consumer_pid()
    logger.info("Sending SIGUSR1 to consumer (pid=%d)...", pid)

    # Clear any stale marker from a previous run before signaling, otherwise
    # we might read the previous rotation's path.
    try:
        os.remove(CDC_ROTATION_MARKER)
    except OSError:
        pass

    os.kill(pid, signal.SIGUSR1)

    deadline = time.monotonic() + _ROTATION_WAIT_SECONDS
    while time.monotonic() < deadline:
        if os.path.exists(CDC_ROTATION_MARKER):
            with open(CDC_ROTATION_MARKER, "r", encoding="utf-8") as f:
                rotated_path = f.read().strip()
            if rotated_path and os.path.exists(rotated_path):
                logger.info("Rotation confirmed: %s", rotated_path)
                return rotated_path
        time.sleep(0.5)

    raise RuntimeError(
        "Rotation marker {} did not appear within {}s — consumer may be stuck"
        .format(CDC_ROTATION_MARKER, _ROTATION_WAIT_SECONDS))


def main() -> int:
    if not _acquire_lock():
        logger.error("Lock %s already held — another EOD run is in progress. Aborting.",
                     CDC_ANNOTATE_LOCKFILE)
        return 1

    try:
        # Pre-flight: disk space check on whatever partition the active file lives on.
        active_dir = os.path.dirname(OUTPUT_FILE) or "."
        free_pct = _disk_free_pct(active_dir)
        if free_pct < CDC_EOD_MIN_DISK_FREE_PCT:
            logger.error("Disk free %d%% on %s is below threshold %d%% — aborting",
                         free_pct, active_dir, CDC_EOD_MIN_DISK_FREE_PCT)
            return 1
        logger.info("Disk free: %d%%", free_pct)

        # Trigger rotation. After this, OUTPUT_FILE is the *new* (empty) file
        # that the consumer keeps writing into; rotated_path is yesterday's data.
        rotated_path = _trigger_rotation_and_wait()

        # Ground-truth photo updates from Snowflake (temporary; replaces the
        # buggy Kafka photo_update signal). Runs sequentially BEFORE annotation
        # so the appended ads are guaranteed in the file, and is wrapped so any
        # failure/timeout is non-fatal — a slow warehouse must never stall the
        # 8-10h annotation window. collect_and_append never raises.
        if COLLECT_PHOTO_UPDATES_SNOWFLAKE:
            from cdc_pipeline.snowflake_photo_updates import collect_and_append
            n = collect_and_append(rotated_path)
            logger.info("Snowflake photo-update step appended %d ads to %s",
                        n, rotated_path)

        # Sanity cap on the rotated file. A file far above expected daily volume
        # signals an upstream bug; we'd rather fail loud than burn Gemini budget.
        try:
            size = os.path.getsize(rotated_path)
        except OSError as e:
            logger.error("Cannot stat rotated file %s: %s", rotated_path, e)
            return 1
        if size > CDC_EOD_FILE_MAX_BYTES:
            logger.error(
                "Rotated file %s is %d bytes (cap %d). Aborting and preserving file "
                "for manual investigation.", rotated_path, size, CDC_EOD_FILE_MAX_BYTES)
            return 1
        logger.info("Rotated file size: %d bytes", size)

        if size == 0:
            logger.info("Rotated file is empty — nothing to annotate. Cleaning up.")
            try:
                os.remove(rotated_path)
            except OSError:
                pass
            return 0

        # Hand off to the existing pipeline. annotate_file does not delete the
        # input itself when clear_on_success=False — we delete here on the
        # success path so the failure path can preserve the file for rerun.
        annotate_file(rotated_path, clear_on_success=False)

        try:
            os.remove(rotated_path)
            logger.info("Annotation succeeded — removed %s", rotated_path)
        except OSError as e:
            # Annotation succeeded but cleanup failed; not fatal, just noisy.
            # The weekly cleanup timer is the backstop.
            logger.warning("Could not remove rotated file %s: %s", rotated_path, e)

        return 0

    except Exception as e:
        logger.exception("EOD run failed: %s", e)
        return 1
    finally:
        _release_lock()


if __name__ == "__main__":
    sys.exit(main())
