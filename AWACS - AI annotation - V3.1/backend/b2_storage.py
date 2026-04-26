"""
Backblaze B2 cloud storage integration for AWACS.
Uses boto3 S3-compatible API to upload output files, generate pre-signed
download URLs, manage lifecycle rules, and list/delete objects.

All public functions are safe to call even when B2 is disabled — they
become no-ops and return sensible defaults.
"""

import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("awacs.b2")

# boto3 is imported lazily in init_b2() so the module can be imported
# even if boto3 is not installed (B2 disabled).
_s3_client = None
_upload_executor: Optional[ThreadPoolExecutor] = None
_b2_enabled = False
_bucket_name = ""
_presigned_url_expiry = 3600

# ── Thread-safe tracking of uploaded B2 keys ──
# Maps local_path -> b2_key so download endpoints can look up the key
# even if the caller didn't store it in a job dict.
_b2_keys: dict[str, str] = {}
_b2_keys_lock = threading.Lock()

# Folder mapping: file_type -> B2 prefix under the root
_FOLDER_MAP = {
    "annotated":          "awacs-outputs/annotated",
    "reannotated":        "awacs-outputs/reannotated",
    "db-annotated":       "awacs-outputs/db-annotated",
    "batches":            "awacs-outputs/batches",
    "db-fetch":           "awacs-outputs/db-fetch",
    "uploads":            "awacs-outputs/uploads",
    "audit-reports":      "awacs-outputs/audit-reports",
    "patch-summaries":    "awacs-outputs/patch-summaries",
    "cdc/fetch":          "awacs-outputs/cdc/fetch",
    "cdc/annotated":      "awacs-outputs/cdc/annotated",
    "cdc/patch-summaries": "awacs-outputs/cdc/patch-summaries",
}


# ═══════════════════════════════════════════════════════
#  Initialisation / Shutdown
# ═══════════════════════════════════════════════════════

def init_b2(config) -> bool:
    """
    Initialise the B2 S3 client from config values.
    Returns True if B2 is enabled and ready, False otherwise.
    """
    global _s3_client, _upload_executor, _b2_enabled, _bucket_name, _presigned_url_expiry

    if not getattr(config, "b2_enabled", False):
        _b2_enabled = False
        return False

    key_id = getattr(config, "b2_key_id", "")
    app_key = getattr(config, "b2_application_key", "")
    endpoint_url = getattr(config, "b2_endpoint_url", "")
    region = getattr(config, "b2_region", "us-west-004")

    if not key_id or not app_key or not endpoint_url:
        logger.warning("[B2] ⚠️  Enabled but missing credentials — falling back to local storage")
        _b2_enabled = False
        return False

    try:
        import boto3
        from botocore.config import Config as BotoConfig

        _s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=key_id,
            aws_secret_access_key=app_key,
            region_name=region,
            config=BotoConfig(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
                request_checksum_calculation="when_required",
            ),
        )
        _bucket_name = getattr(config, "b2_bucket_name", "awacs-outputs")
        _presigned_url_expiry = getattr(config, "b2_presigned_url_expiry", 3600)
        _upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="b2-upload")
        _b2_enabled = True

        # Quick connectivity check — list 0 objects
        _s3_client.list_objects_v2(Bucket=_bucket_name, MaxKeys=1)
        logger.info("[B2] ✅ Connected to bucket '%s' at %s", _bucket_name, endpoint_url)
        return True

    except Exception as e:
        logger.error("[B2] ❌ Failed to initialise: %s", e)
        _b2_enabled = False
        _s3_client = None
        return False


def shutdown_b2():
    """Gracefully shut down the upload thread pool."""
    global _upload_executor
    if _upload_executor:
        _upload_executor.shutdown(wait=True, cancel_futures=False)
        _upload_executor = None
        logger.info("[B2] Upload executor shut down")


def flush_uploads():
    """
    Wait for all pending async uploads to complete.
    Call this before deleting local files that may still be uploading.
    """
    global _upload_executor
    if not _upload_executor:
        return
    # Shut down current executor (waits for pending tasks) and create a new one
    _upload_executor.shutdown(wait=True, cancel_futures=False)
    _upload_executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="b2-upload")
    logger.info("   [B2] All pending uploads flushed")


def is_enabled() -> bool:
    """Check if B2 is initialised and enabled."""
    return _b2_enabled and _s3_client is not None


# ═══════════════════════════════════════════════════════
#  Key / Path Helpers
# ═══════════════════════════════════════════════════════

def b2_key_for_file(file_type: str, filename: str) -> str:
    """
    Build the full B2 object key for a file.

    Uses date-based subfolders for easy navigation:
        awacs-outputs/<type>/YYYY-MM-DD/<filename>

    file_type must be one of the keys in _FOLDER_MAP.
    """
    prefix = _FOLDER_MAP.get(file_type)
    if not prefix:
        raise ValueError(f"Unknown file_type '{file_type}'. Valid: {list(_FOLDER_MAP.keys())}")
    date_folder = datetime.now().strftime("%Y-%m-%d")
    return f"{prefix}/{date_folder}/{filename}"


def _classify_cdc_filename(filename: str) -> Optional[str]:
    """
    Given a CDC output filename, return its B2 folder prefix.
    Returns None if the filename doesn't match any known CDC pattern.
    """
    if filename.startswith("CDC_Fetch_"):
        return "awacs-outputs/cdc/fetch"
    elif filename.startswith("CDC_Patch_Summary_"):
        return "awacs-outputs/cdc/patch-summaries"
    elif "annotated" in filename.lower() or filename.startswith("batch_"):
        return "awacs-outputs/cdc/annotated"
    return None


# ═══════════════════════════════════════════════════════
#  Upload
# ═══════════════════════════════════════════════════════

def upload_to_b2(local_path: str, b2_key: str, delete_local: bool = True) -> bool:
    """
    Upload a local file to B2 (synchronous).

    Returns True on success. On failure, logs a warning and returns False.
    If delete_local is True, the local file is removed after a successful upload.
    The pipeline is NEVER interrupted by upload failures.
    """
    if not is_enabled():
        return False

    try:
        from boto3.s3.transfer import TransferConfig
        _s3_client.upload_file(
            Filename=local_path,
            Bucket=_bucket_name,
            Key=b2_key,
            ExtraArgs={"ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            Config=TransferConfig(use_threads=False),
        )
        with _b2_keys_lock:
            _b2_keys[local_path] = b2_key
        logger.info("   [B2] ✅ Uploaded: %s", b2_key)

        if delete_local and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.info("   [B2] 🗑️  Deleted local: %s", os.path.basename(local_path))
            except OSError as e:
                logger.warning("   [B2] ⚠️  Could not delete local %s: %s", os.path.basename(local_path), e)
        return True

    except Exception as e:
        logger.warning("   [B2] ⚠️  Upload failed for %s: %s", os.path.basename(local_path), e)
        logger.warning("   [B2]     File remains available locally")
        return False


def upload_to_b2_async(local_path: str, b2_key: str, delete_local: bool = True):
    """
    Submit an upload to the background thread pool (non-blocking).
    No-op if B2 is not enabled.
    """
    if not is_enabled() or not _upload_executor:
        return

    # Store the key mapping immediately so download endpoints can find it
    # even before the upload completes.
    with _b2_keys_lock:
        _b2_keys[local_path] = b2_key

    _upload_executor.submit(upload_to_b2, local_path, b2_key, delete_local)


# ═══════════════════════════════════════════════════════
#  Download
# ═══════════════════════════════════════════════════════

def get_download_url(b2_key: str) -> Optional[str]:
    """
    Generate a pre-signed download URL for a B2 object.
    Returns None if B2 is not enabled or URL generation fails.
    """
    if not is_enabled():
        return None

    try:
        url = _s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": _bucket_name, "Key": b2_key},
            ExpiresIn=_presigned_url_expiry,
        )
        return url
    except Exception as e:
        logger.warning("   [B2] ⚠️  Could not generate download URL for %s: %s", b2_key, e)
        return None


def get_b2_key_for_local(local_path: str) -> Optional[str]:
    """Look up the B2 key for a file that was uploaded from a local path."""
    with _b2_keys_lock:
        return _b2_keys.get(local_path)


def get_b2_key_for_cdc_file(filename: str) -> Optional[str]:
    """
    Find the B2 key for a CDC output file by searching under its prefix.
    Used by the CDC download endpoint which only has the filename (no stored b2_key).
    """
    if not is_enabled():
        return None

    prefix = _classify_cdc_filename(filename)
    if not prefix:
        return None

    try:
        # Search across all date folders for this filename
        response = _s3_client.list_objects_v2(
            Bucket=_bucket_name,
            Prefix=prefix + "/",
        )
        for obj in response.get("Contents", []):
            if obj["Key"].endswith("/" + filename):
                return obj["Key"]
        # Check for pagination
        while response.get("IsTruncated"):
            response = _s3_client.list_objects_v2(
                Bucket=_bucket_name,
                Prefix=prefix + "/",
                ContinuationToken=response["NextContinuationToken"],
            )
            for obj in response.get("Contents", []):
                if obj["Key"].endswith("/" + filename):
                    return obj["Key"]
    except Exception as e:
        logger.warning("   [B2] ⚠️  Could not search for CDC file %s: %s", filename, e)

    return None


# ═══════════════════════════════════════════════════════
#  List / Delete
# ═══════════════════════════════════════════════════════

def list_b2_files(prefix: str) -> list[dict]:
    """
    List all objects under a B2 prefix (recursive, across date folders).
    Returns list of {"filename": ..., "size_kb": ..., "created_at": ...}
    sorted by last modified descending (newest first).
    """
    if not is_enabled():
        return []

    files = []
    try:
        paginator = _s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=_bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                # Extract just the filename (last segment of the key)
                filename = obj["Key"].rsplit("/", 1)[-1]
                if not filename:
                    continue
                last_mod = obj.get("LastModified")
                created_at = (
                    last_mod.strftime("%Y-%m-%d %H:%M:%S")
                    if last_mod else ""
                )
                files.append({
                    "filename": filename,
                    "size_kb": round(obj.get("Size", 0) / 1024, 1),
                    "created_at": created_at,
                    "b2_key": obj["Key"],
                })
    except Exception as e:
        logger.warning("   [B2] ⚠️  Could not list files under %s: %s", prefix, e)

    # Sort newest first
    files.sort(key=lambda f: f["created_at"], reverse=True)
    return files


def delete_b2_files(prefix: str, filenames: Optional[list[str]] = None) -> int:
    """
    Delete objects from B2.

    If filenames is None, delete ALL objects under the prefix.
    If filenames is provided, delete only those specific files (searched under prefix).

    Returns count of successfully deleted objects.
    """
    if not is_enabled():
        return 0

    deleted = 0
    try:
        # Collect keys to delete
        keys_to_delete = []

        if filenames is not None:
            # Delete specific files — need to find their full keys first
            all_files = list_b2_files(prefix)
            for f in all_files:
                if f["filename"] in filenames:
                    keys_to_delete.append(f["b2_key"])
        else:
            # Delete all under prefix
            paginator = _s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=_bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    keys_to_delete.append(obj["Key"])

        # Batch delete (S3 API supports up to 1000 per request)
        for i in range(0, len(keys_to_delete), 1000):
            batch = keys_to_delete[i:i + 1000]
            response = _s3_client.delete_objects(
                Bucket=_bucket_name,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = response.get("Errors", [])
            deleted += len(batch) - len(errors)
            for err in errors:
                logger.warning("   [B2] ⚠️  Failed to delete %s: %s", err['Key'], err['Message'])

    except Exception as e:
        logger.warning("   [B2] ⚠️  Could not delete files under %s: %s", prefix, e)

    if deleted:
        logger.info("   [B2] 🗑️  Deleted %d file(s) from %s", deleted, prefix)
    return deleted


# ═══════════════════════════════════════════════════════
#  Lifecycle Rule (30-day auto-expiry)
# ═══════════════════════════════════════════════════════

def setup_lifecycle_rule():
    """
    Set a 30-day expiration lifecycle rule on the bucket.
    Called once at startup. Safe to call repeatedly — it overwrites the rule.
    """
    if not is_enabled():
        return

    try:
        _s3_client.put_bucket_lifecycle_configuration(
            Bucket=_bucket_name,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "awacs-30-day-expiry",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "awacs-outputs/"},
                        "Expiration": {"Days": 30},
                    },
                    {
                        "ID": "awacs-delete-markers-cleanup",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "awacs-outputs/"},
                        "Expiration": {"ExpiredObjectDeleteMarker": True},
                    },
                ]
            },
        )
        logger.info("[B2] ✅ 30-day lifecycle rule set on bucket")
    except Exception as e:
        logger.warning("[B2] ⚠️  Could not set lifecycle rule: %s", e)
        logger.warning("[B2]     You may need to set it manually in the B2 console:")
        logger.warning("[B2]     Bucket Settings > Lifecycle Rules > Keep files for 30 days")
