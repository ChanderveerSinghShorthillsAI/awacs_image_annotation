"""Sends CDC pipeline completion email via AWS SES with Excel attachments."""
import logging
import time
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from botocore.exceptions import ClientError

import cdc_pipeline.config as _cfg

logger = logging.getLogger("awacs.cdc.email_notifier")


def _download_s3_to_bytes(s3_client, bucket: str, key: str,
                          retries: int = 12, delay: float = 5.0) -> bytes:
    """Fetch an object from S3, retrying briefly if the key is not yet visible.

    The CDC pipeline marks the annotation job COMPLETED before B2/S3 upload of
    the output file finishes (DB update + review save + B2 flush all run after).
    run_annotation.py sees COMPLETED and calls this notifier immediately, so on
    a fast network the GET races the PUT. Retrying for up to ~60s covers that
    gap without making the failure mode worse — if the file truly never
    uploads, we still bubble up the error after the retries.
    """
    last_err = None
    for attempt in range(retries):
        try:
            resp = s3_client.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            # Only retry on "key not yet present" — anything else is a real error
            # (permission, bucket missing, throttling) and waiting won't help.
            if code in ("NoSuchKey", "404"):
                last_err = e
                if attempt < retries - 1:
                    logger.info("S3 key %s not yet visible (attempt %d/%d) — waiting %.1fs",
                                key, attempt + 1, retries, delay)
                    time.sleep(delay)
                    continue
            raise
    raise last_err  # pragma: no cover


def _build_attachment(data: bytes, filename: str) -> MIMEBase:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(data)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    return part


def send_pipeline_completion_email(
    annotated_s3_key: str,
    review_s3_key: str | None,
    run_date: str,
    total_ads: int,
    success_count: int,
    dry_run: bool = False,
) -> None:
    """Build and send SES email with Excel attachments. Non-fatal on any error."""
    sender     = _cfg.SES_SENDER
    recipients_raw = _cfg.SES_RECIPIENTS
    region     = _cfg.SES_REGION
    s3_bucket  = _cfg.SES_S3_BUCKET

    if not sender or not recipients_raw or not s3_bucket:
        logger.warning("SES_SENDER, SES_RECIPIENTS or SES_S3_BUCKET not set — skipping email notification")
        return

    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]

    s3  = boto3.client("s3",  region_name=region)
    ses = boto3.client("ses", region_name=region)

    annotated_bytes    = _download_s3_to_bytes(s3, s3_bucket, annotated_s3_key)
    annotated_filename = annotated_s3_key.split("/")[-1]

    review_bytes    = None
    review_filename = None
    if review_s3_key:
        review_bytes    = _download_s3_to_bytes(s3, s3_bucket, review_s3_key)
        review_filename = review_s3_key.split("/")[-1]

    msg = MIMEMultipart()
    msg["Subject"] = f"[AWACS] CDC Pipeline Complete — {run_date}"
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)

    review_line = (
        f"  Review Excel:     {review_filename} (attached)"
        if review_bytes
        else "  Review Excel:     Not generated today (no human-review ads)"
    )

    body = f"""AWACS Nightly CDC Annotation — {run_date}

Pipeline completed successfully.

  Date:             {run_date}
  Total ads:        {total_ads:,}
  Patched to DB:    {success_count:,}

Attached files:
  Annotated Output: {annotated_filename}
{review_line}

Sent automatically by AWACS CDC pipeline.
"""
    msg.attach(MIMEText(body, "plain"))
    msg.attach(_build_attachment(annotated_bytes, annotated_filename))
    if review_bytes:
        msg.attach(_build_attachment(review_bytes, review_filename))

    if dry_run:
        logger.info("[DRY RUN] Would send to: %s | Subject: %s | Attachments: %s%s",
                    recipients, msg["Subject"], annotated_filename,
                    f", {review_filename}" if review_filename else "")
        return

    ses.send_raw_email(
        Source=sender,
        Destinations=recipients,
        RawMessage={"Data": msg.as_bytes()},
    )
    logger.info("Pipeline completion email sent to: %s", recipients)
