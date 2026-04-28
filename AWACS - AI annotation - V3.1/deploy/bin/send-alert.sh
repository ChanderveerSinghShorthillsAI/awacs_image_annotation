#!/usr/bin/env bash
# Install path on the VM: /opt/awacs/bin/send-alert.sh
# Sends an email via Gmail SMTP when a systemd unit fails.
#
# Reads SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, ALERT_TO from environment
# (provided via EnvironmentFile=/etc/cdc/smtp.env in cdc-alert@.service).
#
# Usage: send-alert.sh <failed-unit-name>

set -euo pipefail

UNIT="${1:-unknown.service}"
HOST="$(hostname -f 2>/dev/null || hostname)"
TS="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

# Pull last 50 log lines from the failing unit so the email is actionable.
# --no-pager keeps it from hanging waiting for a TTY.
BODY="$(journalctl -u "$UNIT" -n 50 --no-pager 2>&1 || true)"

# Sanity-check required env vars are present. If anything is missing we still
# want a loud failure rather than a silent skip — let set -u trigger and
# systemd will record the exit code.
: "${SMTP_HOST:?missing}" "${SMTP_PORT:?missing}" "${SMTP_USER:?missing}" "${SMTP_PASS:?missing}" "${ALERT_TO:?missing}"

export UNIT HOST TS BODY

PYTHON_BIN="${PYTHON_BIN:-/opt/awacs/venv/bin/python}"

"$PYTHON_BIN" - <<'PY'
import os, smtplib, ssl
from email.message import EmailMessage

unit = os.environ["UNIT"]
host = os.environ["HOST"]
ts   = os.environ["TS"]
body = os.environ["BODY"]

m = EmailMessage()
m["Subject"] = f"[CDC] {unit} failed on {host} at {ts}"
m["From"]    = os.environ["SMTP_USER"]
m["To"]      = os.environ["ALERT_TO"]
m.set_content(
    f"Unit:   {unit}\n"
    f"Host:   {host}\n"
    f"Time:   {ts}\n\n"
    f"--- last 50 journal lines ---\n{body}\n"
)

ctx = ssl.create_default_context()
with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ["SMTP_PORT"]), timeout=30) as s:
    s.starttls(context=ctx)
    s.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
    s.send_message(m)
PY
