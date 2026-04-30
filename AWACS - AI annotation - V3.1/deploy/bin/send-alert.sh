#!/usr/bin/env bash
# Install path on the VM: /opt/awacs/bin/send-alert.sh
# Sends an SNS notification when a systemd unit fails.
# Credentials come from the EC2 instance role — no static keys needed.
# SNS_TOPIC_ARN is read from /etc/cdc/sns.env via EnvironmentFile= in the unit.

set -euo pipefail

UNIT="${1:-unknown.service}"
HOST="$(hostname -f 2>/dev/null || hostname)"
TS="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

# Last 30 journal lines from the failing unit
BODY="$(journalctl -u "$UNIT" -n 30 --no-pager 2>&1 || true)"

: "${SNS_TOPIC_ARN:?SNS_TOPIC_ARN not set in /etc/cdc/sns.env}"
: "${AWS_DEFAULT_REGION:=us-east-1}"

MESSAGE="CDC FAILURE: $UNIT on $HOST at $TS

--- last 30 journal lines ---
$BODY"

aws sns publish \
    --region "$AWS_DEFAULT_REGION" \
    --topic-arn "$SNS_TOPIC_ARN" \
    --subject "CDC failure: $UNIT on $HOST" \
    --message "$MESSAGE"
