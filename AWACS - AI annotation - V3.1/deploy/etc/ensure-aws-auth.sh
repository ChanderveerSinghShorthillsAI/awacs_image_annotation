#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi

: "${KAFKA_REGION:?Set KAFKA_REGION in .env}"

# ── EC2 instance role detection (IMDSv2) ────────────────────────────────────
# On an EC2 instance, credentials are provided automatically by the attached
# IAM instance role via the metadata service. SSO login requires a browser
# and cannot work headlessly, so we skip it entirely when running on EC2.
_imds_token=""
_on_ec2=false
if _imds_token="$(curl -sf -m 2 \
        -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 10" 2>/dev/null)"; then
    _role="$(curl -sf -m 2 \
        -H "X-aws-ec2-metadata-token: $_imds_token" \
        "http://169.254.169.254/latest/meta-data/iam/security-credentials/" \
        2>/dev/null || true)"
    if [ -n "$_role" ]; then
        _on_ec2=true
    fi
fi

if [ "$_on_ec2" = "true" ]; then
    echo "Running on EC2 — using IAM instance role credentials (skipping SSO login)."
    if aws sts get-caller-identity --region "${KAFKA_REGION}" >/dev/null 2>&1; then
        echo "AWS credentials verified via instance role."
        exit 0
    else
        echo "Error: EC2 instance role credentials are not working."
        echo "Check that an IAM role with AmazonSSMManagedInstanceCore is attached to this instance."
        exit 1
    fi
fi

# ── Developer laptop: SSO flow ───────────────────────────────────────────────
: "${AUTH_PROFILE:?Set AUTH_PROFILE in .env (not needed on EC2, required on dev laptops)}"

check_sts() {
    local out
    if out="$(aws sts get-caller-identity --profile "${AUTH_PROFILE}" --region "${KAFKA_REGION}" 2>&1)"; then
        return 0
    fi

    AWS_STS_ERROR="$out"
    return 1
}

handle_forbidden_profile() {
    echo "Error: profile '${AUTH_PROFILE}' is authenticated but has no AWS account/role access."
    echo "Update AUTH_PROFILE in .env to a profile with MSK access (for example: dev), then retry."
    echo "Available AWS profiles:"
    aws configure list-profiles | sed 's/^/  - /'
}

echo "Checking AWS credentials for profile '${AUTH_PROFILE}'..."
if check_sts; then
    echo "AWS credentials already authenticated."
    exit 0
fi

if echo "${AWS_STS_ERROR}" | grep -qi "ForbiddenException.*No access\|No access"; then
    handle_forbidden_profile
    exit 1
fi

echo "AWS session missing/expired. Running: aws sso login --profile ${AUTH_PROFILE}"
aws sso login --profile "${AUTH_PROFILE}"

if ! check_sts; then
    if echo "${AWS_STS_ERROR}" | grep -qi "ForbiddenException.*No access\|No access"; then
        handle_forbidden_profile
        exit 1
    fi

    echo "Error: AWS login succeeded but STS check still fails for profile '${AUTH_PROFILE}'."
    echo "${AWS_STS_ERROR}"
    exit 1
fi

echo "AWS authentication completed."
