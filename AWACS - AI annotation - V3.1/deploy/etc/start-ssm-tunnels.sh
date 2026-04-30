#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
OS="$(uname -s)"

# Load .env
if [ -f "$ROOT_DIR/.env" ]; then
    set -a
    source "$ROOT_DIR/.env"
    set +a
fi

# AUTH_PROFILE is optional on EC2 (instance role used instead).
# On dev laptops it must be set in .env.
_ssm_profile_flag=""
if [ -n "${AUTH_PROFILE:-}" ]; then
    _ssm_profile_flag="--profile ${AUTH_PROFILE}"
fi
: "${KAFKA_REGION:?Set KAFKA_REGION in .env}"
: "${KAFKA_TARGET_INSTANCE:?Set KAFKA_TARGET_INSTANCE in .env}"
: "${KAFKA_BROKER_1:?Set KAFKA_BROKER_1 in .env}"
: "${KAFKA_BROKER_2:?Set KAFKA_BROKER_2 in .env}"
: "${KAFKA_BROKER_3:?Set KAFKA_BROKER_3 in .env}"
: "${KAFKA_BROKER_PORT:=9096}"

# Session Manager port forwarding requires the local plugin binary.
if ! command -v session-manager-plugin >/dev/null 2>&1; then
    echo "Error: session-manager-plugin is not installed."
    echo "Install on macOS: brew install --cask session-manager-plugin"
    echo "Docs: https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html"
    exit 1
fi

# Verify AWS CLI is authenticated before starting tunnels
echo "Checking AWS credentials..."
if ! aws sts get-caller-identity ${_ssm_profile_flag} --region "${KAFKA_REGION}" >/dev/null 2>&1; then
    echo "Error: AWS CLI is not authenticated."
    [ -n "${AUTH_PROFILE:-}" ] && echo "Run: aws sso login --profile ${AUTH_PROFILE}"
    exit 1
fi
echo "Authenticated as: $(aws sts get-caller-identity ${_ssm_profile_flag} --region "${KAFKA_REGION}" --query 'Arn' --output text)"
echo ""

cleanup() {
    echo ""
    echo "Stopping SSM tunnels and socat bridges..."
    kill $(jobs -p) 2>/dev/null || true
    wait 2>/dev/null || true
    echo "All tunnels stopped."
}
trap cleanup EXIT INT TERM

# === Dev SSM tunnels ===
echo "Starting dev SSM tunnels..."
echo "  Broker 1: ${KAFKA_BROKER_1} → localhost:9096"
echo "  Broker 2: ${KAFKA_BROKER_2} → localhost:9097"
echo "  Broker 3: ${KAFKA_BROKER_3} → localhost:9098"

aws ssm start-session \
    ${_ssm_profile_flag} \
    --region "${KAFKA_REGION}" \
    --target "${KAFKA_TARGET_INSTANCE}" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters host="${KAFKA_BROKER_1}",portNumber="${KAFKA_BROKER_PORT}",localPortNumber="9096" &

aws ssm start-session \
    ${_ssm_profile_flag} \
    --region "${KAFKA_REGION}" \
    --target "${KAFKA_TARGET_INSTANCE}" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters host="${KAFKA_BROKER_2}",portNumber="${KAFKA_BROKER_PORT}",localPortNumber="9097" &

aws ssm start-session \
    ${_ssm_profile_flag} \
    --region "${KAFKA_REGION}" \
    --target "${KAFKA_TARGET_INSTANCE}" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters host="${KAFKA_BROKER_3}",portNumber="${KAFKA_BROKER_PORT}",localPortNumber="9098" &

if [ "$OS" = "Darwin" ]; then
    echo "Skipping host socat bridges on macOS (dockerized msk-socat handles broker mapping)."
else
    # Linux: use loopback aliases for clients expecting all brokers on :9096.
    echo "Starting dev socat bridges..."
    echo "  127.0.0.2:9096 → 127.0.0.1:9097 (dev broker 2)"
    echo "  127.0.0.3:9096 → 127.0.0.1:9098 (dev broker 3)"
    socat TCP-LISTEN:9096,fork,bind=127.0.0.2,reuseaddr TCP:127.0.0.1:9097 &
    socat TCP-LISTEN:9096,fork,bind=127.0.0.3,reuseaddr TCP:127.0.0.1:9098 &
fi

# === Prod SSM tunnels (optional — only if PROD_MSK_PASSWORD is set) ===
if [ -n "${PROD_MSK_PASSWORD:-}" ]; then
    PROD_AUTH_PROFILE="${PROD_AUTH_PROFILE:-tol}"
    PROD_KAFKA_REGION="${PROD_KAFKA_REGION:-us-east-1}"
    PROD_KAFKA_TARGET_INSTANCE="${PROD_KAFKA_TARGET_INSTANCE:-i-0f80554b66e067d69}"
    PROD_KAFKA_BROKER_1="${PROD_KAFKA_BROKER_1:-b-1.datasyndicationmskpro.rkqpha.c13.kafka.us-east-1.amazonaws.com}"
    PROD_KAFKA_BROKER_2="${PROD_KAFKA_BROKER_2:-b-2.datasyndicationmskpro.rkqpha.c13.kafka.us-east-1.amazonaws.com}"
    PROD_KAFKA_BROKER_3="${PROD_KAFKA_BROKER_3:-b-3.datasyndicationmskpro.rkqpha.c13.kafka.us-east-1.amazonaws.com}"
    PROD_KAFKA_BROKER_PORT="${PROD_KAFKA_BROKER_PORT:-9096}"

    _prod_ssm_profile_flag=""
    if [ -n "${PROD_AUTH_PROFILE:-}" ]; then
        _prod_ssm_profile_flag="--profile ${PROD_AUTH_PROFILE}"
    fi

    echo ""
    echo "Checking AWS credentials for prod..."
    if ! aws sts get-caller-identity ${_prod_ssm_profile_flag} --region "${PROD_KAFKA_REGION}" >/dev/null 2>&1; then
        echo "Warning: Prod AWS CLI not authenticated. Skipping prod tunnels."
        [ -n "${PROD_AUTH_PROFILE:-}" ] && echo "Run: aws sso login --profile ${PROD_AUTH_PROFILE}"
    else
        echo "Authenticated as: $(aws sts get-caller-identity ${_prod_ssm_profile_flag} --region "${PROD_KAFKA_REGION}" --query 'Arn' --output text)"

        echo "Starting prod SSM tunnels..."
        echo "  Broker 1: ${PROD_KAFKA_BROKER_1} → localhost:9196"
        echo "  Broker 2: ${PROD_KAFKA_BROKER_2} → localhost:9197"
        echo "  Broker 3: ${PROD_KAFKA_BROKER_3} → localhost:9198"

        aws ssm start-session \
            ${_prod_ssm_profile_flag} \
            --region "${PROD_KAFKA_REGION}" \
            --target "${PROD_KAFKA_TARGET_INSTANCE}" \
            --document-name AWS-StartPortForwardingSessionToRemoteHost \
            --parameters host="${PROD_KAFKA_BROKER_1}",portNumber="${PROD_KAFKA_BROKER_PORT}",localPortNumber="9196" &

        aws ssm start-session \
            ${_prod_ssm_profile_flag} \
            --region "${PROD_KAFKA_REGION}" \
            --target "${PROD_KAFKA_TARGET_INSTANCE}" \
            --document-name AWS-StartPortForwardingSessionToRemoteHost \
            --parameters host="${PROD_KAFKA_BROKER_2}",portNumber="${PROD_KAFKA_BROKER_PORT}",localPortNumber="9197" &

        aws ssm start-session \
            ${_prod_ssm_profile_flag} \
            --region "${PROD_KAFKA_REGION}" \
            --target "${PROD_KAFKA_TARGET_INSTANCE}" \
            --document-name AWS-StartPortForwardingSessionToRemoteHost \
            --parameters host="${PROD_KAFKA_BROKER_3}",portNumber="${PROD_KAFKA_BROKER_PORT}",localPortNumber="9198" &

        if [ "$OS" = "Darwin" ]; then
            echo "Skipping prod host socat bridges on macOS (dockerized msk-socat handles broker mapping)."
        else
            echo "Starting prod socat bridges..."
            echo "  127.0.0.4:9096 → 127.0.0.1:9196 (prod broker 1)"
            echo "  127.0.0.5:9096 → 127.0.0.1:9197 (prod broker 2)"
            echo "  127.0.0.6:9096 → 127.0.0.1:9198 (prod broker 3)"
            socat TCP-LISTEN:9096,fork,bind=127.0.0.4,reuseaddr TCP:127.0.0.1:9196 &
            socat TCP-LISTEN:9096,fork,bind=127.0.0.5,reuseaddr TCP:127.0.0.1:9197 &
            socat TCP-LISTEN:9096,fork,bind=127.0.0.6,reuseaddr TCP:127.0.0.1:9198 &
        fi
    fi
fi

echo ""
echo "SSM tunnels and socat bridges started. Press Ctrl+C to stop."
wait
