#!/usr/bin/env bash
set -euo pipefail

: "${APP_NAME:?APP_NAME is required}"
: "${HEROKU_API_KEY:?HEROKU_API_KEY is required}"
: "${TEMPORAL_NAMESPACE:?TEMPORAL_NAMESPACE is required}"
: "${TEMPORAL_LEGACY_ADDRESS:?TEMPORAL_LEGACY_ADDRESS is required}"
: "${TEMPORAL_LEGACY_NAMESPACE:?TEMPORAL_LEGACY_NAMESPACE is required}"
: "${TEMPORAL_LEGACY_API_KEY:?TEMPORAL_LEGACY_API_KEY is required}"
: "${TEMPORAL_TLS_CA_BASE64:?TEMPORAL_TLS_CA_BASE64 is required}"
: "${TEMPORAL_TLS_CERT_BASE64:?TEMPORAL_TLS_CERT_BASE64 is required}"
: "${TEMPORAL_TLS_KEY_BASE64:?TEMPORAL_TLS_KEY_BASE64 is required}"

python3 - <<'PY' >/tmp/temporal-config.json
import json
import os

namespace = os.environ["TEMPORAL_NAMESPACE"]
print(json.dumps({
    "TEMPORAL_ADDRESS": "app.whiteboardgeeks.com:7233",
    "TEMPORAL_NAMESPACE": namespace,
    "TEMPORAL_API_KEY": None,
    "TEMPORAL_TLS": None,
    "TEMPORAL_TLS_CA": None,
    "TEMPORAL_TLS_CERT": None,
    "TEMPORAL_TLS_KEY": None,
    "TEMPORAL_TLS_CA_BASE64": os.environ["TEMPORAL_TLS_CA_BASE64"],
    "TEMPORAL_TLS_CERT_BASE64": os.environ["TEMPORAL_TLS_CERT_BASE64"],
    "TEMPORAL_TLS_KEY_BASE64": os.environ["TEMPORAL_TLS_KEY_BASE64"],
    "TEMPORAL_TLS_SERVER_NAME": "app.whiteboardgeeks.com",
    "TEMPORAL_WORKFLOW_UI_BASE_URL": (
        "https://app.whiteboardgeeks.com/temporal/namespaces/"
        f"{namespace}/workflows"
    ),
    "TEMPORAL_LEGACY_ADDRESS": os.environ["TEMPORAL_LEGACY_ADDRESS"],
    "TEMPORAL_LEGACY_NAMESPACE": os.environ["TEMPORAL_LEGACY_NAMESPACE"],
    "TEMPORAL_LEGACY_API_KEY": os.environ["TEMPORAL_LEGACY_API_KEY"],
}))
PY

curl --silent --show-error --fail \
  --request PATCH \
  "https://api.heroku.com/apps/${APP_NAME}/config-vars" \
  --header "Accept: application/vnd.heroku+json; version=3" \
  --header "Authorization: Bearer ${HEROKU_API_KEY}" \
  --header "Content-Type: application/json" \
  --data-binary @/tmp/temporal-config.json \
  --output /dev/null

rm -f /tmp/temporal-config.json
echo "Updated ${APP_NAME}; Heroku is restarting its dynos."
