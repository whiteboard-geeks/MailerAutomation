#!/bin/sh
set -eu

: "${TEMPORAL_ADDRESS:?TEMPORAL_ADDRESS is required}"
: "${PROD_NAMESPACE:?PROD_NAMESPACE is required}"
: "${STAGING_NAMESPACE:?STAGING_NAMESPACE is required}"

ensure_namespace() {
  namespace="$1"

  if ! temporal operator namespace describe --namespace "${namespace}" >/dev/null 2>&1; then
    temporal operator namespace create \
      --namespace "${namespace}" \
      --retention 30d
  fi

  # Namespace registration is asynchronous. Wait until all frontend calls can
  # resolve it before creating the namespace-scoped SQL search attribute.
  attempts=0
  until temporal operator namespace describe --namespace "${namespace}" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "Namespace ${namespace} did not become available" >&2
      return 1
    fi
    sleep 2
  done

  attempts=0
  while ! attributes="$(
    temporal operator search-attribute list --namespace "${namespace}" 2>/dev/null
  )"; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "Visibility for namespace ${namespace} did not become available" >&2
      return 1
    fi
    sleep 2
  done

  if ! printf '%s\n' "$attributes" | grep -q '^WaitingForResume'; then
    temporal operator search-attribute create \
      --namespace "${namespace}" \
      --name WaitingForResume \
      --type Bool
  fi
}

ensure_namespace "${PROD_NAMESPACE}"
ensure_namespace "${STAGING_NAMESPACE}"

echo "Temporal namespaces and search attributes are ready."
