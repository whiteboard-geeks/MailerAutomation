#!/usr/bin/env bash
set -euo pipefail

install_dir="${TEMPORAL_INSTALL_DIR:-/opt/temporal}"
backup_dir="${TEMPORAL_BACKUP_DIR:-/var/backups/temporal}"
retention_days="${TEMPORAL_BACKUP_RETENTION_DAYS:-14}"

cd "$install_dir"
set -a
# shellcheck disable=SC1091
source .env
set +a

umask 077
mkdir -p "$backup_dir"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"

for database in temporal temporal_visibility; do
  target="$backup_dir/${database}-${timestamp}.dump"
  temporary="${target}.tmp"
  docker compose exec -T \
    -e "PGPASSWORD=${POSTGRES_PASSWORD}" \
    postgresql \
    pg_dump -U "$POSTGRES_USER" -Fc "$database" >"$temporary"
  test -s "$temporary"
  mv "$temporary" "$target"
done

globals="$backup_dir/globals-${timestamp}.sql"
docker compose exec -T \
  -e "PGPASSWORD=${POSTGRES_PASSWORD}" \
  postgresql \
  pg_dumpall -U "$POSTGRES_USER" --globals-only >"$globals"
test -s "$globals"

sha256sum \
  "$backup_dir/temporal-${timestamp}.dump" \
  "$backup_dir/temporal_visibility-${timestamp}.dump" \
  "$globals" >"$backup_dir/sha256-${timestamp}.txt"

find "$backup_dir" -type f -mtime "+${retention_days}" -delete
