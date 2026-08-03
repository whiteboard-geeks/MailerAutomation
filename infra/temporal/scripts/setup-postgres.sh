#!/bin/sh
set -eu

: "${POSTGRES_SEEDS:?POSTGRES_SEEDS is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${SQL_PASSWORD:?SQL_PASSWORD is required}"

export SQL_PASSWORD

until nc -z -w 5 "${POSTGRES_SEEDS}" "${DB_PORT:-5432}"; do
  echo "Waiting for PostgreSQL..."
  sleep 2
done

setup_database() {
  database="$1"
  schema_dir="$2"

  if ! temporal-sql-tool \
      --plugin postgres12 \
      --ep "${POSTGRES_SEEDS}" \
      -u "${POSTGRES_USER}" \
      -p "${DB_PORT:-5432}" \
      --db "${database}" \
      describe-version >/dev/null 2>&1; then
    temporal-sql-tool \
      --plugin postgres12 \
      --ep "${POSTGRES_SEEDS}" \
      -u "${POSTGRES_USER}" \
      -p "${DB_PORT:-5432}" \
      --db "${database}" \
      create

    temporal-sql-tool \
      --plugin postgres12 \
      --ep "${POSTGRES_SEEDS}" \
      -u "${POSTGRES_USER}" \
      -p "${DB_PORT:-5432}" \
      --db "${database}" \
      setup-schema -v 0.0
  fi

  temporal-sql-tool \
    --plugin postgres12 \
    --ep "${POSTGRES_SEEDS}" \
    -u "${POSTGRES_USER}" \
    -p "${DB_PORT:-5432}" \
    --db "${database}" \
    update-schema -d "${schema_dir}"
}

setup_database temporal /etc/temporal/schema/postgresql/v12/temporal/versioned
setup_database temporal_visibility /etc/temporal/schema/postgresql/v12/visibility/versioned

echo "Temporal PostgreSQL schemas are ready."
