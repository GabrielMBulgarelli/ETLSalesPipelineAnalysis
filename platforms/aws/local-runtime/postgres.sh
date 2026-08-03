#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
SQL_DIR="${SCRIPT_DIR}/../sql/local-postgres"

: "${POSTGRES_DB:=ecommerce_sales}"
: "${POSTGRES_ADMIN_USER:=ecommerce_admin}"
: "${POSTGRES_ADMIN_PASSWORD:?set POSTGRES_ADMIN_PASSWORD from .env.example}"
: "${POSTGRES_ETL_USER:=ecommerce_etl}"
: "${POSTGRES_ETL_PASSWORD:?set POSTGRES_ETL_PASSWORD from .env.example}"

compose=(docker compose -f "${COMPOSE_FILE}")
admin_psql=("${compose[@]}" exec -T -e PGPASSWORD="${POSTGRES_ADMIN_PASSWORD}" postgres psql -X -v ON_ERROR_STOP=1 -U "${POSTGRES_ADMIN_USER}" -d "${POSTGRES_DB}")

wait_ready() {
  for _ in $(seq 1 60); do
    if "${compose[@]}" exec -T postgres pg_isready -U "${POSTGRES_ADMIN_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "PostgreSQL did not become ready" >&2
  exit 1
}

bootstrap() {
  "${compose[@]}" up -d postgres
  wait_ready
  "${admin_psql[@]}" -v loader_role="${POSTGRES_ETL_USER}" -v loader_password="${POSTGRES_ETL_PASSWORD}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L', :'loader_role', :'loader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'loader_role') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT PASSWORD %L', :'loader_role', :'loader_password') \gexec
SQL
  for migration in "${SQL_DIR}"/*.sql; do
    "${admin_psql[@]}" -v loader_role="${POSTGRES_ETL_USER}" < "${migration}"
  done
}

case "${1:-up}" in
  up) bootstrap ;;
  status)
    "${compose[@]}" ps postgres
    "${compose[@]}" exec -T -e PGPASSWORD="${POSTGRES_ETL_PASSWORD}" postgres \
      psql -X -U "${POSTGRES_ETL_USER}" -d "${POSTGRES_DB}" -c \
      'SELECT "BatchID", "PublicationFingerprint", "CompletedAt" FROM audit.completed_publications ORDER BY "CompletedAt" DESC;'
    ;;
  down) "${compose[@]}" stop postgres ;;
  clean)
    "${compose[@]}" stop postgres
    "${compose[@]}" rm -f postgres
    docker volume rm ecommerce-sales-aws-local_postgres-data
    ;;
  *) echo "usage: $0 {up|status|down|clean}" >&2; exit 2 ;;
esac
