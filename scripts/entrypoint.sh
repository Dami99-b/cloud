#!/usr/bin/env bash
set -euo pipefail

ROLE="${1:-api}"
shift || true

log() { printf '{"level":"INFO","service":"entrypoint","message":"%s"}\n' "$*"; }

wait_for_postgres() {
  local attempt=1
  local max_attempts="${DB_WAIT_ATTEMPTS:-60}"
  local url="${DATABASE_URL:-}"
  local host port
  host="$(printf '%s' "$url" | sed -nE 's#.*@([^:/]+).*#\1#p')"
  port="$(printf '%s' "$url" | sed -nE 's#.*@[^:]+:([0-9]+).*#\1#p')"
  host="${host:-postgres}"
  port="${port:-5432}"

  until pg_isready -h "$host" -p "$port" >/dev/null 2>&1; do
    if [ "$attempt" -ge "$max_attempts" ]; then
      log "postgres at ${host}:${port} did not become ready"
      exit 1
    fi
    log "waiting for postgres at ${host}:${port} (${attempt}/${max_attempts})"
    attempt=$((attempt + 1))
    sleep 2
  done
  log "postgres is ready"
}

case "$ROLE" in
  api)
    wait_for_postgres
    log "applying migrations"
    alembic upgrade head
    log "starting uvicorn"
    exec uvicorn app.main:app \
      --host 0.0.0.0 \
      --port "${PORT:-8000}" \
      --workers "${WEB_CONCURRENCY:-1}" \
      --proxy-headers \
      --no-access-log
    ;;
  worker)
    wait_for_postgres
    log "starting worker"
    exec python -m app.worker.main
    ;;
  migrate)
    wait_for_postgres
    exec alembic upgrade head
    ;;
  *)
    exec "$ROLE" "$@"
    ;;
esac
