#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

read_env_var() {
  local key="$1"
  local env_file="${2:-.env}"
  if [ ! -f "$env_file" ]; then
    return 0
  fi
  awk -F= -v key="$key" '
    $0 ~ /^[[:space:]]*#/ { next }
    $0 ~ /^[[:space:]]*$/ { next }
    $1 == key {
      sub(/^[^=]*=/, "", $0)
      print $0
      exit
    }
  ' "$env_file"
}

is_local_clickhouse_url() {
  case "${CLICKHOUSE_URL:-}" in
    http://127.0.0.1*|https://127.0.0.1*|http://localhost*|https://localhost*|http://\[::1\]*|https://\[::1\]*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

clickhouse_ping() {
  if ! command -v curl >/dev/null 2>&1; then
    return 1
  fi
  curl -fsS "${CLICKHOUSE_URL%/}/ping" >/dev/null 2>&1
}

start_local_clickhouse() {
  local binary=""
  local config=""
  local attempt=0

  if clickhouse_ping; then
    return 0
  fi

  if ! is_local_clickhouse_url; then
    echo "ERROR: ClickHouse is not reachable at ${CLICKHOUSE_URL}."
    echo "ERROR: The configured ClickHouse URL is not local, so start_local.sh will not auto-start it."
    exit 1
  fi

  if [ -x "$HOME/.local/clickhouse/bin/clickhouse" ] && [ -f "$HOME/.local/clickhouse/etc/clickhouse-server/config.xml" ]; then
    binary="$HOME/.local/clickhouse/bin/clickhouse"
    config="$HOME/.local/clickhouse/etc/clickhouse-server/config.xml"
  elif command -v clickhouse >/dev/null 2>&1 && [ -f "/etc/clickhouse-server/config.xml" ]; then
    binary="$(command -v clickhouse)"
    config="/etc/clickhouse-server/config.xml"
  elif command -v clickhouse-server >/dev/null 2>&1 && [ -f "/etc/clickhouse-server/config.xml" ]; then
    binary="$(command -v clickhouse-server)"
    config="/etc/clickhouse-server/config.xml"
  else
    echo "ERROR: ClickHouse is not reachable and no local ClickHouse installation was found."
    echo "ERROR: Expected one of:"
    echo "  - $HOME/.local/clickhouse/bin/clickhouse"
    echo "  - clickhouse with /etc/clickhouse-server/config.xml"
    exit 1
  fi

  echo "INFO: starting local ClickHouse with ${binary}"
  if [[ "$(basename "$binary")" == "clickhouse-server" ]]; then
    "$binary" --config-file "$config" --daemon
  else
    "$binary" server --config-file "$config" --daemon
  fi

  while [ "$attempt" -lt 40 ]; do
    if clickhouse_ping; then
      echo "INFO: ClickHouse is ready at ${CLICKHOUSE_URL}"
      return 0
    fi
    sleep 0.5
    attempt=$((attempt + 1))
  done

  echo "ERROR: ClickHouse did not become ready at ${CLICKHOUSE_URL}"
  exit 1
}

stop_existing_app_server() {
  local match="uvicorn app.main:app"
  local pids=""
  local attempt=0

  if ! command -v pgrep >/dev/null 2>&1; then
    return 0
  fi

  pids="$(pgrep -f "$match" || true)"
  if [ -z "$pids" ]; then
    return 0
  fi

  echo "INFO: stopping existing app server process(es): $(printf '%s ' $pids)"
  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    kill "$pid" 2>/dev/null || true
  done <<EOF
$pids
EOF

  while [ "$attempt" -lt 25 ]; do
    if ! pgrep -f "$match" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
    attempt=$((attempt + 1))
  done

  pids="$(pgrep -f "$match" || true)"
  if [ -z "$pids" ]; then
    return 0
  fi

  echo "WARN: force stopping remaining app server process(es): $(printf '%s ' $pids)"
  while IFS= read -r pid; do
    [ -n "$pid" ] || continue
    kill -9 "$pid" 2>/dev/null || true
  done <<EOF
$pids
EOF
}

ensure_port_available() {
  local port="$1"
  local listeners=""

  if ! command -v lsof >/dev/null 2>&1; then
    return 0
  fi

  listeners="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)"
  if [ -n "$listeners" ]; then
    echo "ERROR: Port ${port} is still in use: $(printf '%s ' $listeners)"
    exit 1
  fi
}

if [ -n "${PYTHON_BIN:-}" ]; then
  PYTHON_BIN="$PYTHON_BIN"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  PYTHON_BIN="python3"
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "ERROR: Python not found. Set PYTHON_BIN or install Python 3.10+."
  exit 1
fi

if ! "$PYTHON_BIN" -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)"; then
  echo "ERROR: Python 3.10+ is required."
  exit 1
fi

if [ ! -f ".env" ] && [ -f ".env.example" ]; then
  cp .env.example .env
  echo "INFO: .env not found, created from .env.example."
fi

if [ -z "${HOST:-}" ]; then
  HOST="$(read_env_var HOST)"
fi
if [ -z "${PORT:-}" ]; then
  PORT="$(read_env_var PORT)"
fi
if [ -z "${CLICKHOUSE_URL:-}" ]; then
  CLICKHOUSE_URL="$(read_env_var CLICKHOUSE_URL)"
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

if ! "$PYTHON_BIN" -c "import fastapi, uvicorn, pydantic_settings, jwt, argon2, bcrypt, multipart, openpyxl, cryptography, polars, fastexcel" >/dev/null 2>&1; then
  PY_EXE="$("$PYTHON_BIN" -c 'import sys; print(sys.executable)' 2>/dev/null || echo "$PYTHON_BIN")"
  echo "ERROR: Missing runtime dependencies in interpreter: ${PY_EXE}"
  echo "Run:"
  echo "  conda env create -f environment.yml"
  echo "  conda activate data-manager"
  exit 1
fi

if [ ! -f "./data/keys.json" ]; then
  echo "INFO: key file not found. Generating ./data/keys.json ..."
  "$PYTHON_BIN" scripts/generate_keys.py
fi

if [ -z "${CLICKHOUSE_URL:-}" ]; then
  echo "ERROR: CLICKHOUSE_URL is required."
  exit 1
fi

start_local_clickhouse

"$PYTHON_BIN" scripts/init_db.py

if [ "${CHECK_ONLY:-0}" = "1" ]; then
  echo "INFO: environment check passed."
  exit 0
fi

stop_existing_app_server
ensure_port_available "$PORT"

echo "INFO: starting service on ${HOST}:${PORT}"
exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
