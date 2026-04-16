#!/bin/bash
set -uo pipefail

STATE_ROOT="${HOME}/.codex/s3-archive"
SERVICE_LABEL="com.codex.s3-archive"
SERVICE_NAME="codex-s3-archive"
PLATFORM=""
PYTHON3_PATH=""

usage() {
  cat <<EOF
Usage: $0 [--state-root PATH]
EOF
}

detect_platform() {
  case "$(uname -s)" in
    Darwin|Linux)
      PLATFORM="$(uname -s)"
      ;;
    *)
      echo "DEAD     service=unknown  heartbeat=never  queue=0  dead=0  staging=0"
      exit 2
      ;;
  esac
}

detect_python() {
  PYTHON3_PATH="$(command -v python3 2>/dev/null || true)"
  if [ -z "$PYTHON3_PATH" ]; then
    echo "DEAD     service=unknown  heartbeat=never  queue=0  dead=0  staging=0"
    exit 2
  fi
}

parse_cli_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --state-root)
        STATE_ROOT="${2:-}"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        echo "unknown argument: $1" >&2
        exit 2
        ;;
    esac
  done
}

service_state() {
  if [ "$PLATFORM" = "Darwin" ]; then
    if launchctl list "${SERVICE_LABEL}" >/dev/null 2>&1; then
      echo "running"
    else
      echo "stopped"
    fi
  else
    if [ "$(systemctl --user is-active "${SERVICE_NAME}" 2>/dev/null || true)" = "active" ]; then
      echo "running"
    else
      echo "stopped"
    fi
  fi
}

heartbeat_info() {
  local heartbeat_path="${STATE_ROOT}/heartbeat.json"
  if [ ! -f "$heartbeat_path" ]; then
    echo "never|-1"
    return 0
  fi

  "${PYTHON3_PATH}" -c '
import datetime
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
try:
    data = json.loads(path.read_text(encoding="utf-8"))
    value = data["last_cycle_at"]
    dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    now = datetime.datetime.now(datetime.timezone.utc)
    age = max(int((now - dt).total_seconds()), 0)
    print(f"{age}s_ago|{age}")
except Exception:
    print("never|-1")
' "$heartbeat_path"
}

count_json_files() {
  local dir="$1"
  if [ ! -d "$dir" ]; then
    echo "0"
    return 0
  fi
  find "$dir" -maxdepth 1 -type f -name '*.json' | wc -l | awk '{print $1}'
}

count_files() {
  local dir="$1"
  if [ ! -d "$dir" ]; then
    echo "0"
    return 0
  fi
  find "$dir" -maxdepth 1 -type f | wc -l | awk '{print $1}'
}

main() {
  parse_cli_args "$@"
  detect_platform
  detect_python

  local service
  local heartbeat_pair
  local heartbeat_display
  local heartbeat_age
  local queue_count
  local dead_count
  local staging_count
  local overall
  local exit_code

  service="$(service_state)"
  heartbeat_pair="$(heartbeat_info)"
  heartbeat_display="${heartbeat_pair%%|*}"
  heartbeat_age="${heartbeat_pair##*|}"
  queue_count="$(count_json_files "${STATE_ROOT}/queue")"
  dead_count="$(count_json_files "${STATE_ROOT}/queue/dead")"
  staging_count="$(count_files "${STATE_ROOT}/staging")"

  if [ "$service" != "running" ] || [ "$heartbeat_age" -lt 0 ]; then
    overall="DEAD"
    exit_code=2
  elif [ "$heartbeat_age" -gt 300 ] || [ "$dead_count" -gt 0 ] || [ "$staging_count" -gt 0 ]; then
    overall="WARN"
    exit_code=1
  else
    overall="OK"
    exit_code=0
  fi

  printf '%-8s service=%s  heartbeat=%s  queue=%s  dead=%s  staging=%s\n' \
    "$overall" "$service" "$heartbeat_display" "$queue_count" "$dead_count" "$staging_count"
  exit "$exit_code"
}

main "$@"
