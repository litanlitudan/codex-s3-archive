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
    elif nohup_supervisor_is_running; then
      echo "running"
    elif daemon_is_running; then
      echo "running"
    else
      echo "stopped"
    fi
  fi
}

daemon_pid_path() {
  printf '%s\n' "${STATE_ROOT}/daemon.pid"
}

nohup_supervisor_is_running() {
  local pid_path pid
  pid_path="${STATE_ROOT}/daemon-supervisor.pid"
  if [ ! -f "$pid_path" ]; then
    return 1
  fi

  pid="$(cat "$pid_path" 2>/dev/null || true)"
  if [ -z "$pid" ]; then
    return 1
  fi

  kill -0 "$pid" >/dev/null 2>&1 && nohup_supervisor_pid_matches "$pid"
}

daemon_is_running() {
  local pid
  pid="$(find_daemon_pid)"
  if [ -z "$pid" ]; then
    return 1
  fi
  kill -0 "$pid" >/dev/null 2>&1 && daemon_process_matches "$pid"
}

find_daemon_pid() {
  local pid_path pid
  pid_path="$(daemon_pid_path)"
  if [ -f "$pid_path" ]; then
    pid="$("${PYTHON3_PATH}" -c 'import json, pathlib, sys; path = pathlib.Path(sys.argv[1]); data = json.loads(path.read_text(encoding="utf-8")); print(data.get("pid") or "")' "$pid_path" 2>/dev/null || true)"
    if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1 && daemon_process_matches "$pid"; then
      printf '%s\n' "$pid"
      return 0
    fi
  fi

  ps ax -o pid= -o command= 2>/dev/null | awk -v pat="${STATE_ROOT}/bin/codex-s3-archive-daemon" 'index($0, pat) > 0 {print $1; exit}'
}

nohup_supervisor_pid_matches() {
  local pid="$1"
  local command_line
  command_line="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [ -n "$command_line" ] || return 1
  case "$command_line" in
    *"${STATE_ROOT}/bin/codex-s3-archive-supervisor.sh"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

daemon_process_matches() {
  local pid="$1"
  local command_line
  command_line="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [ -n "$command_line" ] || return 1
  case "$command_line" in
    *"${STATE_ROOT}/bin/codex-s3-archive-daemon"*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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

real_hook_info() {
  local marker_path="${STATE_ROOT}/last-real-stop-hook.json"
  if [ ! -f "$marker_path" ]; then
    echo "never|-1|unknown"
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
    value = data["seen_at"]
    dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    now = datetime.datetime.now(datetime.timezone.utc)
    age = max(int((now - dt).total_seconds()), 0)
    session_id = data.get("session_id") or "unknown"
    print(f"{age}s_ago|{age}|{session_id}")
except Exception:
    print("never|-1|unknown")
' "$marker_path"
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
  local real_hook_triplet
  local real_hook_display
  local real_hook_age
  local real_hook_session
  local overall
  local exit_code

  service="$(service_state)"
  heartbeat_pair="$(heartbeat_info)"
  heartbeat_display="${heartbeat_pair%%|*}"
  heartbeat_age="${heartbeat_pair##*|}"
  real_hook_triplet="$(real_hook_info)"
  real_hook_display="${real_hook_triplet%%|*}"
  real_hook_age="${real_hook_triplet#*|}"
  real_hook_session="${real_hook_age#*|}"
  real_hook_age="${real_hook_age%%|*}"
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

  printf '%-8s service=%s  heartbeat=%s  real_hook=%s  real_session=%s  queue=%s  dead=%s  staging=%s\n' \
    "$overall" "$service" "$heartbeat_display" "$real_hook_display" "$real_hook_session" "$queue_count" "$dead_count" "$staging_count"
  exit "$exit_code"
}

main "$@"
