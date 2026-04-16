#!/bin/bash
set -euo pipefail

STATE_ROOT="${HOME}/.codex/s3-archive"
HOOKS_JSON="${HOME}/.codex/hooks.json"
PLIST_PATH="${HOME}/Library/LaunchAgents/com.codex.s3-archive.plist"
SYSTEMD_UNIT_PATH="${HOME}/.config/systemd/user/codex-s3-archive.service"
SERVICE_LABEL="com.codex.s3-archive"
SERVICE_NAME="codex-s3-archive"

PURGE=0
PLATFORM=""
PYTHON3_PATH=""
DISABLED_PATH=""
HOOKS_RESTORE_SUMMARY="none"

ARCHIVE_HOOK_COMMAND=""
WRAPPER_COMMAND=""
LEGACY_ARCHIVE_HOOK_COMMAND='$HOME/.codex/s3-archive/bin/codex-s3-archive-hook-stop --state-root $HOME/.codex/s3-archive'
LEGACY_WRAPPER_COMMAND='$HOME/.codex/s3-archive/bin/stop-wrapper.sh'

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

shell_quote() {
  printf '%q' "$1"
}

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --state-root PATH
  --purge
  --help
EOF
}

detect_platform() {
  case "$(uname -s)" in
    Darwin|Linux)
      PLATFORM="$(uname -s)"
      ;;
    *)
      fail "unsupported platform: $(uname -s)"
      ;;
  esac
}

detect_python() {
  PYTHON3_PATH="$(command -v python3 2>/dev/null || true)"
  if [ -z "$PYTHON3_PATH" ]; then
    fail "python3 not found"
  fi
}

parse_cli_args() {
  while [ $# -gt 0 ]; do
    case "$1" in
      --state-root)
        STATE_ROOT="${2:-}"
        shift 2
        ;;
      --purge)
        PURGE=1
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        fail "unknown argument: $1"
        ;;
    esac
  done
}

build_hook_commands() {
  local hook_bin wrapper_bin quoted_state_root
  hook_bin="$(shell_quote "${STATE_ROOT}/bin/codex-s3-archive-hook-stop")"
  wrapper_bin="$(shell_quote "${STATE_ROOT}/bin/stop-wrapper.sh")"
  quoted_state_root="$(shell_quote "${STATE_ROOT}")"

  ARCHIVE_HOOK_COMMAND="${hook_bin} --state-root ${quoted_state_root}"
  WRAPPER_COMMAND="${wrapper_bin} --state-root ${quoted_state_root}"
}

nohup_supervisor_pid_path() {
  printf '%s\n' "${STATE_ROOT}/daemon-supervisor.pid"
}

stop_nohup_supervisor() {
  local pid_path pid
  pid_path="$(nohup_supervisor_pid_path)"
  if [ ! -f "$pid_path" ]; then
    return 0
  fi

  pid="$(cat "$pid_path" 2>/dev/null || true)"
  if [ -n "$pid" ] && nohup_supervisor_pid_matches "$pid"; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$pid_path"
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

stop_and_remove_service() {
  if [ "$PLATFORM" = "Darwin" ]; then
    launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
    rm -f "$PLIST_PATH"
  else
    systemctl --user disable --now "${SERVICE_NAME}" >/dev/null 2>&1 || true
    stop_nohup_supervisor
    rm -f "$SYSTEMD_UNIT_PATH"
    systemctl --user daemon-reload >/dev/null 2>&1 || true
  fi
}

restore_hooks_json() {
  local latest_backup=""
  local candidate
  for candidate in "${HOME}"/.codex/hooks.json.bak.*; do
    if [ -f "$candidate" ]; then
      latest_backup="$candidate"
    fi
  done

  if [ -n "$latest_backup" ]; then
    cp "$latest_backup" "$HOOKS_JSON"
    HOOKS_RESTORE_SUMMARY="restored from $(basename "$latest_backup")"
    return 0
  fi

  if [ ! -f "$HOOKS_JSON" ]; then
    HOOKS_RESTORE_SUMMARY="hooks.json missing"
    return 0
  fi

  "${PYTHON3_PATH}" -c '
import json
import pathlib
import sys

hooks_path = pathlib.Path(sys.argv[1])
archive_command = sys.argv[2]
wrapper_command = sys.argv[3]
legacy_archive_command = sys.argv[4]
legacy_wrapper_command = sys.argv[5]

data = json.loads(hooks_path.read_text(encoding="utf-8"))
hooks_root = data.get("hooks", {})
stop_entries = hooks_root.get("Stop", [])
for entry in stop_entries:
    hooks = entry.get("hooks", [])
    if isinstance(hooks, list):
        entry["hooks"] = [
            hook for hook in hooks
            if not (
                isinstance(hook, dict)
                and hook.get("type") == "command"
                and hook.get("command") in {
                    archive_command,
                    wrapper_command,
                    legacy_archive_command,
                    legacy_wrapper_command,
                }
            )
        ]

hooks_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
' "$HOOKS_JSON" "$ARCHIVE_HOOK_COMMAND" "$WRAPPER_COMMAND" "$LEGACY_ARCHIVE_HOOK_COMMAND" "$LEGACY_WRAPPER_COMMAND"
  HOOKS_RESTORE_SUMMARY="removed archive hook entry"
}

remove_bin_contents() {
  rm -f \
    "${STATE_ROOT}/bin/codex-s3-archive-daemon" \
    "${STATE_ROOT}/bin/codex-s3-archive-hook-stop" \
    "${STATE_ROOT}/bin/stop-wrapper.sh" \
    "${STATE_ROOT}/bin/codex-s3-archive-supervisor.sh" \
    "${STATE_ROOT}/original-stop-hook.sh"
}

move_state_root() {
  if [ -d "$STATE_ROOT" ]; then
    DISABLED_PATH="${STATE_ROOT}.disabled.$(date +%s)"
    mv "$STATE_ROOT" "$DISABLED_PATH"
  fi
}

purge_disabled_backups() {
  if [ "$PURGE" -ne 1 ]; then
    return 0
  fi

  local path
  for path in "${HOME}"/.codex/s3-archive.disabled.*; do
    if [ -e "$path" ]; then
      rm -rf "$path"
    fi
  done
}

print_summary() {
  cat <<EOF
Codex S3 Archive uninstalled
  Service removed: yes
  Hooks:           ${HOOKS_RESTORE_SUMMARY}
  Disabled state:  ${DISABLED_PATH:-none}
  Purge:           $( [ "$PURGE" -eq 1 ] && echo yes || echo no )
EOF
}

main() {
  detect_platform
  detect_python
  parse_cli_args "$@"
  build_hook_commands
  stop_and_remove_service
  restore_hooks_json
  remove_bin_contents
  move_state_root
  purge_disabled_backups
  print_summary
}

main "$@"
