#!/bin/bash
set -euo pipefail

STATE_ROOT="${HOME}/.codex/s3-archive"
HOOK_COMMAND="${STATE_ROOT}/bin/codex-s3-archive-hook-stop"
LOG_PATH="${STATE_ROOT}/logs/hook-stop.log"

while [ $# -gt 0 ]; do
  case "$1" in
    --state-root)
      STATE_ROOT="${2:-}"
      shift 2
      ;;
    --help|-h)
      cat <<EOF
Usage: $0 [--state-root PATH]
EOF
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

append_log() {
  mkdir -p "$(dirname "$LOG_PATH")"
  printf '%s %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$1" >>"$LOG_PATH"
}

PAYLOAD_FILE=$(mktemp "${TMPDIR:-/tmp}/codex-stop.XXXXXX")
trap 'rm -f "$PAYLOAD_FILE"' EXIT
cat >"$PAYLOAD_FILE"

# Step 1: archive enqueue (fast, non-blocking)
if [ -x "$HOOK_COMMAND" ]; then
  if ! "$HOOK_COMMAND" --state-root "${STATE_ROOT}" <"$PAYLOAD_FILE"; then
    append_log "hook-stop-error wrapper_exec_failed"
  fi
else
  append_log "hook-stop-error wrapper_missing_hook_command"
fi

# Step 2: original Stop hook (if exists)
ORIGINAL_HOOK="${STATE_ROOT}/original-stop-hook.sh"
if [ -x "$ORIGINAL_HOOK" ]; then
  "$ORIGINAL_HOOK" <"$PAYLOAD_FILE"
fi
