#!/bin/bash
set -euo pipefail

STATE_ROOT="${HOME}/.codex/s3-archive"

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

if [ -x "${HOME}/.local/bin/uv" ]; then
  UV_PATH="${HOME}/.local/bin/uv"
else
  UV_PATH="$(command -v uv 2>/dev/null || true)"
fi

PAYLOAD_FILE=$(mktemp "${TMPDIR:-/tmp}/codex-stop.XXXXXX")
trap 'rm -f "$PAYLOAD_FILE"' EXIT
cat >"$PAYLOAD_FILE"

# Step 1: archive enqueue (fast, non-blocking)
if [ -n "${UV_PATH}" ]; then
  "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-hook-stop" \
    --state-root "${STATE_ROOT}" <"$PAYLOAD_FILE" || true
fi

# Step 2: original Stop hook (if exists)
ORIGINAL_HOOK="${STATE_ROOT}/original-stop-hook.sh"
if [ -x "$ORIGINAL_HOOK" ]; then
  "$ORIGINAL_HOOK" <"$PAYLOAD_FILE"
fi
