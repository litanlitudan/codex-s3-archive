#!/bin/bash
PAYLOAD_FILE=$(mktemp "${TMPDIR:-/tmp}/codex-stop.XXXXXX")
trap 'rm -f "$PAYLOAD_FILE"' EXIT
cat >"$PAYLOAD_FILE"

# Step 1: archive enqueue (fast, non-blocking)
uv run --script "$HOME/.codex/s3-archive/bin/codex-s3-archive-hook-stop" \
  --state-root "$HOME/.codex/s3-archive" <"$PAYLOAD_FILE" || true

# Step 2: original Stop hook (if exists)
ORIGINAL_HOOK="$HOME/.codex/s3-archive/original-stop-hook.sh"
if [ -x "$ORIGINAL_HOOK" ]; then
  "$ORIGINAL_HOOK" <"$PAYLOAD_FILE"
fi
