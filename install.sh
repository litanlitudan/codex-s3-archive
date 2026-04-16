#!/bin/bash
set -euo pipefail

GIST_BASE="__GIST_BASE_PLACEHOLDER__"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

STATE_ROOT="${HOME}/.codex/s3-archive"
HOOKS_JSON="${HOME}/.codex/hooks.json"
PLIST_PATH="${HOME}/Library/LaunchAgents/com.codex.s3-archive.plist"
SYSTEMD_UNIT_PATH="${HOME}/.config/systemd/user/codex-s3-archive.service"
SERVICE_LABEL="com.codex.s3-archive"
SERVICE_NAME="codex-s3-archive"

PLATFORM=""
UV_PATH=""
PYTHON3_PATH=""
SYSTEMD_CHECK_ERROR=""

USER_ID=""
PROVIDER=""
BUCKET=""
MACHINE_ID=""
AWS_REGION=""
R2_ACCOUNT_ID=""
ACCESS_KEY_ID=""
SECRET_ACCESS_KEY=""

HOOKS_BACKUP_PATH=""
SMOKE_TEST_STATUS="not_run"
SERVICE_SUMMARY="unknown"

ARCHIVE_HOOK_COMMAND=""
WRAPPER_COMMAND=""
LEGACY_ARCHIVE_HOOK_COMMAND='$HOME/.codex/s3-archive/bin/codex-s3-archive-hook-stop --state-root $HOME/.codex/s3-archive'
LEGACY_WRAPPER_COMMAND='$HOME/.codex/s3-archive/bin/stop-wrapper.sh'

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "required command not found: $1"
  fi
}

shell_quote() {
  printf '%q' "$1"
}

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --user-id VALUE
  --provider aws|r2
  --bucket VALUE
  --machine-id VALUE
  --region VALUE
  --r2-account-id VALUE
  --access-key-id VALUE
  --secret-access-key VALUE
  --gist-base URL
  --state-root PATH
  --help
EOF
}

detect_platform() {
  local uname_s
  uname_s="$(uname -s)"
  case "$uname_s" in
    Darwin|Linux)
      PLATFORM="$uname_s"
      ;;
    *)
      fail "unsupported platform: $uname_s"
      ;;
  esac
}

detect_uv() {
  if [ -x "${HOME}/.local/bin/uv" ]; then
    UV_PATH="${HOME}/.local/bin/uv"
  else
    UV_PATH="$(command -v uv 2>/dev/null || true)"
  fi

  if [ -z "$UV_PATH" ]; then
    fail "uv not found. Install uv first: https://docs.astral.sh/uv/getting-started/installation/"
  fi
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
      --user-id)
        USER_ID="${2:-}"
        shift 2
        ;;
      --provider)
        PROVIDER="${2:-}"
        shift 2
        ;;
      --bucket)
        BUCKET="${2:-}"
        shift 2
        ;;
      --machine-id)
        MACHINE_ID="${2:-}"
        shift 2
        ;;
      --region)
        AWS_REGION="${2:-}"
        shift 2
        ;;
      --r2-account-id)
        R2_ACCOUNT_ID="${2:-}"
        shift 2
        ;;
      --access-key-id)
        ACCESS_KEY_ID="${2:-}"
        shift 2
        ;;
      --secret-access-key)
        SECRET_ACCESS_KEY="${2:-}"
        shift 2
        ;;
      --gist-base)
        GIST_BASE="${2:-}"
        shift 2
        ;;
      --state-root)
        STATE_ROOT="${2:-}"
        shift 2
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

ensure_tty() {
  if [ ! -r /dev/tty ]; then
    fail "interactive input required for missing arguments, but /dev/tty is unavailable"
  fi
}

prompt_value() {
  local var_name="$1"
  local prompt_label="$2"
  local default_value="$3"
  local secret="${4:-0}"
  local current_value
  current_value="$(eval "printf '%s' \"\${$var_name:-}\"")"
  if [ -n "$current_value" ]; then
    return 0
  fi

  ensure_tty

  local prompt_text="$prompt_label"
  if [ -n "$default_value" ]; then
    prompt_text="$prompt_text [$default_value]"
  fi
  prompt_text="$prompt_text: "

  local reply=""
  while :; do
    if [ "$secret" = "1" ]; then
      IFS= read -r -s -p "$prompt_text" reply </dev/tty || true
      printf '\n' >/dev/tty
    else
      IFS= read -r -p "$prompt_text" reply </dev/tty || true
    fi
    if [ -z "$reply" ]; then
      reply="$default_value"
    fi
    if [ -n "$reply" ]; then
      printf -v "$var_name" '%s' "$reply"
      return 0
    fi
  done
}

interactive_prompts() {
  local hostname_short
  hostname_short="$(hostname -s 2>/dev/null || hostname)"

  prompt_value USER_ID "User ID" ""
  prompt_value PROVIDER "Storage Provider (aws or r2)" "r2"

  case "$PROVIDER" in
    aws|r2)
      ;;
    *)
      fail "provider must be aws or r2"
      ;;
  esac

  prompt_value BUCKET "Bucket" "mach-zero-codex"
  prompt_value MACHINE_ID "Machine ID" "$hostname_short"

  if [ "$PROVIDER" = "aws" ]; then
    prompt_value AWS_REGION "AWS Region" "us-west-2"
    prompt_value ACCESS_KEY_ID "AWS Access Key ID" ""
    prompt_value SECRET_ACCESS_KEY "AWS Secret Access Key" "" 1
  else
    prompt_value R2_ACCOUNT_ID "R2 Account ID" ""
    prompt_value ACCESS_KEY_ID "R2 Access Key ID" ""
    prompt_value SECRET_ACCESS_KEY "R2 Secret Access Key" "" 1
  fi
}

create_state_dirs() {
  mkdir -p \
    "${STATE_ROOT}/bin" \
    "${STATE_ROOT}/queue/dead" \
    "${STATE_ROOT}/staging" \
    "${STATE_ROOT}/checkpoints" \
    "${STATE_ROOT}/logs"
}

download_scripts() {
  local filename target

  for filename in codex-s3-archive-hook-stop codex-s3-archive-daemon stop-wrapper.sh; do
    target="${STATE_ROOT}/bin/${filename}"
    download_or_copy_file "$filename" "$target"
    chmod +x "$target"
  done
}

download_or_copy_file() {
  local filename="$1"
  local dest="$2"
  local download_base="${GIST_BASE%/}"

  if [ -n "$GIST_BASE" ] && [ "$GIST_BASE" != "__GIST_BASE_PLACEHOLDER__" ]; then
    require_command curl
    curl -fsSL "${download_base}/${filename}" -o "$dest"
  elif [ -f "${SCRIPT_DIR}/${filename}" ]; then
    cp "${SCRIPT_DIR}/${filename}" "$dest"
  else
    echo "ERROR: Cannot find ${filename}" >&2
    echo "  Either pass --gist-base <url> to download from a gist," >&2
    echo "  or run install.sh from the repo directory where the companion files live." >&2
    exit 1
  fi
}

write_credentials_json() {
  "${PYTHON3_PATH}" -c '
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
payload = {
    "aws_access_key_id": sys.argv[2],
    "aws_secret_access_key": sys.argv[3],
}
path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
' "${STATE_ROOT}/credentials.json" "$ACCESS_KEY_ID" "$SECRET_ACCESS_KEY"
  chmod 600 "${STATE_ROOT}/credentials.json"
}

write_config_json() {
  local endpoint_url=""
  local region_name=""
  if [ "$PROVIDER" = "r2" ]; then
    endpoint_url="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
    region_name="auto"
  fi

  "${PYTHON3_PATH}" -c '
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
config = {
    "user_id": sys.argv[2],
    "provider": sys.argv[3],
    "bucket": sys.argv[4],
    "prefix": "raw",
    "machine_id": sys.argv[5],
    "state_root": sys.argv[6],
    "max_raw_chunk_bytes": 1048576,
}
provider = sys.argv[3]
region = sys.argv[7]
endpoint_url = sys.argv[8]
region_name = sys.argv[9]
if provider == "aws":
    config["region"] = region
else:
    config["endpoint_url"] = endpoint_url
    config["region_name"] = region_name
path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
' "${STATE_ROOT}/config.json" "$USER_ID" "$PROVIDER" "$BUCKET" "$MACHINE_ID" "$STATE_ROOT" "$AWS_REGION" "$endpoint_url" "$region_name"
}

backup_hooks_json() {
  local timestamp
  timestamp="$(date +%s)"
  mkdir -p "${HOME}/.codex"
  if [ -f "$HOOKS_JSON" ]; then
    HOOKS_BACKUP_PATH="${HOME}/.codex/hooks.json.bak.${timestamp}"
    cp "$HOOKS_JSON" "$HOOKS_BACKUP_PATH"
  fi
}

merge_hooks_json() {
  backup_hooks_json
  "${PYTHON3_PATH}" -c '
import json
import os
import pathlib
import shlex
import sys

hooks_path = pathlib.Path(sys.argv[1])
state_root = pathlib.Path(sys.argv[2])
archive_command = sys.argv[3]
wrapper_command = sys.argv[4]
legacy_archive_command = sys.argv[5]
legacy_wrapper_command = sys.argv[6]

if hooks_path.exists():
    data = json.loads(hooks_path.read_text(encoding="utf-8"))
else:
    data = {"hooks": {}}

hooks_root = data.setdefault("hooks", {})
stop_entries = hooks_root.setdefault("Stop", [{"hooks": []}])
if not stop_entries:
    stop_entries.append({"hooks": []})

for entry in stop_entries:
    entry.setdefault("hooks", [])

all_hooks = []
for entry in stop_entries:
    hooks = entry.get("hooks", [])
    if isinstance(hooks, list):
        all_hooks.extend(hooks)

managed_commands = {
    archive_command,
    wrapper_command,
    legacy_archive_command,
    legacy_wrapper_command,
}
command_hooks = [
    hook for hook in all_hooks
    if isinstance(hook, dict) and hook.get("type") == "command" and hook.get("command")
]
unmanaged = [hook for hook in command_hooks if hook.get("command") not in managed_commands]

original_hook_path = state_root / "original-stop-hook.sh"

if unmanaged:
    lines = [
        "#!/bin/bash",
        "PAYLOAD_FILE=$(mktemp \"${TMPDIR:-/tmp}/codex-stop-original.XXXXXX\")",
        "trap '\''rm -f \"$PAYLOAD_FILE\"'\'' EXIT",
        "cat >\"$PAYLOAD_FILE\"",
        "status=0",
    ]
    for hook in unmanaged:
        cmd = hook["command"]
        lines.append("bash -lc {} <\"$PAYLOAD_FILE\" || status=$?".format(shlex.quote(cmd)))
    lines.append("exit $status")
    original_hook_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(original_hook_path, 0o755)
    replacement_command = wrapper_command
else:
    if original_hook_path.exists():
        original_hook_path.unlink()
    replacement_command = archive_command

replacement_hook = {
    "type": "command",
    "timeout": 5,
    "command": replacement_command,
}
stop_entries[0]["hooks"] = [replacement_hook]
for entry in stop_entries[1:]:
    entry["hooks"] = []

hooks_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
' "$HOOKS_JSON" "$STATE_ROOT" "$ARCHIVE_HOOK_COMMAND" "$WRAPPER_COMMAND" "$LEGACY_ARCHIVE_HOOK_COMMAND" "$LEGACY_WRAPPER_COMMAND"
}

install_service() {
  if [ "$PLATFORM" = "Darwin" ]; then
    mkdir -p "$(dirname "$PLIST_PATH")"
    cat >"$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${SERVICE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
      <string>${UV_PATH}</string>
      <string>run</string>
      <string>--script</string>
      <string>${STATE_ROOT}/bin/codex-s3-archive-daemon</string>
      <string>--state-root</string>
      <string>${STATE_ROOT}</string>
      <string>--config</string>
      <string>${STATE_ROOT}/config.json</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>WorkingDirectory</key>
    <string>${HOME}</string>
    <key>StandardOutPath</key>
    <string>${STATE_ROOT}/logs/daemon.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>${STATE_ROOT}/logs/daemon.stderr.log</string>
  </dict>
</plist>
EOF
    launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
    launchctl load "$PLIST_PATH"
    SERVICE_SUMMARY="running (${SERVICE_LABEL})"
  else
    mkdir -p "$(dirname "$SYSTEMD_UNIT_PATH")"
    cat >"$SYSTEMD_UNIT_PATH" <<EOF
[Unit]
Description=Codex S3 Archive Daemon
After=default.target

[Service]
Type=simple
ExecStart=${UV_PATH} run --script ${STATE_ROOT}/bin/codex-s3-archive-daemon --state-root ${STATE_ROOT} --config ${STATE_ROOT}/config.json
WorkingDirectory=${HOME}
Restart=always
RestartSec=2
StandardOutput=append:${STATE_ROOT}/logs/daemon.stdout.log
StandardError=append:${STATE_ROOT}/logs/daemon.stderr.log

[Install]
WantedBy=default.target
EOF
    if linux_systemd_user_available; then
      systemctl --user daemon-reload
      systemctl --user enable --now "${SERVICE_NAME}"
      SERVICE_SUMMARY="running (${SERVICE_NAME})"
    else
      install_nohup_supervisor
      SERVICE_SUMMARY="running (nohup fallback)"
    fi
  fi
}

service_is_running() {
  if [ "$PLATFORM" = "Darwin" ]; then
    launchctl list "${SERVICE_LABEL}" >/dev/null 2>&1
  else
    if [ "$(systemctl --user is-active "${SERVICE_NAME}" 2>/dev/null || true)" = "active" ]; then
      return 0
    fi
    nohup_supervisor_is_running
  fi
}

linux_systemd_user_available() {
  if [ "$PLATFORM" != "Linux" ]; then
    return 1
  fi

  if ! command -v systemctl >/dev/null 2>&1; then
    SYSTEMD_CHECK_ERROR="systemctl not found"
    return 1
  fi

  if ! SYSTEMD_CHECK_ERROR="$(systemctl --user show-environment 2>&1)"; then
    SYSTEMD_CHECK_ERROR="${SYSTEMD_CHECK_ERROR:-systemctl --user show-environment failed}"
    return 1
  fi

  SYSTEMD_CHECK_ERROR=""
  return 0
}

nohup_supervisor_pid_path() {
  printf '%s\n' "${STATE_ROOT}/daemon-supervisor.pid"
}

nohup_supervisor_path() {
  printf '%s\n' "${STATE_ROOT}/bin/codex-s3-archive-supervisor.sh"
}

nohup_supervisor_is_running() {
  local pid_path pid
  pid_path="$(nohup_supervisor_pid_path)"
  if [ ! -f "$pid_path" ]; then
    return 1
  fi

  pid="$(cat "$pid_path" 2>/dev/null || true)"
  if [ -z "$pid" ]; then
    return 1
  fi

  kill -0 "$pid" >/dev/null 2>&1 && nohup_supervisor_pid_matches "$pid"
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

write_nohup_supervisor() {
  local supervisor_path
  supervisor_path="$(nohup_supervisor_path)"

  cat >"$supervisor_path" <<EOF
#!/bin/bash
set -euo pipefail

while true; do
  "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-daemon" --state-root "${STATE_ROOT}" --config "${STATE_ROOT}/config.json" >>"${STATE_ROOT}/logs/daemon.stdout.log" 2>>"${STATE_ROOT}/logs/daemon.stderr.log"
  sleep 2
done
EOF
  chmod +x "$supervisor_path"
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

install_nohup_supervisor() {
  local pid_path supervisor_path
  pid_path="$(nohup_supervisor_pid_path)"
  supervisor_path="$(nohup_supervisor_path)"

  stop_nohup_supervisor
  write_nohup_supervisor
  nohup "$supervisor_path" >/dev/null 2>&1 </dev/null &
  printf '%s\n' "$!" >"$pid_path"

  if [ -n "$SYSTEMD_CHECK_ERROR" ]; then
    echo "WARN: systemd --user unavailable, using nohup fallback: ${SYSTEMD_CHECK_ERROR}" >&2
  fi
}

heartbeat_age_secs() {
  if [ ! -f "${STATE_ROOT}/heartbeat.json" ]; then
    echo "-1"
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
    age = int((now - dt).total_seconds())
    print(max(age, 0))
except Exception:
    print(-1)
' "${STATE_ROOT}/heartbeat.json"
}

print_smoke_diagnostics() {
  echo "Smoke test diagnostics:" >&2
  if [ -f "${STATE_ROOT}/heartbeat.json" ]; then
    echo "--- heartbeat.json ---" >&2
    cat "${STATE_ROOT}/heartbeat.json" >&2
  else
    echo "heartbeat.json missing" >&2
  fi

  local log_file
  for log_file in "${STATE_ROOT}/logs/daemon.stdout.log" "${STATE_ROOT}/logs/daemon.stderr.log" "${STATE_ROOT}/logs/hook-stop.log"; do
    if [ -f "$log_file" ]; then
      echo "--- tail ${log_file} ---" >&2
      tail -n 50 "$log_file" >&2 || true
    fi
  done
}

smoke_test() {
  sleep 3

  local age=""
  local attempts=0
  while [ "$attempts" -lt 10 ]; do
    if service_is_running; then
      age="$(heartbeat_age_secs)"
      if [ "$age" -ge 0 ] && [ "$age" -le 30 ]; then
        break
      fi
    fi
    attempts=$((attempts + 1))
    sleep 1
  done

  if ! service_is_running; then
    print_smoke_diagnostics
    fail "service failed to start"
  fi

  age="$(heartbeat_age_secs)"
  if [ "$age" -lt 0 ] || [ "$age" -gt 30 ]; then
    print_smoke_diagnostics
    fail "heartbeat.json was not written recently"
  fi

  local before_key=""
  if [ -f "${STATE_ROOT}/heartbeat.json" ]; then
    before_key="$("${PYTHON3_PATH}" -c 'import json, pathlib, sys; data=json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")); print(data.get("last_success_s3_key") or "")' "${STATE_ROOT}/heartbeat.json")"
  fi

  local smoke_transcript="${STATE_ROOT}/smoke-test-$(date +%s).jsonl"
  printf '{"smoke":true,"ts":"%s"}\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >"$smoke_transcript"

  local payload
  payload="$("${PYTHON3_PATH}" -c 'import json, sys; print(json.dumps({"session_id": "smoke-test", "turn_id": "turn-smoke", "transcript_path": sys.argv[1], "cwd": sys.argv[2], "model": "smoke-test", "hook_event_name": "Stop"}))' "$smoke_transcript" "$PWD")"
  printf '%s' "$payload" | "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-hook-stop" --state-root "${STATE_ROOT}"

  attempts=0
  local success_key=""
  while [ "$attempts" -lt 10 ]; do
    if [ -f "${STATE_ROOT}/heartbeat.json" ]; then
      success_key="$("${PYTHON3_PATH}" -c 'import json, pathlib, sys; data=json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")); print(data.get("last_success_s3_key") or "")' "${STATE_ROOT}/heartbeat.json")"
      if [ -n "$success_key" ] && [ "$success_key" != "$before_key" ]; then
        break
      fi
    fi
    attempts=$((attempts + 1))
    sleep 1
  done

  rm -f "$smoke_transcript"

  if [ -z "$success_key" ] || [ "$success_key" = "$before_key" ]; then
    print_smoke_diagnostics
    fail "smoke test upload did not succeed"
  fi

  SMOKE_TEST_STATUS="passed"
}

print_summary() {
  cat <<EOF
OK Codex S3 Archive installed
  State root:  ${STATE_ROOT}
  Provider:    ${PROVIDER}
  Bucket:      ${BUCKET}
  User ID:     ${USER_ID}
  Service:     ${SERVICE_SUMMARY}
  Smoke test:  ${SMOKE_TEST_STATUS}
EOF
}

main() {
  detect_platform
  detect_uv
  detect_python
  parse_cli_args "$@"
  build_hook_commands
  interactive_prompts
  create_state_dirs
  download_scripts
  write_credentials_json
  write_config_json
  merge_hooks_json
  install_service
  smoke_test
  print_summary
}

main "$@"
