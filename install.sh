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
EXISTING_USER_ID=""
EXISTING_PROVIDER=""
EXISTING_BUCKET=""
EXISTING_MACHINE_ID=""
EXISTING_AWS_REGION=""
EXISTING_R2_ACCOUNT_ID=""
EXISTING_ACCESS_KEY_ID=""
EXISTING_SECRET_ACCESS_KEY=""
HAS_EXISTING_INSTALL=0

CLI_USER_ID=0
CLI_PROVIDER=0
CLI_BUCKET=0
CLI_MACHINE_ID=0
CLI_AWS_REGION=0
CLI_R2_ACCOUNT_ID=0
CLI_ACCESS_KEY_ID=0
CLI_SECRET_ACCESS_KEY=0

HOOKS_BACKUP_PATH=""
SMOKE_TEST_STATUS="not_run"
SERVICE_SUMMARY="unknown"
SMOKE_STARTUP_TIMEOUT_SECS=60

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
        CLI_USER_ID=1
        shift 2
        ;;
      --provider)
        PROVIDER="${2:-}"
        CLI_PROVIDER=1
        shift 2
        ;;
      --bucket)
        BUCKET="${2:-}"
        CLI_BUCKET=1
        shift 2
        ;;
      --machine-id)
        MACHINE_ID="${2:-}"
        CLI_MACHINE_ID=1
        shift 2
        ;;
      --region)
        AWS_REGION="${2:-}"
        CLI_AWS_REGION=1
        shift 2
        ;;
      --r2-account-id)
        R2_ACCOUNT_ID="${2:-}"
        CLI_R2_ACCOUNT_ID=1
        shift 2
        ;;
      --access-key-id)
        ACCESS_KEY_ID="${2:-}"
        CLI_ACCESS_KEY_ID=1
        shift 2
        ;;
      --secret-access-key)
        SECRET_ACCESS_KEY="${2:-}"
        CLI_SECRET_ACCESS_KEY=1
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

prompt_yes_no() {
  local var_name="$1"
  local prompt_label="$2"
  local default_answer="${3:-y}"

  ensure_tty

  local suffix="[y/N]"
  case "$default_answer" in
    y|Y)
      suffix="[Y/n]"
      ;;
  esac

  local reply=""
  while :; do
    IFS= read -r -p "$prompt_label $suffix: " reply </dev/tty || true
    if [ -z "$reply" ]; then
      reply="$default_answer"
    fi
    case "$reply" in
      y|Y|yes|YES)
        printf -v "$var_name" '%s' "1"
        return 0
        ;;
      n|N|no|NO)
        printf -v "$var_name" '%s' "0"
        return 0
        ;;
    esac
  done
}

prompt_value() {
  local var_name="$1"
  local prompt_label="$2"
  local default_value="$3"
  local secret="${4:-0}"
  local display_default="${5:-$default_value}"
  local current_value
  current_value="$(eval "printf '%s' \"\${$var_name:-}\"")"
  if [ -n "$current_value" ]; then
    return 0
  fi

  ensure_tty

  local prompt_text="$prompt_label"
  if [ -n "$display_default" ]; then
    prompt_text="$prompt_text [$display_default]"
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

load_existing_install_state() {
  local config_path="${STATE_ROOT}/config.json"
  local credentials_path="${STATE_ROOT}/credentials.json"
  if [ ! -f "$config_path" ] && [ ! -f "$credentials_path" ]; then
    return 0
  fi

  HAS_EXISTING_INSTALL=1

  local existing_output=""
  existing_output="$("${PYTHON3_PATH}" -c '
import json
import pathlib
import sys
from urllib.parse import urlparse

state_root = pathlib.Path(sys.argv[1])
config_path = state_root / "config.json"
creds_path = state_root / "credentials.json"

values = {
    "user_id": "",
    "provider": "",
    "bucket": "",
    "machine_id": "",
    "aws_region": "",
    "r2_account_id": "",
    "access_key_id": "",
    "secret_access_key": "",
}

try:
    if config_path.exists():
        config = json.loads(config_path.read_text(encoding="utf-8"))
        values["user_id"] = str(config.get("user_id") or "")
        values["provider"] = str(config.get("provider") or "")
        values["bucket"] = str(config.get("bucket") or "")
        values["machine_id"] = str(config.get("machine_id") or "")
        values["aws_region"] = str(config.get("region") or "")
        endpoint = str(config.get("endpoint_url") or "")
        if endpoint:
            hostname = urlparse(endpoint).hostname or ""
            if hostname.endswith(".r2.cloudflarestorage.com"):
                values["r2_account_id"] = hostname[: -len(".r2.cloudflarestorage.com")]
    if creds_path.exists():
        creds = json.loads(creds_path.read_text(encoding="utf-8"))
        values["access_key_id"] = str(creds.get("aws_access_key_id") or "")
        values["secret_access_key"] = str(creds.get("aws_secret_access_key") or "")
except Exception:
    pass

for key in (
    "user_id",
    "provider",
    "bucket",
    "machine_id",
    "aws_region",
    "r2_account_id",
    "access_key_id",
    "secret_access_key",
):
    print(values[key])
' "$STATE_ROOT")"

  local lines=()
  while IFS= read -r line; do
    lines+=("$line")
  done <<<"$existing_output"

  EXISTING_USER_ID="${lines[0]:-}"
  EXISTING_PROVIDER="${lines[1]:-}"
  EXISTING_BUCKET="${lines[2]:-}"
  EXISTING_MACHINE_ID="${lines[3]:-}"
  EXISTING_AWS_REGION="${lines[4]:-}"
  EXISTING_R2_ACCOUNT_ID="${lines[5]:-}"
  EXISTING_ACCESS_KEY_ID="${lines[6]:-}"
  EXISTING_SECRET_ACCESS_KEY="${lines[7]:-}"
}

clear_runtime_state() {
  local target_root="${1:-$STATE_ROOT}"
  rm -f \
    "${target_root}/heartbeat.json" \
    "${target_root}/daemon.pid" \
    "${target_root}/daemon-supervisor.pid"
  rm -f "${target_root}/staging"/* 2>/dev/null || true
  rm -f "${target_root}/queue"/*.json 2>/dev/null || true
  rm -f "${target_root}/queue/dead"/*.json 2>/dev/null || true
}

interactive_prompts() {
  local hostname_short
  hostname_short="$(hostname -s 2>/dev/null || hostname)"

  prompt_value USER_ID "User ID" "${EXISTING_USER_ID}"
  prompt_value PROVIDER "Storage Provider (aws or r2)" "${EXISTING_PROVIDER:-r2}"

  case "$PROVIDER" in
    aws|r2)
      ;;
    *)
      fail "provider must be aws or r2"
      ;;
  esac

  prompt_value BUCKET "Bucket" "${EXISTING_BUCKET:-mach-zero-codex}"
  prompt_value MACHINE_ID "Machine ID" "${EXISTING_MACHINE_ID:-$hostname_short}"

  if [ "$PROVIDER" = "aws" ]; then
    prompt_value AWS_REGION "AWS Region" "${EXISTING_AWS_REGION:-us-west-2}"
    prompt_value ACCESS_KEY_ID "AWS Access Key ID" "${EXISTING_ACCESS_KEY_ID}"
    prompt_value SECRET_ACCESS_KEY "AWS Secret Access Key" "${EXISTING_SECRET_ACCESS_KEY}" 1 "${EXISTING_SECRET_ACCESS_KEY:+saved}"
    R2_ACCOUNT_ID=""
  else
    prompt_value R2_ACCOUNT_ID "R2 Account ID" "${EXISTING_R2_ACCOUNT_ID}"
    prompt_value ACCESS_KEY_ID "R2 Access Key ID" "${EXISTING_ACCESS_KEY_ID}"
    prompt_value SECRET_ACCESS_KEY "R2 Secret Access Key" "${EXISTING_SECRET_ACCESS_KEY}" 1 "${EXISTING_SECRET_ACCESS_KEY:+saved}"
    AWS_REGION=""
  fi
}

create_state_dirs() {
  local target_root="${1:-$STATE_ROOT}"
  mkdir -p \
    "${target_root}/bin" \
    "${target_root}/queue/dead" \
    "${target_root}/staging" \
    "${target_root}/checkpoints" \
    "${target_root}/logs"
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
  local target_root="${1:-$STATE_ROOT}"
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
' "${target_root}/credentials.json" "$ACCESS_KEY_ID" "$SECRET_ACCESS_KEY"
  chmod 600 "${target_root}/credentials.json"
}

write_config_json() {
  local target_root="${1:-$STATE_ROOT}"
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
' "${target_root}/config.json" "$USER_ID" "$PROVIDER" "$BUCKET" "$MACHINE_ID" "$target_root" "$AWS_REGION" "$endpoint_url" "$region_name"
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
      case "$SYSTEMD_CHECK_ERROR" in
        container\ environment\ detected*)
          SERVICE_SUMMARY="running (container nohup fallback)"
          ;;
        *)
          SERVICE_SUMMARY="running (nohup fallback)"
          ;;
      esac
    fi
  fi
}

warmup_daemon_runtime() {
  if ! "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-daemon" --state-root "${STATE_ROOT}" --config "${STATE_ROOT}/config.json" --once >>"${STATE_ROOT}/logs/daemon.stdout.log" 2>>"${STATE_ROOT}/logs/daemon.stderr.log"; then
    print_smoke_diagnostics
    fail "daemon runtime warmup failed"
  fi
}

service_is_running() {
  if [ "$PLATFORM" = "Darwin" ]; then
    launchctl list "${SERVICE_LABEL}" >/dev/null 2>&1
  else
    if [ "$(systemctl --user is-active "${SERVICE_NAME}" 2>/dev/null || true)" = "active" ]; then
      return 0
    fi
    nohup_supervisor_is_running || daemon_is_running
  fi
}

linux_systemd_user_available() {
  if [ "$PLATFORM" != "Linux" ]; then
    return 1
  fi

  if linux_container_environment; then
    SYSTEMD_CHECK_ERROR="container environment detected; skipping systemd --user"
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

linux_container_environment() {
  if [ "$PLATFORM" != "Linux" ]; then
    return 1
  fi

  if [ -f "/.dockerenv" ] || [ -f "/run/.containerenv" ]; then
    return 0
  fi

  if [ -n "${container:-}" ]; then
    return 0
  fi

  grep -qaE '/(docker|kubepods|containerd|podman|lxc)/' /proc/1/cgroup 2>/dev/null
}

nohup_supervisor_pid_path() {
  printf '%s\n' "${STATE_ROOT}/daemon-supervisor.pid"
}

daemon_pid_path() {
  printf '%s\n' "${STATE_ROOT}/daemon.pid"
}

nohup_supervisor_path() {
  printf '%s\n' "${STATE_ROOT}/bin/codex-s3-archive-supervisor.sh"
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

daemon_is_running() {
  local pid
  pid="$(find_daemon_pid)"
  if [ -z "$pid" ]; then
    return 1
  fi
  kill -0 "$pid" >/dev/null 2>&1 && daemon_process_matches "$pid"
}

wait_for_pid_exit() {
  local pid="$1"
  local attempts=0
  while kill -0 "$pid" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 20 ]; then
      kill -9 "$pid" >/dev/null 2>&1 || true
      break
    fi
    sleep 0.25
  done
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
set -uo pipefail

while true; do
  "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-daemon" --state-root "${STATE_ROOT}" --config "${STATE_ROOT}/config.json" >>"${STATE_ROOT}/logs/daemon.stdout.log" 2>>"${STATE_ROOT}/logs/daemon.stderr.log"
  daemon_status="\$?"
  if [ "\$daemon_status" -ne 0 ]; then
    printf '%s WARN supervisor observed daemon exit %s\n' "\$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "\$daemon_status" >>"${STATE_ROOT}/logs/daemon.stderr.log"
  fi
  sleep 2
done
EOF
  chmod +x "$supervisor_path"
}

stop_nohup_daemon() {
  local pid
  pid="$(find_daemon_pid)"
  if [ -n "$pid" ]; then
    kill "$pid" >/dev/null 2>&1 || true
    wait_for_pid_exit "$pid"
  fi
  rm -f "$(daemon_pid_path)"
}

stop_nohup_supervisor() {
  local pid_path pid
  pid_path="$(nohup_supervisor_pid_path)"
  if [ ! -f "$pid_path" ]; then
    stop_nohup_daemon
    return 0
  fi

  pid="$(cat "$pid_path" 2>/dev/null || true)"
  if [ -n "$pid" ] && nohup_supervisor_pid_matches "$pid"; then
    kill "$pid" >/dev/null 2>&1 || true
    wait_for_pid_exit "$pid"
  fi
  rm -f "$pid_path"
  stop_nohup_daemon
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

print_signature_mismatch_hint() {
  local diagnostics_root="${1:-$STATE_ROOT}"
  local signature_detected=0
  local log_file

  if [ -f "${diagnostics_root}/heartbeat.json" ] && grep -q 'SignatureDoesNotMatch' "${diagnostics_root}/heartbeat.json" 2>/dev/null; then
    signature_detected=1
  fi

  if [ "$signature_detected" -eq 0 ]; then
    for log_file in "${diagnostics_root}/logs/daemon.stdout.log" "${diagnostics_root}/logs/daemon.stderr.log" "${diagnostics_root}/logs/hook-stop.log"; do
      if [ -f "$log_file" ] && grep -q 'SignatureDoesNotMatch' "$log_file" 2>/dev/null; then
        signature_detected=1
        break
      fi
    done
  fi

  if [ "$signature_detected" -eq 0 ]; then
    return 0
  fi

  echo "Likely storage signature mismatch:" >&2
  if [ "$PROVIDER" = "r2" ]; then
    cat <<EOF >&2
- verify Access Key ID / Secret Access Key come from an R2 S3 API token
- verify the R2 account ID matches the endpoint URL
- verify the bucket belongs to the same Cloudflare account as the credentials
EOF
  else
    cat <<EOF >&2
- verify the access key ID / secret access key pair is correct
- verify the configured region matches the bucket's region
EOF
  fi
}

print_smoke_diagnostics() {
  local diagnostics_root="${1:-$STATE_ROOT}"
  echo "Smoke test diagnostics:" >&2
  if [ -f "${diagnostics_root}/heartbeat.json" ]; then
    echo "--- heartbeat.json ---" >&2
    cat "${diagnostics_root}/heartbeat.json" >&2
  else
    echo "heartbeat.json missing" >&2
  fi

  local log_file
  for log_file in "${diagnostics_root}/logs/daemon.stdout.log" "${diagnostics_root}/logs/daemon.stderr.log" "${diagnostics_root}/logs/hook-stop.log"; do
    if [ -f "$log_file" ]; then
      echo "--- tail ${log_file} ---" >&2
      tail -n 50 "$log_file" >&2 || true
    fi
  done

  print_signature_mismatch_hint "$diagnostics_root"
}

smoke_test() {
  local age=""
  local deadline=$((SECONDS + SMOKE_STARTUP_TIMEOUT_SECS))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if service_is_running; then
      age="$(heartbeat_age_secs)"
      if [ "$age" -ge 0 ] && [ "$age" -le 30 ]; then
        break
      fi
    fi
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

  local smoke_state_root
  smoke_state_root="$(mktemp -d "${TMPDIR:-/tmp}/codex-s3-archive-smoke.XXXXXX")"
  create_state_dirs "$smoke_state_root"
  write_credentials_json "$smoke_state_root"
  write_config_json "$smoke_state_root"

  local smoke_transcript="${smoke_state_root}/smoke-test-$(date +%s).jsonl"
  printf '{"smoke":true,"ts":"%s"}\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" >"$smoke_transcript"

  local payload
  payload="$("${PYTHON3_PATH}" -c 'import json, sys; print(json.dumps({"session_id": "smoke-test", "turn_id": "turn-smoke", "transcript_path": sys.argv[1], "cwd": sys.argv[2], "model": "smoke-test", "hook_event_name": "Stop"}))' "$smoke_transcript" "$PWD")"
  printf '%s' "$payload" | "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-hook-stop" --state-root "${smoke_state_root}"

  if ! "${UV_PATH}" run --script "${STATE_ROOT}/bin/codex-s3-archive-daemon" --state-root "${smoke_state_root}" --config "${smoke_state_root}/config.json" --once; then
    print_smoke_diagnostics "$smoke_state_root"
    echo "Smoke test state preserved at: ${smoke_state_root}" >&2
    fail "smoke test upload did not succeed"
  fi

  local success_key=""
  if [ -f "${smoke_state_root}/heartbeat.json" ]; then
    success_key="$("${PYTHON3_PATH}" -c 'import json, pathlib, sys; data=json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")); print(data.get("last_success_s3_key") or "")' "${smoke_state_root}/heartbeat.json")"
  fi

  if [ -z "$success_key" ]; then
    print_smoke_diagnostics "$smoke_state_root"
    echo "Smoke test state preserved at: ${smoke_state_root}" >&2
    fail "smoke test upload did not succeed"
  fi

  rm -rf "$smoke_state_root"
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
  load_existing_install_state

  [ "$CLI_USER_ID" -eq 1 ] || USER_ID=""
  [ "$CLI_PROVIDER" -eq 1 ] || PROVIDER=""
  [ "$CLI_BUCKET" -eq 1 ] || BUCKET=""
  [ "$CLI_MACHINE_ID" -eq 1 ] || MACHINE_ID=""
  [ "$CLI_AWS_REGION" -eq 1 ] || AWS_REGION=""
  [ "$CLI_R2_ACCOUNT_ID" -eq 1 ] || R2_ACCOUNT_ID=""
  [ "$CLI_ACCESS_KEY_ID" -eq 1 ] || ACCESS_KEY_ID=""
  [ "$CLI_SECRET_ACCESS_KEY" -eq 1 ] || SECRET_ACCESS_KEY=""

  if [ "$HAS_EXISTING_INSTALL" -eq 1 ]; then
    local should_clear_runtime_state=0
    prompt_yes_no should_clear_runtime_state "Existing install detected. Clear pending queue and transient runtime state before reinstall?" "y"
    if [ "$should_clear_runtime_state" -eq 1 ]; then
      clear_runtime_state
    fi
  fi

  build_hook_commands
  interactive_prompts
  create_state_dirs
  download_scripts
  write_credentials_json
  write_config_json
  merge_hooks_json
  warmup_daemon_runtime
  install_service
  smoke_test
  print_summary
}

main "$@"
