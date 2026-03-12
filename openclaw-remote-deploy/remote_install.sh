#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

if [[ -z "${OPENCLAW_DEPLOY_PAYLOAD_B64:-}" ]]; then
  echo '{"ok":false,"error":"OPENCLAW_DEPLOY_PAYLOAD_B64 is missing"}'
  exit 1
fi

if [[ "$(id -u)" -ne 0 ]] && ! command -v sudo >/dev/null 2>&1; then
  echo '{"ok":false,"error":"current SSH user is not root and sudo is not available"}'
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -y
    sudo apt-get install -y python3
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y python3
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y python3
  else
    echo '{"ok":false,"error":"python3 is required and no supported package manager is available"}'
    exit 1
  fi
fi

eval "$(
python3 - <<'PY'
import base64
import json
import os
import shlex

payload = json.loads(base64.b64decode(os.environ["OPENCLAW_DEPLOY_PAYLOAD_B64"]).decode("utf-8"))
install = payload["install"]
gateway = install["gateway"]

def emit(name, value):
    if isinstance(value, bool):
        v = "1" if value else "0"
    elif value is None:
        v = ""
    else:
        v = str(value)
    print(f"{name}={shlex.quote(v)}")

emit("OPENCLAW_NODE_MAJOR", install.get("node_version_major", 22))
emit("OPENCLAW_PACKAGE", install.get("openclaw_package", "openclaw@latest"))
emit("OPENCLAW_WORKSPACE", install["workspace"])
emit("OPENCLAW_GATEWAY_PORT", gateway.get("port", 18789))
emit("OPENCLAW_GATEWAY_BIND", gateway.get("bind", "lan"))
emit("OPENCLAW_GATEWAY_AUTH_MODE", gateway.get("auth_mode", "token"))
emit("OPENCLAW_GATEWAY_TOKEN", gateway.get("token", ""))
emit("OPENCLAW_GATEWAY_OPEN_FIREWALL", gateway.get("open_firewall", True))
emit("OPENCLAW_GATEWAY_INSTALL_SERVICE", gateway.get("install_service", True))
emit("OPENCLAW_GATEWAY_START_SERVICE", gateway.get("start_service", True))
emit("OPENCLAW_GATEWAY_ENABLE_LINGER", gateway.get("enable_linger", True))
PY
)"

log() {
  printf '[INFO] %s\n' "$*"
}

run_root() {
  if [[ "$(id -u)" -eq 0 ]]; then
    "$@"
  else
    sudo "$@"
  fi
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

ensure_base_packages() {
  if have_cmd apt-get; then
    run_root apt-get update -y
    run_root apt-get install -y curl ca-certificates gnupg
    return
  fi

  if have_cmd dnf; then
    run_root dnf install -y curl ca-certificates gnupg2
    return
  fi

  if have_cmd yum; then
    run_root yum install -y curl ca-certificates gnupg2
    return
  fi

  if have_cmd apk; then
    run_root apk add --no-cache bash curl ca-certificates nodejs npm python3
    return
  fi

  echo '{"ok":false,"error":"unsupported package manager"}'
  exit 1
}

ensure_node() {
  if have_cmd node; then
    local current
    current="$(node -v 2>/dev/null || true)"
    if [[ "$current" =~ ^v([0-9]+)\. ]] && (( BASH_REMATCH[1] >= OPENCLAW_NODE_MAJOR )); then
      log "Node.js already installed: $current"
      return
    fi
  fi

  if have_cmd apt-get; then
    log "Installing Node.js ${OPENCLAW_NODE_MAJOR}.x via NodeSource (apt)"
    curl -fsSL "https://deb.nodesource.com/setup_${OPENCLAW_NODE_MAJOR}.x" | run_root bash -
    run_root apt-get install -y nodejs
    return
  fi

  if have_cmd dnf; then
    log "Installing Node.js ${OPENCLAW_NODE_MAJOR}.x via NodeSource (dnf)"
    curl -fsSL "https://rpm.nodesource.com/setup_${OPENCLAW_NODE_MAJOR}.x" | run_root bash -
    run_root dnf install -y nodejs
    return
  fi

  if have_cmd yum; then
    log "Installing Node.js ${OPENCLAW_NODE_MAJOR}.x via NodeSource (yum)"
    curl -fsSL "https://rpm.nodesource.com/setup_${OPENCLAW_NODE_MAJOR}.x" | run_root bash -
    run_root yum install -y nodejs
    return
  fi

  if have_cmd apk && have_cmd node; then
    return
  fi

  echo '{"ok":false,"error":"failed to install Node.js"}'
  exit 1
}

resolve_openclaw() {
  if have_cmd openclaw; then
    command -v openclaw
    return
  fi

  local npm_prefix
  npm_prefix="$(npm prefix -g)"
  if [[ -x "${npm_prefix}/bin/openclaw" ]]; then
    echo "${npm_prefix}/bin/openclaw"
    return
  fi

  echo ""
}

invoke_openclaw() {
  local exe="$1"
  shift
  log "openclaw $1${2:+ $2}"
  "$exe" "$@"
}

open_firewall() {
  if [[ "${OPENCLAW_GATEWAY_OPEN_FIREWALL}" != "1" ]]; then
    return
  fi

  if have_cmd ufw; then
    run_root ufw allow "${OPENCLAW_GATEWAY_PORT}/tcp" || true
    return
  fi

  if have_cmd firewall-cmd; then
    run_root firewall-cmd --permanent --add-port="${OPENCLAW_GATEWAY_PORT}/tcp" || true
    run_root firewall-cmd --reload || true
    return
  fi
}

enable_linger() {
  if [[ "${OPENCLAW_GATEWAY_ENABLE_LINGER}" != "1" ]]; then
    return
  fi

  if have_cmd loginctl; then
    run_root loginctl enable-linger "$(id -un)" || true
  fi
}

RESULT_FILE="$(mktemp)"
cat >"$RESULT_FILE" <<JSON
{"ok":false,"host":"$(hostname)","user":"$(id -un)","steps":[]}
JSON

python3 - "$RESULT_FILE" <<'PY'
import json, sys
path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
data["steps"].append("bootstrap")
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f)
PY

ensure_base_packages
ensure_node

log "Installing ${OPENCLAW_PACKAGE}"
npm install -g "${OPENCLAW_PACKAGE}"
OPENCLAW_EXE="$(resolve_openclaw)"
if [[ -z "$OPENCLAW_EXE" ]]; then
  echo '{"ok":false,"error":"openclaw command not found after npm install"}'
  exit 1
fi

mkdir -p "${OPENCLAW_WORKSPACE}"
invoke_openclaw "$OPENCLAW_EXE" setup --mode local --non-interactive --workspace "${OPENCLAW_WORKSPACE}"
invoke_openclaw "$OPENCLAW_EXE" config set gateway.mode local
invoke_openclaw "$OPENCLAW_EXE" config set gateway.bind "${OPENCLAW_GATEWAY_BIND}"
invoke_openclaw "$OPENCLAW_EXE" config set --strict-json gateway.port "${OPENCLAW_GATEWAY_PORT}"
invoke_openclaw "$OPENCLAW_EXE" config set gateway.auth.mode "${OPENCLAW_GATEWAY_AUTH_MODE}"
if [[ -n "${OPENCLAW_GATEWAY_TOKEN}" ]]; then
  invoke_openclaw "$OPENCLAW_EXE" config set gateway.auth.token "${OPENCLAW_GATEWAY_TOKEN}"
fi
invoke_openclaw "$OPENCLAW_EXE" config set agents.defaults.workspace "${OPENCLAW_WORKSPACE}"

open_firewall
enable_linger

if [[ "${OPENCLAW_GATEWAY_INSTALL_SERVICE}" == "1" ]]; then
  INSTALL_ARGS=(gateway install --force --port "${OPENCLAW_GATEWAY_PORT}")
  if [[ "${OPENCLAW_GATEWAY_AUTH_MODE}" == "token" && -n "${OPENCLAW_GATEWAY_TOKEN}" ]]; then
    INSTALL_ARGS+=(--token "${OPENCLAW_GATEWAY_TOKEN}")
  fi
  invoke_openclaw "$OPENCLAW_EXE" "${INSTALL_ARGS[@]}"
fi

if [[ "${OPENCLAW_GATEWAY_START_SERVICE}" == "1" ]]; then
  invoke_openclaw "$OPENCLAW_EXE" gateway start
fi

STATUS_ARGS=(gateway status --json --url "ws://127.0.0.1:${OPENCLAW_GATEWAY_PORT}")
if [[ -n "${OPENCLAW_GATEWAY_TOKEN}" ]]; then
  STATUS_ARGS+=(--token "${OPENCLAW_GATEWAY_TOKEN}")
fi
STATUS_JSON="$("$OPENCLAW_EXE" "${STATUS_ARGS[@]}" || true)"
python3 - "$RESULT_FILE" "$STATUS_JSON" <<'PY'
import json, sys
path = sys.argv[1]
raw = sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
data["ok"] = True
data["steps"].extend(["node", "openclaw", "config", "service"])
if raw:
    try:
        data["gatewayStatus"] = json.loads(raw)
    except Exception:
        data["gatewayStatusRaw"] = raw
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f)
PY

cat "$RESULT_FILE"
rm -f "$RESULT_FILE"
