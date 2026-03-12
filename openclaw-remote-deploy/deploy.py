#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import secrets
import shlex
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def load_inventory(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Inventory root must be a mapping.")
    if not isinstance(data.get("hosts"), list) or not data["hosts"]:
        raise ValueError("Inventory must contain a non-empty 'hosts' list.")
    return data


def prompt_text(label: str, default: str | None = None, required: bool = True) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        if not required:
            return ""


def prompt_bool(label: str, default: bool = True) -> bool:
    default_text = "Y/n" if default else "y/N"
    value = input(f"{label} [{default_text}]: ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes", "true", "1"}


def build_prompt_host() -> dict[str, Any]:
    host = prompt_text("VPS 地址/IP")
    username = prompt_text("SSH 用户名", "root")
    key_filename = prompt_text("SSH 私钥路径", str(Path.home() / ".ssh" / "id_ed25519"))
    gateway_port = int(prompt_text("OpenClaw Gateway 端口", "18789"))
    token_default = secrets.token_hex(24)
    gateway_token = prompt_text("Gateway token", token_default)
    workspace_default = "/root/.openclaw/workspace" if username == "root" else f"/home/{username}/.openclaw/workspace"
    workspace = prompt_text("OpenClaw 工作目录", workspace_default)
    open_firewall = prompt_bool("自动开放 VPS 防火墙端口", True)
    enable_linger = prompt_bool("启用 systemd linger 保持用户服务常驻", True)

    return {
        "name": host,
        "address": host,
        "username": username,
        "ssh": {
            "port": 22,
            "timeout_sec": 30,
            "known_hosts": "autoadd",
            "key_filename": key_filename,
        },
        "install": {
            "node_version_major": 22,
            "openclaw_package": "openclaw@latest",
            "workspace": workspace,
            "gateway": {
                "port": gateway_port,
                "bind": "lan",
                "auth_mode": "token",
                "token": gateway_token,
                "open_firewall": open_firewall,
                "install_service": True,
                "start_service": True,
                "enable_linger": enable_linger,
            },
        },
    }


def build_host_payload(defaults: dict[str, Any], host: dict[str, Any]) -> dict[str, Any]:
    payload = deep_merge(defaults, host)
    required = ("name", "address", "username")
    missing = [key for key in required if not payload.get(key)]
    if missing:
        raise ValueError(f"Host entry missing required fields: {', '.join(missing)}")

    ssh_cfg = payload.setdefault("ssh", {})
    install = payload.setdefault("install", {})
    gateway = install.setdefault("gateway", {})

    ssh_cfg.setdefault("port", 22)
    ssh_cfg.setdefault("timeout_sec", 30)
    ssh_cfg.setdefault("known_hosts", "autoadd")
    ssh_cfg.setdefault("identity_only", True)
    if not ssh_cfg.get("key_filename"):
        raise ValueError(f"{payload['name']}: ssh.key_filename is required")

    install.setdefault("node_version_major", 22)
    install.setdefault("openclaw_package", "openclaw@latest")
    if not install.get("workspace"):
        raise ValueError(f"{payload['name']}: install.workspace is required")

    gateway.setdefault("port", 18789)
    gateway.setdefault("bind", "lan")
    gateway.setdefault("auth_mode", "token")
    gateway.setdefault("token", secrets.token_hex(24))
    gateway.setdefault("open_firewall", True)
    gateway.setdefault("install_service", True)
    gateway.setdefault("start_service", True)
    gateway.setdefault("enable_linger", True)

    return payload


def build_remote_script(script_path: Path, remote_payload: dict[str, Any]) -> str:
    payload_json = json.dumps(remote_payload, ensure_ascii=False)
    payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("ascii")
    script = script_path.read_text(encoding="utf-8")
    return f"export OPENCLAW_DEPLOY_PAYLOAD_B64={shlex.quote(payload_b64)}\n{script}\n"


def build_ssh_argv(host_payload: dict[str, Any]) -> list[str]:
    ssh_cfg = host_payload["ssh"]
    argv = [
        "ssh",
        "-p",
        str(int(ssh_cfg["port"])),
        "-o",
        f"ConnectTimeout={int(ssh_cfg['timeout_sec'])}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "BatchMode=yes",
    ]

    known_hosts = ssh_cfg.get("known_hosts", "autoadd")
    if known_hosts == "autoadd":
        argv += ["-o", "StrictHostKeyChecking=accept-new"]
    elif known_hosts == "off":
        argv += ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]

    if ssh_cfg.get("identity_only", True):
        argv += ["-o", "IdentitiesOnly=yes"]

    argv += ["-i", str(ssh_cfg["key_filename"])]
    argv.append(f"{host_payload['username']}@{host_payload['address']}")
    argv.append("bash -s --")
    return argv


def parse_remote_result(stdout: str) -> dict[str, Any]:
    if not stdout:
        return {}
    last_line = stdout.splitlines()[-1]
    try:
        return {"remote": json.loads(last_line)}
    except json.JSONDecodeError:
        return {"remote_raw": last_line}


def run_host(script_path: Path, host_payload: dict[str, Any]) -> dict[str, Any]:
    remote_payload = {"install": host_payload["install"]}
    remote_script = build_remote_script(script_path, remote_payload)
    argv = build_ssh_argv(host_payload)
    proc = subprocess.run(
        argv,
        input=remote_script,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=int(host_payload["ssh"]["timeout_sec"]) + 2400,
    )
    result: dict[str, Any] = {
        "name": host_payload["name"],
        "address": host_payload["address"],
        "status_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "client": "ssh",
        "transport": "ssh",
        "argv": " ".join(shlex.quote(part) for part in argv[:-1]) + " <bash-script>",
    }
    result.update(parse_remote_result(result["stdout"]))
    return result


def select_hosts(hosts: list[dict[str, Any]], limit: list[str]) -> list[dict[str, Any]]:
    if not limit:
        return hosts
    wanted = set(limit)
    return [host for host in hosts if host.get("name") in wanted or host.get("address") in wanted]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-click OpenClaw installer for Linux VPS over SSH.")
    parser.add_argument("inventory", nargs="?", type=Path, help="Path to inventory YAML. Omit to enter values interactively.")
    parser.add_argument("--script", type=Path, default=Path(__file__).with_name("remote_install.sh"), help="Remote Bash installer script.")
    parser.add_argument("--limit", action="append", default=[], help="Limit execution to a host name or address. Repeatable.")
    parser.add_argument("--report", type=Path, help="Write JSON report to this file.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop on first failed host.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.inventory:
        inventory = load_inventory(args.inventory)
        defaults = inventory.get("defaults", {})
        raw_hosts = select_hosts(inventory["hosts"], args.limit)
    else:
        defaults = {}
        raw_hosts = [build_prompt_host()]

    if not raw_hosts:
        print("No hosts to deploy.", file=sys.stderr)
        return 2

    results = []
    failed = False

    for raw_host in raw_hosts:
        host_payload = build_host_payload(defaults, raw_host)
        print(f"==> {host_payload['name']} ({host_payload['address']})", flush=True)
        try:
            result = run_host(args.script, host_payload)
        except Exception as exc:
            result = {
                "name": host_payload["name"],
                "address": host_payload["address"],
                "status_code": -1,
                "error": str(exc),
            }

        results.append(result)
        remote = result.get("remote", {})
        ok = result.get("status_code") == 0 and remote.get("ok") is True
        print(f"    {'OK' if ok else 'FAILED'}", flush=True)
        if not ok:
            failed = True
            if args.fail_fast:
                break

    if args.report:
        args.report.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
