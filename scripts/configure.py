"""Writes Synapse, mautrix-meta and launchd configuration for messenger-mcp.

Run with the Synapse venv python (it provides PyYAML). Idempotent: every run
rewrites the generated files from secrets.env and the fixed ports below.
"""

import os
import plistlib
import sys
from pathlib import Path

import yaml

HOME = Path.home()
DATA = Path(os.environ.get("MM_DATA", HOME / "Library/Application Support/messenger-mcp"))
LOGS = Path(os.environ.get("MM_LOGS", HOME / "Library/Logs/messenger-mcp"))
VENV = Path(os.environ.get("MM_VENV", HOME / ".local/share/messenger-mcp-synapse-venv"))
BIN = Path(os.environ.get("MM_BIN", HOME / ".local/bin"))
AGENTS = HOME / "Library/LaunchAgents"
SERVER = "messenger.local"
SYN_PORT, BRIDGE_PORT = 7382, 7383
LABEL = "com.0xjesus.messenger."


def secrets():
    out = {}
    for line in (DATA / "secrets.env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def write(path, text, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.chmod(tmp, mode)
    tmp.replace(path)


def synapse(sec):
    syn = DATA / "synapse"
    cfg = {
        "server_name": SERVER,
        "pid_file": str(syn / "homeserver.pid"),
        "listeners": [{
            "port": SYN_PORT, "bind_addresses": ["127.0.0.1"], "type": "http", "tls": False,
            "x_forwarded": False, "resources": [{"names": ["client", "media"], "compress": False}],
        }],
        "database": {"name": "sqlite3", "args": {"database": str(syn / "homeserver.db")}},
        "log_config": str(syn / "log.yaml"),
        "media_store_path": str(syn / "media"),
        "signing_key_path": str(syn / f"{SERVER}.signing.key"),
        "registration_shared_secret": sec["SYNAPSE_REGISTRATION_SECRET"],
        "macaroon_secret_key": sec["SYNAPSE_MACAROON_SECRET"],
        "form_secret": sec["SYNAPSE_FORM_SECRET"],
        "enable_registration": False,
        "report_stats": False,
        "trusted_key_servers": [],
        "suppress_key_server_warning": True,
        "federation_domain_whitelist": [],
        "presence": {"enabled": False},
        "url_preview_enabled": False,
        "max_upload_size": "100M",
        "app_service_config_files": [str(DATA / "bridge" / "registration.yaml")],
        # Single-user server: linking creates one portal per chat at once.
        "rc_message": {"per_second": 100, "burst_count": 200},
        "rc_joins": {"local": {"per_second": 50, "burst_count": 500}},
        "rc_joins_per_room": {"per_second": 50, "burst_count": 500},
        "rc_invites": {"per_room": {"per_second": 50, "burst_count": 500},
                       "per_user": {"per_second": 50, "burst_count": 500},
                       "per_issuer": {"per_second": 50, "burst_count": 500}},
    }
    write(syn / "homeserver.yaml", yaml.safe_dump(cfg, sort_keys=False))
    log = {
        "version": 1,
        "formatters": {"precise": {"format": "%(asctime)s %(name)s %(levelname)s %(message)s"}},
        "handlers": {"file": {
            "class": "logging.handlers.TimedRotatingFileHandler", "formatter": "precise",
            "filename": str(LOGS / "synapse.log"), "when": "midnight", "backupCount": 3, "encoding": "utf8",
        }},
        "root": {"level": "WARNING", "handlers": ["file"]},
        "disable_existing_loggers": False,
    }
    write(syn / "log.yaml", yaml.safe_dump(log, sort_keys=False))


def bridge(sec):
    path = DATA / "bridge" / "config.yaml"
    cfg = yaml.safe_load(path.read_text())
    db = str(DATA / "bridge" / "meta.db").replace(" ", "%20")
    cfg["database"].update({"type": "sqlite3-fk-wal", "uri": f"file:{db}?_txlock=immediate"})
    cfg["homeserver"].update({"address": f"http://127.0.0.1:{SYN_PORT}", "domain": SERVER, "software": "standard"})
    cfg["appservice"].update({
        "address": f"http://127.0.0.1:{BRIDGE_PORT}", "hostname": "127.0.0.1", "port": BRIDGE_PORT,
        "id": "meta", "username_template": "meta_{{.}}",
    })
    cfg["appservice"]["bot"].update({"username": "metabot", "displayname": "Messenger bridge"})
    cfg["bridge"]["permissions"] = {f"@jesus:{SERVER}": "admin"}
    cfg["matrix"]["message_status_events"] = True
    cfg["provisioning"]["shared_secret"] = sec["BRIDGE_PROVISIONING_SECRET"]
    cfg["backfill"].update({"enabled": True, "max_initial_messages": 50})
    cfg["encryption"]["allow"] = False
    cfg["logging"] = {"min_level": "info", "writers": [{
        "type": "file", "format": "json", "filename": str(LOGS / "bridge.log"),
        "max_size": 100, "max_backups": 3, "compress": False,
    }]}
    write(path, yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True))


def agent(name, args, cwd=None):
    plist = {
        "Label": LABEL + name,
        "ProgramArguments": [str(a) for a in args],
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "StandardOutPath": str(LOGS / f"{name}.out.log"),
        "StandardErrorPath": str(LOGS / f"{name}.out.log"),
        "EnvironmentVariables": {"PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"},
    }
    if cwd:
        plist["WorkingDirectory"] = str(cwd)
    AGENTS.mkdir(parents=True, exist_ok=True)
    with open(AGENTS / f"{LABEL}{name}.plist", "wb") as f:
        plistlib.dump(plist, f)


def agents(sec):
    syn, br = DATA / "synapse", DATA / "bridge"
    agent("synapse", [VENV / "bin/python", "-m", "synapse.app.homeserver", "-c", syn / "homeserver.yaml"], syn)
    agent("bridge", [BIN / "mautrix-meta", "-n", "-c", br / "config.yaml", "-r", br / "registration.yaml"], br)
    agent("mcp", [BIN / "messenger-mcp", "serve", "--data", DATA], DATA)
    agent("awake", ["/usr/bin/caffeinate", "-i", "-s"])
    target = sec.get("TUNNEL_TARGET")
    key = HOME / ".ssh/messenger_tunnel_ed25519"
    if target and key.exists():
        agent("tunnel", [
            "/usr/bin/ssh", "-NT", "-i", key, "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3", "-o", "StrictHostKeyChecking=accept-new",
            "-R", "127.0.0.1:7381:127.0.0.1:7381", target,
        ])


if __name__ == "__main__":
    step = sys.argv[1]
    sec = secrets()
    {"synapse": synapse, "bridge": bridge, "agents": agents}[step](sec)
