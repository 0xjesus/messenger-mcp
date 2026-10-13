#!/usr/bin/env bash
# Installs messenger-mcp on macOS: Synapse + mautrix-meta + messenger-mcp as
# launchd agents. Safe to run again; secrets are only generated once.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
source "$REPO/deploy/versions.env"
export MM_DATA="$HOME/Library/Application Support/messenger-mcp"
export MM_LOGS="$HOME/Library/Logs/messenger-mcp"
export MM_VENV="$HOME/.local/share/messenger-mcp-synapse-venv"
export MM_BIN="$HOME/.local/bin"
SECRETS="$MM_DATA/secrets.env"
LABEL=com.0xjesus.messenger
GUI="gui/$(id -u)"
PY=/usr/local/bin/python3.12

say() { printf '\n== %s\n' "$*"; }

umask 077
mkdir -p "$MM_DATA/synapse" "$MM_DATA/bridge" "$MM_DATA/uploads" "$MM_DATA/downloads" "$MM_LOGS" "$MM_BIN"
chmod 700 "$MM_DATA"

if [ ! -f "$SECRETS" ]; then
  say "Generando secretos en $SECRETS"
  r() { openssl rand -hex 32; }
  cat > "$SECRETS" <<SEC
MATRIX_PASSWORD=$(r)
BRIDGE_PROVISIONING_SECRET=$(r)
INTERNAL_SECRET=$(r)
SYNAPSE_REGISTRATION_SECRET=$(r)
SYNAPSE_MACAROON_SECRET=$(r)
SYNAPSE_FORM_SECRET=$(r)
SEC
fi
chmod 600 "$SECRETS"
if [ -n "${TUNNEL_TARGET:-}" ] && ! grep -q '^TUNNEL_TARGET=' "$SECRETS"; then
  echo "TUNNEL_TARGET=$TUNNEL_TARGET" >> "$SECRETS"
fi

if [ ! -x "$MM_VENV/bin/python" ] || ! "$MM_VENV/bin/python" -c "import synapse,sys; sys.exit(synapse.__version__ != '$SYNAPSE_VERSION')"; then
  say "Instalando Synapse $SYNAPSE_VERSION"
  "$PY" -m venv "$MM_VENV"
  "$MM_VENV/bin/pip" install --quiet --upgrade pip
  "$MM_VENV/bin/pip" install --quiet "matrix-synapse==$SYNAPSE_VERSION"
fi

if ! "$MM_BIN/mautrix-meta" -v 2>/dev/null | grep -q .; then
  say "Compilando mautrix-meta $MAUTRIX_META_VERSION"
  src="$HOME/src/mautrix-meta"
  [ -d "$src" ] || git clone https://github.com/mautrix/meta.git "$src"
  git -C "$src" fetch --tags --quiet && git -C "$src" checkout --quiet "$MAUTRIX_META_VERSION"
  (cd "$src" && go build -tags goolm -ldflags "-s -w" -o "$MM_BIN/mautrix-meta" ./cmd/mautrix-meta)
fi

say "Compilando messenger-mcp"
(cd "$REPO" && go build -ldflags "-s -w" -o "$MM_BIN/messenger-mcp" ./cmd/messenger-mcp)

say "Configurando Synapse y el puente"
"$MM_VENV/bin/python" "$REPO/scripts/configure.py" synapse
[ -f "$MM_DATA/bridge/config.yaml" ] || "$MM_BIN/mautrix-meta" -e -c "$MM_DATA/bridge/config.yaml" >/dev/null
"$MM_VENV/bin/python" "$REPO/scripts/configure.py" bridge
if [ ! -f "$MM_DATA/bridge/registration.yaml" ]; then
  (cd "$MM_DATA/bridge" && "$MM_BIN/mautrix-meta" -g -c config.yaml -r registration.yaml >/dev/null)
fi
chmod 600 "$MM_DATA/bridge/config.yaml" "$MM_DATA/bridge/registration.yaml"
(cd "$MM_DATA/synapse" && "$MM_VENV/bin/python" -m synapse.app.homeserver -c homeserver.yaml --generate-keys >/dev/null)

if [ -n "${TUNNEL_TARGET:-}" ] && [ ! -f "$HOME/.ssh/messenger_tunnel_ed25519" ]; then
  ssh-keygen -q -t ed25519 -N "" -C "messenger-tunnel@$(hostname -s)" -f "$HOME/.ssh/messenger_tunnel_ed25519"
fi
"$MM_VENV/bin/python" "$REPO/scripts/configure.py" agents

load() {
  launchctl bootout "$GUI/$LABEL.$1" 2>/dev/null || true
  launchctl bootstrap "$GUI" "$HOME/Library/LaunchAgents/$LABEL.$1.plist"
}
wait_http() {
  for _ in $(seq 1 60); do curl -fs -o /dev/null "$1" && return 0; sleep 1; done
  echo "no responde: $1" >&2; return 1
}

say "Arrancando Synapse"
load synapse
wait_http http://127.0.0.1:7382/health
set +e
out=$(cd "$MM_DATA/synapse" && "$MM_VENV/bin/register_new_matrix_user" --no-admin -u jesus \
  -p "$(grep '^MATRIX_PASSWORD=' "$SECRETS" | cut -d= -f2-)" -c homeserver.yaml http://127.0.0.1:7382 2>&1)
rc=$?
set -e
if [ $rc -ne 0 ] && ! grep -q "already taken" <<<"$out"; then echo "$out" >&2; exit 1; fi

say "Arrancando puente, servicio y agentes"
load bridge
load mcp
load awake
[ -f "$HOME/Library/LaunchAgents/$LABEL.tunnel.plist" ] && load tunnel
wait_http http://127.0.0.1:7381/healthz

say "Listo"
echo "Servicio: http://127.0.0.1:7381/mcp"
echo "Liga de vinculación: $MM_BIN/messenger-mcp pair-url"
[ -f "$HOME/.ssh/messenger_tunnel_ed25519.pub" ] && echo "Llave del túnel: $HOME/.ssh/messenger_tunnel_ed25519.pub"
