#!/bin/sh
# Installs or upgrades idm as a systemd service on Debian/Ubuntu.
# Usage (as root, from the folder holding the binary):  sh install.sh [binary]
set -eu

BIN=${1:-}
if [ -z "$BIN" ]; then
	case "$(dpkg --print-architecture)" in
		amd64) BIN=idm-linux-amd64 ;;
		arm64) BIN=idm-linux-arm64 ;;
		*) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;;
	esac
fi
[ "$(id -u)" -eq 0 ] || { echo "run as root" >&2; exit 1; }
[ -f "$BIN" ] || { echo "binary not found: $BIN" >&2; exit 1; }

DATA=/var/lib/idm
DOWNLOADS=/srv/downloads

id idm >/dev/null 2>&1 || useradd --system --home-dir "$DATA" --shell /usr/sbin/nologin idm

# The database is locked while the server runs.
systemctl stop idm 2>/dev/null || true

install -m 0755 "$BIN" /usr/local/bin/idm
install -d -o idm -g idm -m 0700 "$DATA"
install -d -o idm -g idm -m 0755 "$DOWNLOADS"
install -d -m 0755 /etc/idm
if [ ! -f /etc/idm/env ]; then
	cat > /etc/idm/env <<EOF
IDM_ADDR=0.0.0.0:8080
IDM_DOWNLOAD_DIR=$DOWNLOADS
EOF
	chmod 0644 /etc/idm/env
fi

# First install: save the UI password in the database (owned by idm).
if [ ! -f "$DATA/idm.db" ]; then
	echo "Choose the password for the web UI."
	runuser -u idm -- /usr/local/bin/idm init -data "$DATA" -dir "$DOWNLOADS"
fi

install -m 0644 "$(dirname "$0")/idm.service" /etc/systemd/system/idm.service
systemctl daemon-reload
systemctl enable idm >/dev/null
systemctl start idm
sleep 1
systemctl --no-pager --lines=5 status idm || true
echo
echo "idm is running on port 8080."
echo "To change the password later:"
echo "  systemctl stop idm && runuser -u idm -- idm init -data $DATA && systemctl start idm"
