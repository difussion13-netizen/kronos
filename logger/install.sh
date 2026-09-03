#!/usr/bin/env bash
# Установка kronolog на Ubuntu 22.04/24.04 или Amazon Linux 2023.
# Запускать из директории logger/ от root:  sudo bash install.sh
set -euo pipefail

APP=/opt/kronolog/app
SRC="$(cd "$(dirname "$0")" && pwd)"

if command -v apt-get >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y python3-venv python3-pip rsync
else
  dnf install -y python3-pip python3-rsync rsync 2>/dev/null || dnf install -y python3-pip rsync
fi

mkdir -p "$APP"
rsync -a --delete --exclude 'kronolog-selftest' --exclude '__pycache__' "$SRC"/ "$APP"/

python3 -m venv /opt/kronolog/venv
/opt/kronolog/venv/bin/pip install -q --upgrade pip
/opt/kronolog/venv/bin/pip install -q -r "$APP/requirements.txt"

id -u kronolog &>/dev/null || useradd -r -s /usr/sbin/nologin kronolog
mkdir -p /var/lib/kronolog
chown -R kronolog:kronolog /var/lib/kronolog /opt/kronolog

cp "$APP/kronolog.service" /etc/systemd/system/kronolog.service
systemctl daemon-reload
systemctl enable --now kronolog

sleep 5
systemctl status kronolog --no-pager | head -15 || true
echo "--- status.json:"
cat /var/lib/kronolog/status.json 2>/dev/null || echo "(ещё не создан)"
