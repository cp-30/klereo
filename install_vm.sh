#!/usr/bin/env bash
# ============================================================================
# Klereo Monitor - one-time VM installer with git auto-deploy
#
# Run this ONCE on the VM, from inside the cloned repo, e.g.:
#     sudo git clone https://github.com/<you>/klereo-monitor.git /opt/klereo
#     sudo bash /opt/klereo/install_vm.sh
#
# It sets up:
#   * python + requests
#   * the monitor as a systemd service (auto-start, auto-restart)
#   * a 5-minute timer that 'git pull's this repo and restarts ONLY when the
#     code actually changed -> future updates deploy themselves.
#
# Secrets live in /opt/klereo/klereo.env (created from the example, never
# committed to git). The database and env file are git-ignored, so pulls
# never touch them.
# ============================================================================
set -e

# App dir = wherever this script lives (the cloned repo)
APPDIR="$(cd "$(dirname "$0")" && pwd)"
echo ">> App directory: $APPDIR"

if [ "$(id -u)" -ne 0 ]; then echo "Please run with sudo."; exit 1; fi

echo ">> Installing packages (python3, requests, git)"
apt-get update -qq
apt-get install -y -qq python3 python3-requests git

git config --global --add safe.directory "$APPDIR" || true

echo ">> Preparing secrets file"
if [ ! -f "$APPDIR/klereo.env" ]; then
  cp "$APPDIR/klereo.env.example" "$APPDIR/klereo.env"
  chmod 600 "$APPDIR/klereo.env"
  NEEDS_SECRETS=1
  echo "   Created $APPDIR/klereo.env - you must edit it with your secrets."
else
  echo "   Existing klereo.env kept (not overwritten)."
fi

echo ">> Installing monitor service"
cat >/etc/systemd/system/klereo-monitor.service <<EOF
[Unit]
Description=Klereo Pool Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$APPDIR
EnvironmentFile=$APPDIR/klereo.env
ExecStart=/usr/bin/python3 $APPDIR/klereo_monitor.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo ">> Installing auto-update script"
cat >/usr/local/bin/klereo-update.sh <<EOF
#!/usr/bin/env bash
# Pull latest code; restart the monitor only if the repo advanced.
cd "$APPDIR" || exit 0
git config --global --add safe.directory "$APPDIR" >/dev/null 2>&1 || true
before=\$(git rev-parse HEAD 2>/dev/null || echo none)
git pull --quiet 2>/dev/null || exit 0
after=\$(git rev-parse HEAD 2>/dev/null || echo none)
if [ "\$before" != "\$after" ]; then
  echo "klereo-update: updated \$before -> \$after, restarting monitor"
  systemctl restart klereo-monitor
fi
EOF
chmod +x /usr/local/bin/klereo-update.sh

echo ">> Installing auto-update timer (every 5 minutes)"
cat >/etc/systemd/system/klereo-update.service <<EOF
[Unit]
Description=Klereo Monitor auto-update (git pull + restart on change)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/klereo-update.sh
EOF

cat >/etc/systemd/system/klereo-update.timer <<EOF
[Unit]
Description=Run Klereo Monitor auto-update periodically

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Persistent=true

[Install]
WantedBy=timers.target
EOF

echo ">> Enabling and starting everything"
systemctl daemon-reload
systemctl enable --now klereo-monitor
systemctl enable --now klereo-update.timer

echo
echo "==================== DONE ===================="
if [ "${NEEDS_SECRETS:-0}" = "1" ]; then
  echo "FIRST-TIME SETUP: edit your secrets, then restart:"
  echo "    sudo nano $APPDIR/klereo.env"
  echo "    sudo systemctl restart klereo-monitor"
  echo
fi
echo "Status:      systemctl status klereo-monitor --no-pager"
echo "Live logs:   journalctl -u klereo-monitor -f"
echo "Update log:  journalctl -u klereo-update.service -n 20 --no-pager"
echo
echo "From now on, committing a change to the repo auto-deploys within ~5 min."
