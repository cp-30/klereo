#!/usr/bin/env bash
# ---------------------------------------------------------------
# Klereo Monitor - one-shot setup for a Debian/Ubuntu cloud VM
# (Google Cloud e2-micro, Oracle, Hetzner, etc.)
#
# Before running, make sure these two files are in the SAME folder
# as this script (your home directory is fine):
#     klereo_monitor.py
#     klereo.env         (with your secrets filled in)
#
# Then run:   bash setup_klereo_gcp.sh
# ---------------------------------------------------------------
set -e

APPDIR="$HOME/klereo"
echo ">> Installing into $APPDIR"
mkdir -p "$APPDIR"

# Move the app + env next to each other (if run from elsewhere)
for f in klereo_monitor.py klereo.env; do
  if [ -f "$HOME/$f" ] && [ "$HOME/$f" != "$APPDIR/$f" ]; then mv "$HOME/$f" "$APPDIR/$f"; fi
  if [ -f "./$f" ] && [ "$(pwd)/$f" != "$APPDIR/$f" ]; then cp "./$f" "$APPDIR/$f"; fi
done

if [ ! -f "$APPDIR/klereo_monitor.py" ]; then echo "ERROR: klereo_monitor.py not found"; exit 1; fi
if [ ! -f "$APPDIR/klereo.env" ];       then echo "ERROR: klereo.env not found";       exit 1; fi
chmod 600 "$APPDIR/klereo.env"   # secrets: readable only by you

echo ">> Installing Python + requests"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-requests

echo ">> Creating systemd service"
sudo tee /etc/systemd/system/klereo-monitor.service >/dev/null <<EOF
[Unit]
Description=Klereo Pool Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$APPDIR
EnvironmentFile=$APPDIR/klereo.env
ExecStart=/usr/bin/python3 $APPDIR/klereo_monitor.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo ">> Enabling + starting service"
sudo systemctl daemon-reload
sudo systemctl enable --now klereo-monitor

sleep 3
echo ">> Status:"
sudo systemctl status klereo-monitor --no-pager || true
echo
echo ">> Recent log (should show a poll within a minute):"
sudo journalctl -u klereo-monitor -n 15 --no-pager || true
echo
echo "Done. The monitor is running and will auto-start on reboot."
echo "To watch it live:   journalctl -u klereo-monitor -f"
echo "After editing klereo.env:   sudo systemctl restart klereo-monitor"
