# Klereo Monitor — self-deploying setup

This turns the pool monitor into a service that **updates itself**. You commit a
code change to a GitHub repo you control; within ~5 minutes your cloud VM pulls
it and restarts automatically. No SSH, no file uploads for day-to-day changes.

## What's in the repo

| File | Purpose | In git? |
|------|---------|---------|
| `klereo_monitor.py` | The app (this is what you'll update over time) | yes |
| `install_vm.sh` | One-time VM setup: service + auto-update timer | yes |
| `klereo.env.example` | Template for your secrets | yes |
| `.gitignore` | Keeps secrets + database out of git | yes |
| `README.md` | This file | yes |
| `klereo.env` | **Your real secrets** — lives only on the VM | **NO** |
| `klereo_monitor.db` | The database — lives only on the VM | **NO** |

Your passwords never go into GitHub. They sit in `klereo.env` on the VM, which
`.gitignore` deliberately excludes.

---

## One-time setup

### 1. Create the GitHub repo

1. Make a free account at github.com if you don't have one.
2. Create a new **private** repository named `klereo-monitor` (don't add a README — we have one).
3. On the repo page, click **Add file → Upload files**, drag in these five files, and **Commit**:
   `klereo_monitor.py`, `install_vm.sh`, `klereo.env.example`, `.gitignore`, `README.md`
4. Copy the repo's HTTPS URL (green **Code** button), e.g.
   `https://github.com/yourname/klereo-monitor.git`

### 2. Install on the VM (once)

In the Google Cloud browser SSH window:

```bash
sudo git clone https://github.com/yourname/klereo-monitor.git /opt/klereo
sudo bash /opt/klereo/install_vm.sh
```

### 3. Add your secrets (once)

```bash
sudo nano /opt/klereo/klereo.env
```

Fill in `KLEREO_PASSWORD`, `DASH_PASS`, and `SMTP_PASS` (Gmail App Password),
save (Ctrl-O, Enter) and exit (Ctrl-X), then:

```bash
sudo systemctl restart klereo-monitor
```

Check it's alive:

```bash
journalctl -u klereo-monitor -f      # Ctrl-C to stop watching
```

You should see a `polled: ...` line within a minute.

---

## How to ship a change (the whole point)

Whenever the app needs updating:

1. I give you the new `klereo_monitor.py`.
2. On GitHub, open `klereo_monitor.py` → click the **pencil (Edit)** icon →
   paste the new contents → **Commit changes**. (All in the browser.)
3. Within ~5 minutes the VM pulls it and restarts itself. Done.

That's it — no server access needed for updates.

To force an update immediately instead of waiting:

```bash
sudo /usr/local/bin/klereo-update.sh
```

---

## Handy commands

```bash
systemctl status klereo-monitor --no-pager     # is it running?
journalctl -u klereo-monitor -f                # live app log
journalctl -u klereo-update.service -n 20 --no-pager   # last auto-update runs
sudo systemctl restart klereo-monitor          # restart manually
```

## Notes

- The service runs on this dedicated single-purpose VM and is reachable only
  over your private Tailscale network on port 8080; the dashboard is also
  password-protected (`DASH_USER` / `DASH_PASS`).
- Auto-update only pulls from **your** repo and only restarts when the code
  actually changed — nothing else can push to your server.
- If a pull ever fails (e.g. network blip) it simply keeps running the current
  version and tries again next cycle.
