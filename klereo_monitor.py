#!/usr/bin/env python3
"""
Klereo pool monitor  --  unattended poller + web dashboard  (stdlib + requests)
===============================================================================
One self-contained service that:
  * logs into Klereo Connect every few minutes (READ-ONLY),
  * records pH, ORP/Redox, water temp, salt, filtration state and the
    liquid-chlorine odometer to a local SQLite file,
  * emails you once when the current chlorine bottle passes your threshold,
  * serves a small password-protected web dashboard where you can watch live
    readings + a usage chart, change the threshold/bottle size, and press
    "New bottle fitted" to reset the baseline.

It never writes anything to your Klereo controller. Uses only the Python
standard library plus `requests`. Runs on Windows, macOS or Linux.

    pip install requests
    python klereo_monitor.py
    # then open  http://localhost:8080/  (or the machine's IP from another device)

--------------------------------------------------------------------------
Environment variables
--------------------------------------------------------------------------
  KLEREO_LOGIN       your Klereo login             (required)
  KLEREO_PASSWORD    your Klereo password          (required)
  KLEREO_POOL_ID     pool id, e.g. 156682          (optional; auto if only one)

  DASH_USER          dashboard username            (default: admin)
  DASH_PASS          dashboard password            (set this before exposing it)

  SMTP_HOST          e.g. smtp.gmail.com           (optional; needed for email)
  SMTP_PORT          e.g. 587
  SMTP_USER          e.g. paulking247@gmail.com
  SMTP_PASS          Gmail App Password (16 chars)
  ALERT_TO           where to email alerts
  ALERT_FROM         (optional; defaults to SMTP_USER)

  PORT               web port                      (default: 8080)
  POLL_MINUTES       how often to poll Klereo      (default: 15)
  DB_PATH            sqlite file path              (default: next to this script)
  BOTTLE_THRESHOLD_L default alert level litres    (default: 15)
  BOTTLE_SIZE_L      default bottle size litres    (default: 20)
"""

import os
import re
import sys
import json
import time
import hashlib
import sqlite3
import smtplib
import threading
from email.message import EmailMessage
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from http.cookies import SimpleCookie
from base64 import b64decode

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run:  pip install requests")

# --------------------------------------------------------------------------
BASE_URL = "https://connect.klereo.fr/"
APP_KIND, VERSION, LANG, HTTP_TIMEOUT = "Web", "3-W", "en", 30

DB_PATH      = os.environ.get("DB_PATH", os.path.join(
                   os.path.dirname(os.path.abspath(__file__)), "klereo_monitor.db"))
COVER_IMG    = os.path.join(os.path.dirname(DB_PATH), "cover_latest.jpg")
PORT         = int(os.environ.get("PORT", "8080"))
POLL_MINUTES = float(os.environ.get("POLL_MINUTES", "15"))
DASH_USER    = os.environ.get("DASH_USER", "admin")
DASH_PASS    = os.environ.get("DASH_PASS")

DEF_THRESHOLD = float(os.environ.get("BOTTLE_THRESHOLD_L", "15"))
DEF_BOTTLE    = float(os.environ.get("BOTTLE_SIZE_L", "20"))

# Token stored in a long-lived cookie so the login persists (no repeated prompts
# in the iOS home-screen app). Derived from the dashboard password.
AUTH_TOKEN = (hashlib.sha256(("klereo-auth|" + (DASH_USER or "") + "|" +
              (DASH_PASS or "")).encode()).hexdigest() if DASH_PASS else None)
COOKIE_MAXAGE = 34560000  # ~400 days

# Probe type constants (from the Klereo bundle)
T_PH, T_REDOX, T_EAU, T_PRESSION, T_SALIN, T_CHLORE = 3, 4, 5, 6, 8, 14
# Output categories (out.index)
SCHED_LIGHT, SCHED_FILTRE, SCHED_PH, SCHED_TRAIT, SCHED_CHAUF = 0, 1, 2, 3, 4

_lock = threading.Lock()


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Storage (SQLite)  -- one connection per call keeps it thread-safe & simple
# --------------------------------------------------------------------------
def db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        c.execute("""CREATE TABLE IF NOT EXISTS readings (
            ts TEXT, total_time REAL, debit REAL, ph REAL, orp REAL,
            temp REAL, salt REAL, filtration INTEGER, used_l REAL, filt_total REAL)""")
        # migrate older DBs that predate the filter-runtime column
        cols = [r[1] for r in c.execute("PRAGMA table_info(readings)").fetchall()]
        if "filt_total" not in cols:
            c.execute("ALTER TABLE readings ADD COLUMN filt_total REAL")
    if kv_get("bottle_l")         is None: kv_set("bottle_l", DEF_BOTTLE)
    if kv_get("poll_minutes")     is None: kv_set("poll_minutes", POLL_MINUTES)
    if kv_get("warn_remaining_l") is None: kv_set("warn_remaining_l", 5.0)
    if kv_get("final_remaining_l") is None: kv_set("final_remaining_l", 0.5)
    if kv_get("notified_warn")    is None: kv_set("notified_warn", 0)
    if kv_get("notified_final")   is None: kv_set("notified_final", 0)
    if kv_get("pump_kw")          is None: kv_set("pump_kw", 1.1)
    if kv_get("price_kwh")        is None: kv_set("price_kwh", 0.25)
    if kv_get("currency")         is None: kv_set("currency", "€")


def kv_get(k, default=None):
    with db() as c:
        r = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def kv_set(k, v):
    with db() as c:
        c.execute("INSERT INTO kv(k,v) VALUES(?,?) "
                  "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))


# Config/secrets: a value edited in the web UI is stored in the DB (kv, on the
# VM only) and takes priority over the environment variable of the same name.
CFG_KEYS = ("KLEREO_LOGIN", "KLEREO_PASSWORD", "KLEREO_POOL_ID",
            "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "ALERT_TO", "ALERT_FROM", "SMTP_PASS")


def cfg_get(key):
    v = kv_get("cfg_" + key)
    if isinstance(v, str) and v != "":
        return v
    return os.environ.get(key)


def cfg_set(key, value):
    kv_set("cfg_" + key, value)


# --------------------------------------------------------------------------
# Klereo client (read-only)
# --------------------------------------------------------------------------
class KlereoError(Exception):
    pass


class Klereo:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0 (KlereoMonitor)",
                               "X-Requested-With": "XMLHttpRequest"})
        self.token = None

    def _post(self, path, data=None, auth=True):
        h = {"Authorization": "Bearer " + self.token} if auth else {}
        r = self.s.post(BASE_URL + path, data=data or {}, headers=h, timeout=HTTP_TIMEOUT)
        r.raise_for_status()
        a = r.headers.get("authorization")
        if a and a.lower().startswith("bearer "):
            self.token = a[7:]
        try:
            b = r.json()
        except ValueError:
            raise KlereoError(f"{path}: non-JSON response")
        if b.get("status") == "error":
            raise KlereoError(f"{path}: {b.get('detail')}")
        if b.get("status") != "ok":
            raise KlereoError(f"{path}: status {b.get('status')!r}")
        return b

    def login(self, login, password):
        self._post("php/GetJWT.php",
                   {"login": login, "password": hashlib.sha1(password.encode()).hexdigest(),
                    "version": VERSION, "app": APP_KIND}, auth=False)
        if not self.token:
            raise KlereoError("login ok but no JWT header")

    def pool_ids(self):
        r = self._post("php/GetIndex.php", {"S": "", "max": 100, "start": 0}).get("response") or []
        return [it.get("idSystem") for it in r if isinstance(it, dict) and it.get("idSystem")]

    def pool(self, pid):
        r = self._post("php/GetPoolDetails.php", {"poolID": pid, "lang": LANG}).get("response")
        return r[0] if isinstance(r, list) and r else r


def probe_value(pool, ptype):
    for p in pool.get("probes") or []:
        if p.get("type") == ptype:
            v = p.get("filteredValue")
            if v is None or v <= -1000:      # Klereo sentinel for "not available"
                return None
            return v
    return None


def out_status(pool, index):
    for o in pool.get("outs") or []:
        if o.get("index") == index:
            return 1 if o.get("status") else 0
    return None


def probe_seuils(pool, ptype):
    """Return (seuilMin, seuilMax) - the system's regulation limits for a probe."""
    for p in pool.get("probes") or []:
        if p.get("type") == ptype:
            return p.get("seuilMin"), p.get("seuilMax")
    return None, None


def out_mode(pool, index):
    for o in pool.get("outs") or []:
        if o.get("index") == index:
            return o.get("mode")
    return None


def out_total(pool, index):
    """Lifetime run-time (seconds) of an output - the odometer we diff per day."""
    for o in pool.get("outs") or []:
        if o.get("index") == index:
            return o.get("totalTime")
    return None


# --------------------------------------------------------------------------
# Poll + alert
# --------------------------------------------------------------------------
def _clean(s):
    """Trim spaces/newlines and strip non-breaking spaces (\\xa0) that sneak in
    when pasting a Gmail app password from Google's UI."""
    return (s or "").replace("\xa0", " ").strip()


def send_email(subject, body):
    host = _clean(cfg_get("SMTP_HOST"))
    user = _clean(cfg_get("SMTP_USER"))
    to   = _clean(cfg_get("ALERT_TO"))
    frm  = _clean(cfg_get("ALERT_FROM") or user)
    # Gmail app passwords are 16 chars with no spaces; remove ALL whitespace
    # (incl. the non-breaking space Google's UI sometimes inserts).
    pw = re.sub(r"\s+", "", (cfg_get("SMTP_PASS") or "").replace("\xa0", " "))
    if not all([host, user, pw, to]):
        print("[email] not configured; skipping"); return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = frm
    msg["To"] = to
    msg.set_content(body)
    with smtplib.SMTP(host, int(_clean(cfg_get("SMTP_PORT")) or "587"),
                      timeout=HTTP_TIMEOUT) as s:
        s.starttls(); s.login(user, pw); s.send_message(msg)
    print(f"[email] sent to {to}")
    return True


def poll_once():
    login = cfg_get("KLEREO_LOGIN"); password = cfg_get("KLEREO_PASSWORD")
    if not login or not password:
        raise KlereoError("KLEREO_LOGIN / KLEREO_PASSWORD not set")
    k = Klereo(); k.login(login, password)
    pid = cfg_get("KLEREO_POOL_ID")
    if not pid:
        ids = k.pool_ids()
        if len(ids) != 1:
            raise KlereoError(f"Set KLEREO_POOL_ID (pools: {ids})")
        pid = ids[0]
    pool = k.pool(pid)
    params = pool.get("params") or {}
    extra  = pool.get("ExtraParams") or {}

    total = extra.get("HybChl_TotalTime")
    debit = params.get("Chlore_Debit")
    total = float(total) if total is not None else None
    debit = float(debit) if debit is not None else None

    baseline = kv_get("baseline_total_time")
    used = None
    if baseline is not None and total is not None and debit is not None:
        used = (total - baseline) * debit / 36000.0

    # Today's dosed volume (independent of the bottle baseline):
    #   today_mL = HybChl_TodayTime * Chlore_Debit / 36
    today_time = extra.get("HybChl_TodayTime")
    today_time = float(today_time) if today_time is not None else None
    today_ml = (today_time * debit / 36.0) if (today_time is not None and debit is not None) else None

    ph_min, ph_max = probe_seuils(pool, T_PH)
    orp_min, orp_max = probe_seuils(pool, T_REDOX)

    # Salt cell chlorine produced: Elec_GramDone is already in grams and reads 0
    # when the cell is idle (identified from /api/raw). mL-liquid-equivalent uses
    # the configurable liquid strength (g active Cl per litre).
    kv_set("last_params", params)      # kept for /api/raw diagnostics
    kv_set("last_extra", extra)
    salt_g = params.get("Elec_GramDone")
    salt_g = float(salt_g) if salt_g is not None else None
    gpl = float(kv_get("liquid_cl_gpl", 48.0))
    salt_ml = (salt_g / gpl * 1000.0) if (salt_g is not None and gpl > 0) else None

    reading = {
        "ts": now_iso(),
        "nickname": pool.get("poolNickname"),
        "total_time": total,
        "debit": debit,
        "today_ml": today_ml,
        "ph": probe_value(pool, T_PH),
        "orp": probe_value(pool, T_REDOX),
        "temp": probe_value(pool, T_EAU),
        "salt": probe_value(pool, T_SALIN),
        "ph_min": ph_min, "ph_max": ph_max,
        "orp_min": orp_min, "orp_max": orp_max,
        "salt_g": salt_g, "salt_ml": salt_ml,
        "filtration": out_status(pool, SCHED_FILTRE),
        "filt_mode": out_mode(pool, SCHED_FILTRE),
        "filt_total": out_total(pool, SCHED_FILTRE),
        "treatment": out_status(pool, SCHED_TRAIT),
        "used_l": used,
        "suspended": pool.get("suspended"),
    }
    with db() as c:
        c.execute("INSERT INTO readings "
                  "(ts,total_time,debit,ph,orp,temp,salt,filtration,used_l,filt_total) "
                  "VALUES (?,?,?,?,?,?,?,?,?,?)",
                  (reading["ts"], total, debit, reading["ph"], reading["orp"],
                   reading["temp"], reading["salt"], reading["filtration"], used,
                   reading["filt_total"]))
    kv_set("last_reading", reading)
    kv_set("last_ok", now_iso())
    kv_set("last_error", None)

    # Two-tier bottle alerts, by litres REMAINING:
    #   warn  (e.g. 5 L left)   -> "check you have a spare"
    #   final (e.g. 0.5 L left) -> "change the bottle now"
    if used is not None:
        bottle = float(kv_get("bottle_l", DEF_BOTTLE))
        warn_rem  = float(kv_get("warn_remaining_l", 5.0))
        final_rem = float(kv_get("final_remaining_l", 0.5))
        remaining = bottle - used
        pool_name = reading["nickname"]

        def _alert(subject, body, flag):
            try:
                if send_email(subject, body):
                    kv_set(flag, 1); kv_set(flag + "_at", now_iso())
            except Exception as e:
                print("[email] error:", e)

        if remaining <= final_rem:
            if not kv_get("notified_final"):
                _alert("Klereo: CHANGE the chlorine bottle now",
                       f"Your pool '{pool_name}' has ~{max(remaining,0):.1f} L of liquid "
                       f"chlorine left (used {used:.1f} L of a {bottle:.0f} L bottle).\n\n"
                       f"Change the bottle now. When you fit the new one, press "
                       f"'New bottle fitted' on the dashboard.\n", "notified_final")
                kv_set("notified_warn", 1)   # suppress the redundant earlier warning
        elif remaining <= warn_rem:
            if not kv_get("notified_warn"):
                _alert("Klereo: chlorine getting low - check you have a spare",
                       f"Your pool '{pool_name}' has ~{remaining:.1f} L of liquid chlorine "
                       f"left (used {used:.1f} L of a {bottle:.0f} L bottle).\n\n"
                       f"Make sure you have a spare 20 L bottle ready - you'll get a second "
                       f"email when it's time to actually change it.\n", "notified_warn")
    return reading


def poller_loop():
    while True:
        try:
            with _lock:
                r = poll_once()
            print(f"[{r['ts']}] polled: pH={r['ph']} ORP={r['orp']} temp={r['temp']} "
                  f"used={r['used_l']}")
        except Exception as e:
            print("[poll] error:", e)
            kv_set("last_error", f"{now_iso()}: {e}")
        try:
            mins = float(kv_get("poll_minutes", POLL_MINUTES))
        except (TypeError, ValueError):
            mins = POLL_MINUTES
        time.sleep(max(1.0, mins) * 60)


# --------------------------------------------------------------------------
# Web dashboard (stdlib http.server)
# --------------------------------------------------------------------------
ICON_PNG = b64decode(
 "iVBORw0KGgoAAAANSUhEUgAAALQAAAC0CAYAAAA9zQYyAAAGcElEQVR4nO3cu5EUVxSH8TNbkIMCwODh"
 "UBABIWDgbAZyFYAMBYBLBjgYCoEIoHBgZRAAOahKYGgbmqYf9/Z9/8/3MyWYvXf62zN3Zrq4WCeePnv"
 "xtfUacN67t28urddgZtZsEQSsrVXgVX/oXsSfP32suRRkdu/ho83/VzPu4j9oK2IC1rYVeOm4iz34Wsh"
 "E7NNa3KXCzv6gy5CJGHPLuHOHnfXB5jETMvbMw84ZdZYHYirjjBLTOvkBmMpIlXNaX6X8ZWJGDvN2Ur+"
 "fOPXbQMgoJXVaR09oYkZJqdM6KmhiRg0pUQcHTcyo6WzU0UcOYkYtZ1oLCnr6DSFm1DY1FzqlD4MmZrQ"
 "WE/Vu0NyzjN4cNRl05GA6o7XQBjeD5qiB3oQcPVaD5qiB3m01unvkYDqjN0dN/hI0Rw30bu/okXS3HdC"
 "bn4JmOmMUW1OaCQ0pBA0p34PmuIHRrB07mNCQQtCQcmXGcQPjWh47mNCQQtCQQtCQcuH8DAXTv+fBhIY"
 "UgoYUgoYUgq7kwev3rZfgAkFDCkFXME1npnR5BA0pBF3YciozpcsiaEgh6IK2pjFTuhyChhSCLuRoCjOl"
 "yyBoSCHoAkKnL1M6P4KGFIKGFILOLPYYwbEjL4KGFILO6Oy0ZUrnQ9CQQtCZpE5ZpnQeBA0pBJ1BrunKl"
 "E5H0JBC0IlyT1WmdBqChhSChhSCTlDqeMCx4zyChhSCPqn0FGVKn0PQkELQJ9SankzpeAQNKQQdqfbUZE"
 "rHIWhIIegIraYlUzocQUMKQUMKQQdq/bLf+uePgqAhhaAD9DIde1lHzwgaUgj6QG9Tsbf19IagIYWgd/Q6"
 "DXtdVw8IGlIIekPvU7D39bVC0JBC0CtGmX6jrLMmgoYUgoYUgl4Y7WV8tPWWRtAzo8Yx6rpLIOhbo0cx+v"
 "pzIWjTiUFlHyncB60Wgdp+YrkOWvXiq+4rhOugocdt0OpTTH1/W1wG7eVie9nnnMugoctd0N6mlrf9ugra"
 "28WdeNr3ndYLGNVfD+6+Wv63P2/+/b3FWvADQUdaC3n5/wi7HTdHjhwvu3sxn/lzNXk5drgJOlVspD1G7Q"
 "FBBzgbJ1HX5yJoLy+3Rzw8Dy6CTpE6ZZnSdRE0pBA0pMgH7eHcGEP9+ZAPGr4Q9IHUb/341rAugoYUgg5w"
 "dsoynesj6ECxcRJzGwQdITRSYm6H20cjTbFyP3SfCPok4u0TRw5IIWhIkQ/65vpJ6yV0Rf35kA8avhA0pB"
 "A0pLgIWv3cGMrD8+AiaPhB0JDiJmgPL7d7vOzfTdDwgaAhxVXQXl52lzzt21XQZr4urpm//boLGtpcBu1la"
 "nnZ55zLoM30L7b6/ra4DRqaXAetOsVU9xXCddBmehdfbT+x3AdtphOByj5SEPSt0WMYff25EPTMqFGMuu4S"
 "CHphtDhGW29p7v6hmecvPxz+mb+vH5tZ3/84+BRy0H7+eFx2MR25PH324quZ2edPH1uvpZiQi77ln9/+y7e"
 "QTO5/Of/Cqhr3vYePzEx8QqeEPJni6SHslJAn03OiGrZk0DlCXmoZdo6Ql1TDlntTWCLmuftfrqq9Ebu5fl"
 "Ik5rnSz1dtUmfomhdnPtlyvnmc/7K02s+IpjP0xcxMIeqYi7938XI8TkzgW9O+p/30bor53ds3F4mgS3101"
 "eojMbX9lCYV9NFFynGBavyMmj+r5n5qmAc99JvCWhfm6HFynXXV9tPCldn/ZZv9KF1B7inTemqp7SeX+XQ2"
 "G/hju70pUupi5XrzFfv3R9xPK8MGvaX05Kk92dT2U9r3oEc6dvQ6Pc6uS20/tSyPG2a3n0NPRvu0Y/mE15w"
 "285+d6+eq7ac0uaDh21rQP52hRzp2wLe1mM0E3xTCt1+CZkqjd1vT2exgQhM1enPU5GrQa+UDPdlqdHNCc/R"
 "Ab/aOGpOgN4VEjdZCG9wNmqMHenPU5OGE5uiB1kKOGpOgIwdRo5WYmM1OfLFC1KjlTGvBQc9/Q4gapc0bi3k"
 "vFzWhiRo1nI3Z7MSRg6hRUkrMZovbR2NNt5uaccsp0qSGPEm6245pjRxyxWyWOKEn80ltxrRGmOUQzPFFXtZ"
 "vAjmCIFTOqTyX/attpjX2lJjKc8Xu1ViGbUbcXq29vyp1n1Dxm4/WwjYjbnVbHxKUvuGt6t10W3GbEfjo9j7"
 "lqnnXZrPbQ/fixvha3Xrczf3OBD62Xu6d/waR27vgYdrrdgAAAABJRU5ErkJggg==")

PAGE = b"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="icon" href="/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Pool Stats">
<meta name="theme-color" content="#0f172a">
<title>Pool Stats</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
 body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#0f172a;color:#e2e8f0}
 .wrap{max-width:900px;margin:0 auto;padding:18px}
 h1{font-size:20px;margin:6px 0 2px} .sub{color:#94a3b8;font-size:13px;margin-bottom:14px}
 .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
 .card{background:#1e293b;border-radius:12px;padding:14px}
 .card .lbl{color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em}
 .card .val{font-size:26px;font-weight:600;margin-top:4px}
 .card .unit{font-size:14px;color:#94a3b8}
 .on{color:#4ade80} .off{color:#f87171}
 .ok{color:#4ade80} .warn{color:#fbbf24} .bad{color:#f87171}
 .status{font-size:15px;color:#cbd5e1;margin-bottom:14px}
 h1 img{width:28px;height:28px;vertical-align:middle;border-radius:6px;margin-right:9px}
 button svg{vertical-align:-3px;margin-right:7px}
 .bar{position:relative;height:14px;background:linear-gradient(90deg,#22c55e,#eab308,#ef4444);border-radius:8px;overflow:hidden;margin-top:8px}
 .bar>div{position:absolute;top:0;right:0;height:100%;background:#334155}
 .panel{background:#1e293b;border-radius:12px;padding:16px;margin-top:16px}
 button{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:9px 14px;font-size:14px;cursor:pointer}
 button.ghost{background:#334155}
 input{background:#0f172a;border:1px solid #334155;color:#e2e8f0;border-radius:6px;padding:7px;width:90px}
 label{font-size:13px;color:#cbd5e1;margin-right:6px}
 .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:8px}
 .setrow{display:flex;justify-content:space-between;align-items:center;margin:10px 0}
 .setrow label{margin:0} .setrow input{width:70px;text-align:right} .setrow .u{color:#94a3b8;margin-left:6px}
 .err{background:#7f1d1d;color:#fecaca;padding:8px 12px;border-radius:8px;margin-top:12px;font-size:13px}
 .toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%) translateY(80px);background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:12px 18px;border-radius:10px;font-size:14px;box-shadow:0 8px 24px rgba(0,0,0,.45);opacity:0;transition:all .25s;z-index:30;max-width:90%;text-align:center}
 .toast.show{transform:translateX(-50%) translateY(0);opacity:1}
 .toast.ok{border-color:#16a34a} .toast.bad{border-color:#dc2626}
 .seg{display:inline-flex;background:#0f172a;border:1px solid #334155;border-radius:8px;overflow:hidden}
 .seg button{background:transparent;color:#cbd5e1;border:0;padding:6px 12px;font-size:13px;cursor:pointer}
 .seg button.active{background:#2563eb;color:#fff}
 .modal{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:center;justify-content:center;z-index:40;padding:20px}
 .modal.show{display:flex}
 .modalcard{background:#1e293b;border:1px solid #334155;border-radius:14px;padding:20px;width:320px;max-width:100%}
</style></head><body>
 <div id="ptr" style="position:fixed;top:0;left:0;right:0;text-align:center;padding:8px;color:#94a3b8;font-size:13px;transform:translateY(-40px);transition:transform .15s;z-index:6">&#8595; pull to refresh</div>
 <div id="toast" class="toast"></div>
 <div id="bottleModal" class="modal"><div class="modalcard">
   <div style="font-size:16px;font-weight:600;margin-bottom:14px">Register new bottle</div>
   <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer"><input type="checkbox" id="bmNow" checked onchange="bmToggle()"> Fitted now</label>
   <div id="bmWhen" style="margin-top:12px;display:none">
     <div style="font-size:13px;color:#94a3b8;margin-bottom:5px">Date &amp; time fitted (past only):</div>
     <input type="datetime-local" id="bmTime" style="width:100%;box-sizing:border-box;background:#0f172a;border:1px solid #334155;color:#e2e8f0;border-radius:6px;padding:9px;font-size:14px">
   </div>
   <div style="display:flex;gap:10px;margin-top:18px">
     <button type="button" onclick="closeBottle()" class="ghost" style="flex:1;margin:0">Cancel</button>
     <button type="button" onclick="confirmBottle()" style="flex:1">Confirm</button>
   </div>
 </div></div>
 <div class="wrap">
 <a href="/config" title="Settings" style="position:fixed;top:10px;right:58px;z-index:5;font-size:22px;line-height:1;padding:6px 10px;text-decoration:none;color:#fff;background:#334155;border-radius:8px">&#9881;</a>
 <button id="refbtn" class="ghost" title="Refresh" style="position:fixed;top:10px;right:10px;z-index:5;font-size:18px;line-height:1;padding:8px 12px" onclick="refresh()">&#8635;</button>
 <h1 id="title"><img src="/apple-touch-icon.png" alt="">Pool Stats <span id="nick" style="font-size:14px;color:#94a3b8;font-weight:400"></span></h1>
 <div class="status" id="sub">loading...</div>
 <div id="err"></div>
 <div class="grid">
  <div class="card"><div class="lbl">pH</div><div class="val" id="ph">-</div><div class="unit">target 6.6-8.0</div></div>
  <div class="card"><div class="lbl">ORP / Redox</div><div class="val" id="orp">-</div><div class="unit">mV</div></div>
  <div class="card"><div class="lbl">Water temp</div><div class="val" id="temp">-</div><div class="unit">&deg;C</div></div>
  <div class="card"><div class="lbl">Filtration</div><div class="val" id="filt">-</div><div class="unit" id="filtmode"></div></div>
  <div class="card"><div class="lbl">Dosed today</div><div class="val" id="today">-</div><div class="unit">mL liquid Cl</div></div>
  <div class="card"><div class="lbl">Salt cell today</div><div class="val" id="saltgen">-</div><div class="unit" id="saltml"></div></div>
 </div>
 <div class="panel">
  <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase">Liquid chlorine bottle</div>
  <div class="val" style="font-size:30px;font-weight:700"><span id="used">-</span> <span class="unit">L used</span>
     &nbsp; <span class="unit">/ <span id="rem">-</span> L left of <span id="bottle">-</span> L</span></div>
  <div class="bar"><div id="barfill" style="width:0%"></div></div>
  <div class="sub" id="bottleinfo" style="margin-top:10px"></div>
  <button type="button" onclick="openBottle()"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 2h6M10 2v3.5L7 9v11a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2V9l-3-3.5V2"/><path d="M7 13h10"/></svg>Register new bottle</button>
 </div>
 <div class="panel">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase">pH &amp; Redox</div>
    <div class="seg">
      <button id="segR24" class="active" type="button" onclick="setRange(1)">24h</button>
      <button id="segR7" type="button" onclick="setRange(7)">7d</button>
      <button id="segR30" type="button" onclick="setRange(30)">30d</button>
    </div>
  </div>
  <div style="position:relative;height:200px"><canvas id="chart"></canvas></div>
 </div>
 <div class="panel">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase">Chlorine used</div>
    <div class="seg">
      <button id="segDay" class="active" type="button" onclick="setUsage('day')">Day</button>
      <button id="segWeek" type="button" onclick="setUsage('week')">Week</button>
      <button id="segMonth" type="button" onclick="setUsage('month')">Month</button>
    </div>
  </div>
  <canvas id="usageChart" height="120"></canvas>
  <div class="sub" id="usageSummary" style="margin-top:8px"></div>
 </div>
 <div class="panel">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase">Filter runtime &amp; cost</div>
    <div class="seg">
      <button id="fsegDay" class="active" type="button" onclick="setFilterPeriod('day')">Day</button>
      <button id="fsegWeek" type="button" onclick="setFilterPeriod('week')">Week</button>
      <button id="fsegMonth" type="button" onclick="setFilterPeriod('month')">Month</button>
    </div>
  </div>
  <canvas id="filterChart" height="120"></canvas>
  <div class="sub" id="filterSummary" style="margin-top:8px"></div>
 </div>
 <div class="panel" id="coverPanel">
  <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase;margin-bottom:6px">Pool cover</div>
  <div class="val" id="coverState" style="font-size:22px">-</div>
  <div class="sub" id="coverInfo" style="margin:4px 0 8px"></div>
  <img id="coverImg" style="width:100%;border-radius:8px;display:none" alt="">
 </div>
</div>
<script>
async function load(){
 const s = await (await fetch('/api/status')).json();
 const r = s.reading || {};
 document.getElementById('nick').textContent = (r.nickname||'');
 document.getElementById('sub').textContent =
    'Updated ' + relTime(s.last_ok) + '  |  every ' + s.poll_minutes + ' min'
    + (s.notified_final ? '  |  CHANGE-BOTTLE alert sent' : (s.notified_warn ? '  |  low-chlorine alert sent' : ''));
 document.getElementById('err').innerHTML = s.last_error ? '<div class="err">Last error: '+s.last_error+'</div>' : '';
 const set=(id,v,d)=>document.getElementById(id).textContent=(v==null?'-':(typeof v==='number'?v.toFixed(d):v));
 set('temp', r.temp, 1); set('today', r.today_ml, 0);
 setZone('ph', r.ph, r.ph_min, r.ph_max, 2);
 setZone('orp', r.orp, r.orp_min, r.orp_max, 0);
 set('saltgen', r.salt_g, 1);
 document.getElementById('saltml').textContent = (r.salt_ml!=null) ? ('g Cl  ~ '+r.salt_ml.toFixed(0)+' mL liquid') : 'g Cl';
 const f=document.getElementById('filt');
 if(r.filtration==null){f.textContent='-';f.className='val';}
 else{f.textContent=r.filtration? 'ON':'OFF'; f.className='val '+(r.filtration?'on':'off');}
 document.getElementById('filtmode').textContent = modeName(r.filt_mode);
 document.getElementById('bottle').textContent=(s.bottle_l==null?'-':s.bottle_l);
 if(r.used_l!=null){
   const used=r.used_l, bottle=s.bottle_l||20, rem=Math.max(bottle-used,0);
   document.getElementById('used').textContent=used.toFixed(1);
   document.getElementById('rem').textContent=rem.toFixed(1);
   document.getElementById('barfill').style.width=Math.max(0,100-used/bottle*100)+'%';
   document.getElementById('bottleinfo').textContent=
      'fitted '+relTime(s.bottle_fitted_at)+'   |   alerts at '+s.warn_remaining_l+' L and '+s.final_remaining_l+' L left';
 } else {
   document.getElementById('used').textContent='no baseline';
   document.getElementById('bottleinfo').textContent='Tap "Register new bottle" to start tracking.';
 }
 // Pool cover (from the home camera bridge)
 const cs=document.getElementById('coverState'), ci=document.getElementById('coverInfo'), cimg=document.getElementById('coverImg');
 if(s.cover_ts){
   cs.textContent = s.cover_state ? s.cover_state.toUpperCase() : 'image received';
   cs.className = 'val '+(s.cover_state==='open'?'on':(s.cover_state==='closed'?'off':''));
   ci.textContent = 'updated '+relTime(s.cover_ts);
   cimg.src='/api/cover-latest.jpg?t='+Date.now(); cimg.style.display='block';
 } else {
   cs.textContent='no data'; cs.className='val';
   ci.textContent='Waiting for the home camera bridge...';
 }
 lastReading = r;
 drawChart();
 loadUsage();
 filterCfg={pump:(s.pump_kw||1.1), price:(s.price_kwh||0.25), cur:(s.currency||'')};
 loadFilter();
}
function relTime(iso){
 if(!iso) return 'never';
 const d=new Date(iso); if(isNaN(d)) return iso;
 const now=new Date(), y=new Date(); y.setDate(now.getDate()-1);
 const same=(a,b)=>a.toDateString()===b.toDateString();
 const hm=d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'});
 if(same(d,now)) return 'today '+hm;
 if(same(d,y)) return 'yesterday '+hm;
 return d.toLocaleDateString([], {weekday:'short',day:'numeric',month:'short'})+' '+hm;
}
function zone(v,min,max){
 if(v==null||min==null||max==null) return '';
 if(v<min||v>max) return 'bad';
 const rng=max-min, m=rng*0.12;
 if(v<=min+m||v>=max-m) return 'warn';
 return 'ok';
}
function setZone(id,v,min,max,dec){
 const el=document.getElementById(id);
 el.textContent=(v==null?'-':v.toFixed(dec));
 el.className='val '+zone(v,min,max);
}
function modeName(m){
 const names={0:'Manual',1:'Scheduled',2:'Timer',3:'Regulated',4:'Cloned',5:'Special',6:'Test',8:'Pulse'};
 return (m==null)?'':(names[m]||('mode '+m));
}
function showToast(msg, ok){
 const t=document.getElementById('toast');
 t.textContent=msg; t.className='toast show '+(ok===false?'bad':'ok');
 clearTimeout(window._tt); window._tt=setTimeout(()=>{t.className='toast';},3500);
}
async function postAction(url, body){
 const opt={method:'POST'};
 if(body){opt.headers={'Content-Type':'application/x-www-form-urlencoded'}; opt.body=body;}
 try{ const r=await fetch(url,opt); return await r.json(); }
 catch(e){ return {ok:false, message:'Network error'}; }
}
function localNowStr(){ const d=new Date(); const p=n=>String(n).padStart(2,'0');
 return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+'T'+p(d.getHours())+':'+p(d.getMinutes()); }
function bmToggle(){ document.getElementById('bmWhen').style.display=document.getElementById('bmNow').checked?'none':'block'; }
function openBottle(){
 document.getElementById('bmNow').checked=true;
 const t=document.getElementById('bmTime'); t.value=localNowStr(); t.max=localNowStr();
 bmToggle();
 document.getElementById('bottleModal').classList.add('show');
}
function closeBottle(){ document.getElementById('bottleModal').classList.remove('show'); }
async function confirmBottle(){
 let body='';
 if(!document.getElementById('bmNow').checked){
   const t=document.getElementById('bmTime').value;
   if(!t){ showToast('Pick a date and time.', false); return; }
   const chosen=new Date(t).getTime();
   if(chosen>Date.now()){ showToast('Date must be in the past.', false); return; }
   body='at_ms='+chosen;
 }
 closeBottle();
 const r=await postAction('/new-bottle', body); showToast(r.message, r.ok); load();
}
let usageChart, usagePeriod='day', usageData=[];
async function loadUsage(){
 try{ usageData = await (await fetch('/api/usage')).json(); }catch(e){ usageData=[]; }
 drawUsage();
}
function setUsage(p){
 usagePeriod=p;
 ['Day','Week','Month'].forEach(x=>document.getElementById('seg'+x).classList.toggle('active', x.toLowerCase()===p));
 drawUsage();
}
function bucketUsage(){
 if(usagePeriod==='day') return usageData.slice(-30).map(d=>({label:d.date.slice(5), litres:d.litres}));
 const map={};
 usageData.forEach(d=>{
   let key;
   if(usagePeriod==='month'){ key=d.date.slice(0,7); }
   else { const dt=new Date(d.date+'T00:00:00'); const off=(dt.getDay()+6)%7;
          const mon=new Date(dt); mon.setDate(dt.getDate()-off); key=mon.toISOString().slice(0,10); }
   map[key]=(map[key]||0)+d.litres;
 });
 const keys=Object.keys(map).sort();
 const sliced = usagePeriod==='month'? keys.slice(-12) : keys.slice(-16);
 return sliced.map(k=>({label: usagePeriod==='week'? k.slice(5) : k, litres:+map[k].toFixed(2)}));
}
function drawUsage(){
 const b=bucketUsage();
 const total=b.reduce((s,x)=>s+x.litres,0);
 document.getElementById('usageSummary').textContent = b.length
   ? ('total shown: '+total.toFixed(1)+' L   |   latest '+usagePeriod+': '+b[b.length-1].litres.toFixed(2)+' L')
   : 'No usage data yet - this builds up as the monitor runs (needs 2+ days).';
 const ctx=document.getElementById('usageChart');
 const data={labels:b.map(x=>x.label), datasets:[{label:'L used', data:b.map(x=>x.litres), backgroundColor:'#f59e0b', borderRadius:4}]};
 const opts={responsive:true, plugins:{legend:{display:false}},
   scales:{y:{beginAtZero:true,title:{display:true,text:'L'},grid:{color:'#334155'}},
           x:{ticks:{maxTicksLimit:10,color:'#94a3b8'},grid:{display:false}}}};
 if(usageChart) usageChart.destroy();
 usageChart=new Chart(ctx,{type:'bar',data,options:opts});
}
let filterChart, filterPeriod='day', filterData=[], filterCfg={pump:1.1,price:0.25,cur:''};
async function loadFilter(){
 try{ filterData = await (await fetch('/api/filter-usage')).json(); }catch(e){ filterData=[]; }
 drawFilter();
}
function setFilterPeriod(p){
 filterPeriod=p;
 ['Day','Week','Month'].forEach(x=>document.getElementById('fseg'+x).classList.toggle('active', x.toLowerCase()===p));
 drawFilter();
}
function bucketFilter(){
 if(filterPeriod==='day') return filterData.slice(-30).map(d=>({label:d.date.slice(5), hours:d.hours}));
 const map={};
 filterData.forEach(d=>{
   let key;
   if(filterPeriod==='month'){ key=d.date.slice(0,7); }
   else { const dt=new Date(d.date+'T00:00:00'); const off=(dt.getDay()+6)%7;
          const mon=new Date(dt); mon.setDate(dt.getDate()-off); key=mon.toISOString().slice(0,10); }
   map[key]=(map[key]||0)+d.hours;
 });
 const keys=Object.keys(map).sort();
 const sliced = filterPeriod==='month'? keys.slice(-12) : keys.slice(-16);
 return sliced.map(k=>({label: filterPeriod==='week'? k.slice(5) : k, hours:+map[k].toFixed(1)}));
}
function drawFilter(){
 const b=bucketFilter();
 const totalH=b.reduce((s,x)=>s+x.hours,0);
 const cost=totalH*filterCfg.pump*filterCfg.price;
 document.getElementById('filterSummary').textContent = b.length
   ? ('total shown: '+totalH.toFixed(1)+' h  ~ '+filterCfg.cur+cost.toFixed(2)
      +'   ('+filterCfg.pump+' kW @ '+filterCfg.cur+filterCfg.price+'/kWh)')
   : 'No filter data yet - builds up as the monitor runs (needs 2+ days).';
 const data={labels:b.map(x=>x.label), datasets:[{label:'hours', data:b.map(x=>x.hours), backgroundColor:'#38bdf8', borderRadius:4}]};
 const opts={responsive:true, plugins:{legend:{display:false}},
   scales:{y:{beginAtZero:true,title:{display:true,text:'h'},grid:{color:'#334155'}},
           x:{ticks:{maxTicksLimit:10,color:'#94a3b8'},grid:{display:false}}}};
 if(filterChart) filterChart.destroy();
 filterChart=new Chart(document.getElementById('filterChart'),{type:'bar',data,options:opts});
}
let chart, chartRange=1, lastReading={};
function setRange(d){
 chartRange=d;
 [[1,'segR24'],[7,'segR7'],[30,'segR30']].forEach(a=>document.getElementById(a[1]).classList.toggle('active', a[0]===d));
 drawChart();
}
async function drawChart(){
 const r=lastReading||{};
 let h=[];
 try{ h=await (await fetch('/api/history?days='+chartRange)).json(); }catch(e){}
 const fmt=x=>{const d=new Date(x.ts); return chartRange<=1
    ? d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})
    : (d.getDate()+'/'+(d.getMonth()+1)); };
 const data={labels:h.map(fmt), datasets:[
   {label:'pH', data:h.map(x=>x.ph), yAxisID:'y1', borderColor:'#38bdf8', borderWidth:2, tension:.3, pointRadius:0},
   {label:'ORP', data:h.map(x=>x.orp), yAxisID:'y2', borderColor:'#a78bfa', borderWidth:2, tension:.3, pointRadius:0},
 ]};
 const y1={position:'left',title:{display:true,text:'pH',color:'#94a3b8'},grid:{color:'#334155'},ticks:{color:'#cbd5e1',font:{size:12}}};
 const y2={position:'right',title:{display:true,text:'mV',color:'#94a3b8'},grid:{display:false},ticks:{color:'#cbd5e1',font:{size:12}}};
 if(r.ph_min!=null && r.ph_max!=null){ y1.min=+(r.ph_min*0.95).toFixed(2); y1.max=+(r.ph_max*1.05).toFixed(2); }
 if(r.orp_min!=null && r.orp_max!=null){ y2.min=Math.round(r.orp_min*0.95); y2.max=Math.round(r.orp_max*1.05); }
 const opts={responsive:true, maintainAspectRatio:false, interaction:{mode:'index',intersect:false},
   scales:{y1, y2, x:{ticks:{maxTicksLimit:6,color:'#94a3b8',font:{size:11},maxRotation:0,autoSkip:true},grid:{display:false}}},
   plugins:{legend:{labels:{color:'#cbd5e1',boxWidth:12,font:{size:13}}}}};
 if(chart) chart.destroy();
 chart=new Chart(document.getElementById('chart'),{type:'line',data,options:opts});
}
async function refresh(){
 const ptr=document.getElementById('ptr');
 ptr.textContent='Refreshing...'; ptr.style.transform='translateY(0)';
 try{ await fetch('/poll-now',{method:'POST'}); }catch(e){}
 await load();
 ptr.style.transform='translateY(-40px)';
 setTimeout(()=>{ptr.textContent='\\u2193 pull to refresh';},300);
}
// Pull-to-refresh (iOS home-screen apps disable Safari's native one)
let ptrStartY=null;
addEventListener('touchstart',e=>{ ptrStartY = (scrollY<=0)? e.touches[0].clientY : null; },{passive:true});
addEventListener('touchmove',e=>{
 if(ptrStartY==null) return;
 const dy=e.touches[0].clientY-ptrStartY;
 if(dy>0){ document.getElementById('ptr').style.transform='translateY('+Math.min(dy-40,12)+'px)'; }
},{passive:true});
addEventListener('touchend',e=>{
 if(ptrStartY==null) return;
 const dy=e.changedTouches[0].clientY-ptrStartY;
 if(dy>70){ refresh(); } else { document.getElementById('ptr').style.transform='translateY(-40px)'; }
 ptrStartY=null;
},{passive:true});
load(); setInterval(load, 60000);
</script></body></html>"""


def login_html(error=""):
    err = ('<div style="color:#f87171;font-size:13px;margin-bottom:8px">'
           + error + '</div>') if error else ""
    return ("""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Pool">
<meta name="theme-color" content="#0f172a"><title>Pool - Login</title>
<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;background:#0f172a;color:#e2e8f0;
display:flex;min-height:100vh;margin:0;align-items:center;justify-content:center}
form{background:#1e293b;padding:26px;border-radius:14px;width:260px}
h1{font-size:18px;margin:0 0 16px}
input{width:100%;box-sizing:border-box;background:#0f172a;border:1px solid #334155;color:#e2e8f0;
border-radius:8px;padding:10px;margin-bottom:10px;font-size:15px}
button{width:100%;background:#2563eb;color:#fff;border:0;border-radius:8px;padding:11px;font-size:15px}
</style></head><body>
<form method="post" action="/login">
 <h1>&#128167; Pool Monitor</h1>""" + err + """
 <input name="username" type="text" placeholder="Username" value="admin" autocomplete="username">
 <input name="password" type="password" placeholder="Password" autocomplete="current-password">
 <button type="submit">Log in</button>
</form></body></html>""")


def config_html():
    import html as _html
    def val(k):
        v = cfg_get(k)
        return _html.escape("" if v is None else str(v), quote=True)
    bottle = kv_get("bottle_l", DEF_BOTTLE)
    warn   = kv_get("warn_remaining_l", 5.0)
    final  = kv_get("final_remaining_l", 0.5)
    poll   = kv_get("poll_minutes", POLL_MINUTES)
    gpl    = kv_get("liquid_cl_gpl", 48.0)
    pump   = kv_get("pump_kw", 1.1)
    price  = kv_get("price_kwh", 0.25)
    import html as _h
    curr   = _h.escape(str(kv_get("currency", "EUR")), quote=True)
    port   = val("SMTP_PORT") or "587"
    head = ("""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-title" content="Pool">
<meta name="theme-color" content="#0f172a"><title>Pool - Settings</title>
<style>
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#0f172a;color:#e2e8f0}
.wrap{max-width:560px;margin:0 auto;padding:18px}
h1{font-size:20px;margin:8px 0 14px}
.panel{background:#1e293b;border-radius:12px;padding:16px;margin-top:14px}
.lbl2{color:#94a3b8;font-size:12px;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}
.setrow{display:flex;justify-content:space-between;align-items:center;margin:10px 0;gap:10px}
.setrow label{font-size:14px;color:#cbd5e1;white-space:nowrap}
input{background:#0f172a;border:1px solid #334155;color:#e2e8f0;border-radius:6px;padding:8px;font-size:14px}
.setrow input{flex:1;min-width:0;text-align:right} .u{color:#94a3b8;margin-left:6px}
button{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:11px 14px;font-size:14px;cursor:pointer}
button.ghost{background:#334155;margin-top:10px}
a{color:#93c5fd}
.hint{color:#94a3b8;font-size:12px;margin-top:6px}
.toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%) translateY(80px);background:#1e293b;border:1px solid #334155;color:#e2e8f0;padding:12px 18px;border-radius:10px;font-size:14px;box-shadow:0 8px 24px rgba(0,0,0,.45);opacity:0;transition:all .25s;z-index:30;max-width:90%;text-align:center}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.toast.ok{border-color:#16a34a} .toast.bad{border-color:#dc2626}
</style></head><body>
<div id="toast" class="toast"></div>
<div class="wrap">
<a href="/">&larr; Dashboard</a>
<h1>Settings</h1>""")
    fields = f"""
<div class="panel"><div class="lbl2">Alerts &amp; polling</div>
 <div class="setrow"><label>Bottle size</label><span><input id="bottle_l" type="number" step="1" value="{bottle}"><span class="u">L</span></span></div>
 <div class="setrow"><label>1st alert at</label><span><input id="warn" type="number" step="0.5" value="{warn}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>Final alert at</label><span><input id="final" type="number" step="0.1" value="{final}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>Check every</label><span><input id="poll" type="number" step="1" min="1" value="{poll}"><span class="u">min</span></span></div>
 <div class="setrow"><label>Liquid Cl strength</label><span><input id="gpl" type="number" step="1" value="{gpl}"><span class="u">g/L</span></span></div>
 <div class="hint">Used to show the salt cell's output as a liquid-chlorine mL equivalent.</div>
</div>
<div class="panel"><div class="lbl2">Filtration cost</div>
 <div class="setrow"><label>Pump power</label><span><input id="pump" type="number" step="0.1" value="{pump}"><span class="u">kW</span></span></div>
 <div class="setrow"><label>Electricity price</label><span><input id="price" type="number" step="0.01" value="{price}"><span class="u">/kWh</span></span></div>
 <div class="setrow"><label>Currency symbol</label><span><input id="currency" type="text" value="{curr}" style="width:60px"></span></div>
 <div class="hint">Used to estimate filter running costs. For a variable-speed pump, use an average kW.</div>
</div>
<div class="panel"><div class="lbl2">Klereo account</div>
 <div class="setrow"><label>Login</label><input id="KLEREO_LOGIN" type="text" value="{val('KLEREO_LOGIN')}"></div>
 <div class="setrow"><label>Pool ID</label><input id="KLEREO_POOL_ID" type="text" value="{val('KLEREO_POOL_ID')}"></div>
 <div class="setrow"><label>Password</label><input id="KLEREO_PASSWORD" type="password" placeholder="unchanged" autocomplete="off"></div>
 <div class="hint">Leave password blank to keep the current one.</div>
</div>
<div class="panel"><div class="lbl2">Email alerts</div>
 <div class="setrow"><label>SMTP host</label><input id="SMTP_HOST" type="text" value="{val('SMTP_HOST')}"></div>
 <div class="setrow"><label>SMTP port</label><input id="SMTP_PORT" type="text" value="{port}"></div>
 <div class="setrow"><label>SMTP user</label><input id="SMTP_USER" type="text" value="{val('SMTP_USER')}"></div>
 <div class="setrow"><label>Alerts to</label><input id="ALERT_TO" type="text" value="{val('ALERT_TO')}"></div>
 <div class="setrow"><label>App password</label><input id="SMTP_PASS" type="password" placeholder="unchanged" autocomplete="off"></div>
 <div class="hint">Gmail app password (16 chars). Leave blank to keep the current one.</div>
 <button class="ghost" type="button" onclick="testEmail()">Send test email</button>
</div>
<button type="button" onclick="saveConfig()" style="width:100%;margin-top:14px">Save settings</button>
<div class="hint" style="text-align:center;margin-top:8px">Saved on the server; secrets are never shown back here.</div>
<div class="panel"><div class="lbl2">Diagnostics</div>
 <a href="/api/raw" target="_blank">View raw Klereo chemistry fields (/api/raw)</a>
 <div class="hint">Handy for checking values like the salt cell (Elec_GramDone).</div>
</div>
"""
    tail = ("""
</div><script>
function showToast(msg, ok){
 const t=document.getElementById('toast');
 t.textContent=msg; t.className='toast show '+(ok===false?'bad':'ok');
 clearTimeout(window._tt); window._tt=setTimeout(()=>{t.className='toast';},3500);
}
async function postAction(url, body){
 const opt={method:'POST'};
 if(body){opt.headers={'Content-Type':'application/x-www-form-urlencoded'}; opt.body=body;}
 try{ const r=await fetch(url,opt); return await r.json(); }catch(e){ return {ok:false,message:'Network error'}; }
}
async function testEmail(){ showToast('Sending test email...'); const r=await postAction('/test-email'); showToast(r.message, r.ok); }
async function saveConfig(){
 const ids=['bottle_l','warn','final','poll','gpl','pump','price','currency','KLEREO_LOGIN','KLEREO_POOL_ID','KLEREO_PASSWORD','SMTP_HOST','SMTP_PORT','SMTP_USER','ALERT_TO','SMTP_PASS'];
 const p=new URLSearchParams();
 ids.forEach(id=>{const el=document.getElementById(id); if(el && el.value!=='') p.append(id, el.value);});
 const r=await postAction('/config', p.toString()); showToast(r.message, r.ok);
 ['KLEREO_PASSWORD','SMTP_PASS'].forEach(id=>document.getElementById(id).value='');
}
</script></body></html>""")
    return head + fields + tail


def status_payload():
    return {
        "reading": kv_get("last_reading"),
        "last_ok": kv_get("last_ok"),
        "last_error": kv_get("last_error"),
        "bottle_l": kv_get("bottle_l", DEF_BOTTLE),
        "warn_remaining_l": kv_get("warn_remaining_l", 5.0),
        "final_remaining_l": kv_get("final_remaining_l", 0.5),
        "baseline_total_time": kv_get("baseline_total_time"),
        "bottle_fitted_at": kv_get("bottle_fitted_at"),
        "notified_warn": kv_get("notified_warn"),
        "notified_final": kv_get("notified_final"),
        "poll_minutes": kv_get("poll_minutes", POLL_MINUTES),
        "cover_ts": kv_get("cover_ts"),
        "cover_state": kv_get("cover_state"),
        "pump_kw": kv_get("pump_kw", 1.1),
        "price_kwh": kv_get("price_kwh", 0.25),
        "currency": kv_get("currency", "EUR"),
    }


def history_payload(days=1):
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc).astimezone() - timedelta(days=days)).isoformat()
    with db() as c:
        rows = c.execute("SELECT ts,ph,orp,temp,used_l FROM readings WHERE ts>=? "
                         "ORDER BY ts", (cutoff,)).fetchall()
    rows = [dict(r) for r in rows]
    if len(rows) > 500:                      # downsample so mobile charts stay light
        step = len(rows) // 500 + 1
        rows = rows[::step]
    return rows


def raw_payload():
    """Chemistry-related raw params from the last poll, for diagnosing which
    field really tracks salt-cell production. Filtered to keep the dump small."""
    p = kv_get("last_params") or {}
    e = kv_get("last_extra") or {}
    pat = re.compile(r"(elec|chlor|salt|sel|prod|hyb|trait|redox|orp|couv|cover|conso)", re.I)
    out = {}
    for src, vals in (("params", p), ("ExtraParams", e)):
        if isinstance(vals, dict):
            for k, v in vals.items():
                if pat.search(k):
                    out[f"{src}.{k}"] = v
    return out


def filter_usage_payload():
    """Filtration hours per calendar day, from the filter output's run-time
    odometer (day-end minus previous day-end). Returns [{date, hours}]."""
    with db() as c:
        rows = c.execute(
            "SELECT date(ts) AS d, MAX(filt_total) AS ft FROM readings "
            "WHERE filt_total IS NOT NULL GROUP BY d ORDER BY d").fetchall()
    out, prev = [], None
    for r in rows:
        ft = r["ft"]
        if ft is None:
            continue
        if prev is not None:
            out.append({"date": r["d"], "hours": round(max((ft - prev) / 3600.0, 0), 2)})
        prev = ft
    return out[-120:]


def usage_payload():
    """Chlorine used per calendar day, derived from the lifetime odometer.
    litres(reading) = total_time * debit / 36000; daily use = day-end minus
    previous day-end. Returns [{date, litres}] oldest->newest (last 120 days)."""
    with db() as c:
        rows = c.execute(
            "SELECT date(ts) AS d, MAX(total_time) AS tt, "
            "       (SELECT debit FROM readings r2 WHERE date(r2.ts)=date(r1.ts) "
            "        AND debit IS NOT NULL ORDER BY ts DESC LIMIT 1) AS debit "
            "FROM readings r1 WHERE total_time IS NOT NULL "
            "GROUP BY d ORDER BY d").fetchall()
    out, prev = [], None
    for r in rows:
        tt, debit = r["tt"], r["debit"]
        if tt is None or debit is None:
            continue
        cum = tt * debit / 36000.0
        if prev is not None:
            out.append({"date": r["d"], "litres": round(max(cum - prev, 0), 3)})
        prev = cum
    return out[-120:]


def do_new_bottle(at_ms=None):
    """Register a new bottle. at_ms (epoch ms) backdates the baseline to the
    odometer reading nearest that time; None means 'now'."""
    if at_ms is None:
        r = kv_get("last_reading") or {}
        total = r.get("total_time")
        if total is None:
            with _lock:
                r = poll_once()
            total = r.get("total_time")
        debit = r.get("debit")
        fitted = now_iso()
    else:
        target = at_ms / 1000.0
        with db() as c:
            rows = c.execute("SELECT ts,total_time,debit FROM readings "
                             "WHERE total_time IS NOT NULL ORDER BY ts").fetchall()
        chosen = None
        for row in rows:
            try:
                ep = datetime.fromisoformat(row["ts"]).timestamp()
            except ValueError:
                continue
            if ep <= target:
                chosen = row
            else:
                break
        if chosen is None:
            chosen = rows[0] if rows else None
        if chosen is None:               # no history at all -> fall back to now
            return do_new_bottle(None)
        total = chosen["total_time"]; debit = chosen["debit"]
        fitted = datetime.fromtimestamp(target).astimezone().isoformat(timespec="minutes")
    kv_set("baseline_total_time", total)
    kv_set("debit_at_baseline", debit)
    kv_set("bottle_fitted_at", fitted)
    kv_set("notified_warn", 0)
    kv_set("notified_final", 0)
    # Re-poll so the dashboard immediately reflects usage against the new baseline
    try:
        with _lock:
            poll_once()
    except Exception:
        pass


class Handler(BaseHTTPRequestHandler):
    server_version = "KlereoMonitor"

    def log_message(self, *a):
        pass  # keep console clean; poller prints its own lines

    def _authed(self):
        if not DASH_PASS:
            return True
        # 1) long-lived cookie (set at /login) - persists the session
        ck = self.headers.get("Cookie")
        if ck:
            try:
                sc = SimpleCookie(ck)
                if "klereo_auth" in sc and sc["klereo_auth"].value == AUTH_TOKEN:
                    return True
            except Exception:
                pass
        # 2) HTTP Basic (for curl / scripts / API)
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                user, _, pw = b64decode(h[6:]).decode("utf-8", "replace").partition(":")
                return user == DASH_USER and pw == DASH_PASS
            except Exception:
                return False
        return False

    def _auth_challenge(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Klereo Monitor"')
        self.end_headers()
        self.wfile.write(b"Auth required")

    def _login_ok(self):
        self.send_response(302)
        self.send_header("Location", "/")
        self.send_header("Set-Cookie",
                         f"klereo_auth={AUTH_TOKEN}; Max-Age={COOKIE_MAXAGE}; "
                         f"Path=/; HttpOnly; SameSite=Lax")
        self.end_headers()

    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj):
        self._send(200, json.dumps(obj), "application/json")

    def _redirect(self, to="/"):
        self.send_response(302)
        self.send_header("Location", to)
        self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        # icon is public (iOS may fetch it without auth) and not sensitive
        if path in ("/apple-touch-icon.png", "/apple-touch-icon-precomposed.png",
                    "/favicon.ico", "/icon.png"):
            return self._send(200, ICON_PNG, "image/png")
        if path == "/login":
            return self._send(200, login_html())
        if not self._authed():
            # pages -> friendly login form; API -> basic-auth challenge
            if path.startswith("/api"):
                return self._auth_challenge()
            return self._redirect("/login")
        if path == "/":
            self._send(200, PAGE)
        elif path == "/config":
            self._send(200, config_html())
        elif path == "/api/status":
            self._json(status_payload())
        elif path == "/api/history":
            q = parse_qs(urlparse(self.path).query)
            try:
                days = max(1, min(90, int(q.get("days", ["1"])[0])))
            except ValueError:
                days = 1
            self._json(history_payload(days))
        elif path == "/api/usage":
            self._json(usage_payload())
        elif path == "/api/filter-usage":
            self._json(filter_usage_payload())
        elif path == "/api/raw":
            self._json(raw_payload())
        elif path == "/api/cover-latest.jpg":
            if os.path.exists(COVER_IMG):
                with open(COVER_IMG, "rb") as fh:
                    self._send(200, fh.read(), "image/jpeg")
            else:
                self._send(404, "no image yet")
        else:
            self._send(404, "not found")

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", "0") or 0)

        # Binary image ingest from the home camera bridge (read raw bytes).
        if path == "/api/cover-image":
            if not self._authed():
                return self._auth_challenge()
            data = self.rfile.read(length) if length else b""
            state = parse_qs(urlparse(self.path).query).get("state", [""])[0]
            try:
                with open(COVER_IMG, "wb") as fh:
                    fh.write(data)
                kv_set("cover_ts", now_iso())
                if state:
                    kv_set("cover_state", state)
                return self._json({"ok": True, "bytes": len(data)})
            except Exception as e:
                return self._json({"ok": False, "message": str(e)})

        body = self.rfile.read(length).decode("utf-8") if length else ""
        form = {k: v[0] for k, v in parse_qs(body).items()}

        # login is reachable without prior auth
        if path == "/login":
            if (not DASH_PASS) or (form.get("password") == DASH_PASS
                                   and form.get("username", DASH_USER) == DASH_USER):
                return self._login_ok()
            return self._send(401, login_html("Wrong username or password."))

        if not self._authed():
            return self._auth_challenge()
        try:
            if path == "/new-bottle":
                at = form.get("at_ms")
                do_new_bottle(int(at) if at else None)
                self._json({"ok": True, "message":
                            "New bottle recorded" + (" (backdated)." if at else ".")})
            elif path == "/config":
                if form.get("bottle_l"): kv_set("bottle_l", float(form["bottle_l"]))
                if form.get("warn"): kv_set("warn_remaining_l", max(0.0, float(form["warn"])))
                if form.get("final"): kv_set("final_remaining_l", max(0.0, float(form["final"])))
                if form.get("poll"): kv_set("poll_minutes", max(1.0, float(form["poll"])))
                if form.get("gpl"): kv_set("liquid_cl_gpl", max(1.0, float(form["gpl"])))
                if form.get("pump"): kv_set("pump_kw", max(0.0, float(form["pump"])))
                if form.get("price"): kv_set("price_kwh", max(0.0, float(form["price"])))
                if form.get("currency"): kv_set("currency", form["currency"][:4])
                for key in CFG_KEYS:
                    if key in form and form[key].strip() != "":
                        cfg_set(key, form[key].strip())
                self._json({"ok": True, "message": "Settings saved."})
            elif path == "/poll-now":
                with _lock:
                    poll_once()
                self._json({"ok": True, "message": "Updated from the controller."})
            elif path == "/test-email":
                try:
                    ok = send_email("Klereo Monitor: test email",
                                    "This is a test from your Klereo Monitor. "
                                    "If you received it, email alerts are working.")
                    msg = ("Test email sent - check your inbox (and spam)." if ok
                           else "Email is NOT configured (SMTP_* missing in klereo.env).")
                    self._json({"ok": bool(ok), "message": msg})
                except Exception as e:
                    self._json({"ok": False, "message": f"Email failed: {e}"})
            else:
                self._send(404, "not found")
        except Exception as e:
            kv_set("last_error", f"{now_iso()}: {e}")
            self._json({"ok": False, "message": f"Error: {e}"})


def main():
    if not DASH_PASS:
        print("WARNING: DASH_PASS not set - the dashboard is UNPROTECTED. "
              "Set DASH_PASS before allowing access from other machines.")
    init_db()
    threading.Thread(target=poller_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Klereo Monitor on http://0.0.0.0:{PORT}  (polling every {POLL_MINUTES} min)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.")
        srv.shutdown()


if __name__ == "__main__":
    main()
