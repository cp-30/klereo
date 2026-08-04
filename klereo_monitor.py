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
APP_VERSION = "2.10.0"          # bump on every change; shown at bottom of Settings

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
        # migrate older DBs that predate later columns
        cols = [r[1] for r in c.execute("PRAGMA table_info(readings)").fetchall()]
        if "filt_total" not in cols:
            c.execute("ALTER TABLE readings ADD COLUMN filt_total REAL")
        if "ph_total" not in cols:      # pH-pump lifetime odometer (seconds)
            c.execute("ALTER TABLE readings ADD COLUMN ph_total REAL")
        # bottle-change history (one row per fitted bottle, per chemical)
        c.execute("""CREATE TABLE IF NOT EXISTS bottles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chem TEXT NOT NULL,          -- 'cl' (chlorine) or 'ph' (pH-minus)
            fitted_at TEXT NOT NULL,     -- local ISO time the bottle was fitted
            baseline REAL,               -- odometer value at fit (seconds)
            factor REAL,                 -- litres per odometer-second at fit time
            size_l REAL)""")
        # PoolLab / LabCom manual water tests (one row per measurement, deduped)
        c.execute("""CREATE TABLE IF NOT EXISTS lab_tests (
            meas_id TEXT PRIMARY KEY,    -- LabCom measurement unique id
            ts TEXT,                     -- measurement timestamp (local ISO)
            account_id TEXT,
            parameter TEXT,              -- raw LabCom parameter, e.g. 'PL pH'
            param_key TEXT,              -- normalized key, e.g. 'ph','fc','cya'
            value REAL,
            unit TEXT,
            ideal_low REAL,
            ideal_high REAL,
            operator TEXT,
            comment TEXT,
            device_serial TEXT)""")
        c.execute("CREATE INDEX IF NOT EXISTS ix_lab_param_ts ON lab_tests(param_key, ts)")
    # chlorine defaults
    if kv_get("bottle_l")         is None: kv_set("bottle_l", DEF_BOTTLE)
    if kv_get("poll_minutes")     is None: kv_set("poll_minutes", POLL_MINUTES)
    if kv_get("warn_remaining_l") is None: kv_set("warn_remaining_l", 5.0)
    if kv_get("final_remaining_l") is None: kv_set("final_remaining_l", 0.5)
    if kv_get("notified_warn")    is None: kv_set("notified_warn", 0)
    if kv_get("notified_final")   is None: kv_set("notified_final", 0)
    # pH-minus bottle defaults (mirrors chlorine)
    if kv_get("ph_bottle_l")      is None: kv_set("ph_bottle_l", 20.0)
    if kv_get("ph_warn_remaining_l")  is None: kv_set("ph_warn_remaining_l", 5.0)
    if kv_get("ph_final_remaining_l") is None: kv_set("ph_final_remaining_l", 0.5)
    if kv_get("ph_notified_warn")  is None: kv_set("ph_notified_warn", 0)
    if kv_get("ph_notified_final") is None: kv_set("ph_notified_final", 0)
    # pH dosing pump flow rate, litres/hour (user-calibrated against the app)
    if kv_get("ph_pump_lph")      is None: kv_set("ph_pump_lph", 1.5)
    if kv_get("pump_kw")          is None: kv_set("pump_kw", 0.9)
    if kv_get("price_kwh")        is None: kv_set("price_kwh", 0.182)   # day / peak
    if kv_get("price_offpeak")    is None: kv_set("price_offpeak", 0.142)  # night / off-peak
    if kv_get("offpeak_window")   is None: kv_set("offpeak_window", "22:00-06:00")
    if kv_get("currency")         is None: kv_set("currency", "EUR")
    # bottle forecast: days of history to average consumption over
    if kv_get("bottle_avg_days") is None: kv_set("bottle_avg_days", 14)
    # current-weather widget (Open-Meteo, no API key). Default: pool location.
    if kv_get("weather_enabled") is None: kv_set("weather_enabled", 1)
    if kv_get("weather_lat")     is None: kv_set("weather_lat", 45.942)
    if kv_get("weather_lon")     is None: kv_set("weather_lon", 0.479)
    if kv_get("weather_place")   is None: kv_set("weather_place", "Saint-Laurent-de-Ceris")
    # PoolLab / LabCom lab-test sync
    if kv_get("labcom_poll_hours") is None: kv_set("labcom_poll_hours", 1.0)
    # re-key any lab rows to the canonical key for their parameter name, so rows
    # stored before a mapping/keyword existed (e.g. alkalinity) grade correctly.
    with db() as c:
        for row in c.execute("SELECT DISTINCT parameter FROM lab_tests").fetchall():
            pname = row["parameter"]
            key, _label, _dec = _param_meta(pname)
            c.execute("UPDATE lab_tests SET param_key=? WHERE parameter=? AND param_key<>?",
                      (key, pname, key))
        # drop any out-of-range 'OR' sentinel readings (1e6) stored by old versions
        c.execute("DELETE FROM lab_tests WHERE value IS NOT NULL AND ABS(value) >= 100000")
    # one-time migration: fold the old single chlorine baseline into bottles[]
    _migrate_legacy_bottle()


def _migrate_legacy_bottle():
    if kv_get("bottles_migrated"):
        return
    base = kv_get("baseline_total_time")
    if base is not None:
        with db() as c:
            n = c.execute("SELECT COUNT(*) AS n FROM bottles WHERE chem='cl'").fetchone()["n"]
            if not n:
                debit = kv_get("debit_at_baseline") or 15.0
                c.execute("INSERT INTO bottles(chem,fitted_at,baseline,factor,size_l) "
                          "VALUES('cl',?,?,?,?)",
                          (kv_get("bottle_fitted_at") or now_iso(), base,
                           float(debit) / 36000.0, float(kv_get("bottle_l", DEF_BOTTLE))))
    kv_set("bottles_migrated", 1)


# --- Bottle tracking, shared by chlorine ('cl') and pH-minus ('ph') -----------
CHEM = {
    "cl": {"name": "chlorine", "odo": "total_time",
           "bottle_l": "bottle_l", "warn": "warn_remaining_l", "final": "final_remaining_l",
           "nwarn": "notified_warn", "nfinal": "notified_final"},
    "ph": {"name": "pH-minus", "odo": "ph_total",
           "bottle_l": "ph_bottle_l", "warn": "ph_warn_remaining_l", "final": "ph_final_remaining_l",
           "nwarn": "ph_notified_warn", "nfinal": "ph_notified_final"},
}


def _chem_odo(reading, chem):
    """Current lifetime odometer (seconds) for the chemical from a reading dict."""
    return (reading or {}).get(CHEM[chem]["odo"])


def _chem_factor(chem):
    """Litres of product per odometer-second, using the live rate.
    chlorine: debit/36000 (debit ~15 -> 1.5 L/h). pH: pump L/h / 3600."""
    if chem == "cl":
        debit = (kv_get("last_reading") or {}).get("debit") or 15.0
        return float(debit) / 36000.0
    return float(kv_get("ph_pump_lph", 1.5)) / 3600.0


def current_bottle(chem):
    with db() as c:
        r = c.execute("SELECT * FROM bottles WHERE chem=? "
                      "ORDER BY fitted_at DESC, id DESC LIMIT 1", (chem,)).fetchone()
    return dict(r) if r else None


def bottle_used_l(chem, odo_now=None):
    """Litres of product used from the current bottle (None if no bottle).

    Counts the recorded odometer climbs since the bottle was fitted, so it stays
    correct through reboots that erase runtime from the controller's own counter.
    Passing `odo_now` forces a simple endpoint difference (used by the history
    view for already-replaced bottles)."""
    b = current_bottle(chem)
    if not b or b.get("baseline") is None:
        return None
    if odo_now is not None:
        factor = b.get("factor") or _chem_factor(chem)
        return max((odo_now - b["baseline"]) * factor, 0.0)
    if not b.get("fitted_at"):
        return None
    return _used_litres_since(chem, b["fitted_at"], b["baseline"])


def bottle_history(chem):
    """History rows newest-first: fitted/replaced time, size and litres used per bottle."""
    with db() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT * FROM bottles WHERE chem=? ORDER BY fitted_at ASC, id ASC",
            (chem,)).fetchall()]
    odo_now = _chem_odo(kv_get("last_reading"), chem)
    live = _chem_factor(chem)
    out = []
    for i, b in enumerate(rows):
        factor = b.get("factor") or live
        nxt = rows[i + 1] if i + 1 < len(rows) else None
        end_odo = nxt["baseline"] if nxt else odo_now
        litres = None
        if b.get("baseline") is not None and end_odo is not None:
            litres = round(max((end_odo - b["baseline"]) * factor, 0.0), 2)
        out.append({"id": b["id"], "fitted_at": b["fitted_at"],
                    "replaced_at": (nxt["fitted_at"] if nxt else None),
                    "size_l": b.get("size_l"), "litres_used": litres,
                    "current": nxt is None})
    out.reverse()
    return out


def _resync_cl_legacy():
    """Keep the old single-baseline kv in step with the current chlorine bottle
    so the existing dashboard/alert fields stay correct after edits/deletes."""
    b = current_bottle("cl")
    kv_set("baseline_total_time", b["baseline"] if b else None)
    kv_set("bottle_fitted_at", b["fitted_at"] if b else None)


def bottle_forecast(chem):
    """Remaining-focused view of a bottle + a consumption-based lifetime forecast.
    Average is taken over the last `bottle_avg_days` days (or as many as exist)."""
    from datetime import timedelta
    size = float(kv_get(CHEM[chem]["bottle_l"]))
    avg_days = int(kv_get("bottle_avg_days", 14) or 14)
    out = {"size": size, "remaining": None, "pct": None, "used_today": None,
           "avg_per_day": None, "days_left": None, "est_empty": None,
           "avg_days": avg_days, "fitted_at": None}
    b = current_bottle(chem)
    if b:
        out["fitted_at"] = b.get("fitted_at")
    used = bottle_used_l(chem)
    if used is None:
        return out
    remaining = max(size - used, 0.0)
    out["remaining"] = remaining
    out["pct"] = max(0.0, min(100.0, (remaining / size * 100.0))) if size > 0 else None
    tml = today_dose_ml(chem)
    out["used_today"] = (tml / 1000.0) if tml is not None else None
    # average daily consumption over the last `avg_days` days
    du = _daily_usage(chem, days=avg_days + 3)
    recent = [du[d] for d in sorted(du)[-avg_days:]]
    if recent:
        avg = sum(recent) / len(recent)
        out["avg_per_day"] = avg
        if avg > 0:
            days_left = remaining / avg
            out["days_left"] = days_left
            est = datetime.now(timezone.utc).astimezone() + timedelta(days=days_left)
            out["est_empty"] = est.date().isoformat()
    return out


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
            "SMTP_HOST", "SMTP_PORT", "SMTP_USER", "ALERT_TO", "ALERT_FROM", "SMTP_PASS",
            "LABCOM_TOKEN")


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

    # pH-minus dosing pump: lifetime run-time odometer (seconds), same shape as
    # the filtration odometer. Usage is derived from run-time * configured L/h.
    ph_total = out_total(pool, SCHED_PH)
    ph_total = float(ph_total) if ph_total is not None else None

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
    kv_set("last_outs", pool.get("outs") or [])   # for identifying the pH pump output
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
        "ph_total": ph_total,
        "treatment": out_status(pool, SCHED_TRAIT),
        "used_l": used,
        "suspended": pool.get("suspended"),
    }
    with db() as c:
        c.execute("INSERT INTO readings "
                  "(ts,total_time,debit,ph,orp,temp,salt,filtration,used_l,filt_total,ph_total) "
                  "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (reading["ts"], total, debit, reading["ph"], reading["orp"],
                   reading["temp"], reading["salt"], reading["filtration"], used,
                   reading["filt_total"], ph_total))
    kv_set("last_reading", reading)          # so odometer lookups see this reading

    # pH-minus usage (from the current pH bottle) + today's dosed mL
    reading["used_ph"] = bottle_used_l("ph")
    reading["ph_today_ml"] = today_dose_ml("ph")
    kv_set("last_reading", reading)
    kv_set("last_ok", now_iso())
    kv_set("last_error", None)

    # Two-tier "litres remaining" alerts for each chemical bottle.
    for chem in ("cl", "ph"):
        try:
            _check_bottle_alerts(chem, reading.get("nickname"))
        except Exception as e:
            print(f"[alert:{chem}] error:", e)
    return reading


def _daily_usage(chem, days=130):
    """Litres of product dosed per calendar day, robust to power events.

    Walks the odometer readings in order and sums only the POSITIVE step-to-step
    increments, each capped at the real elapsed time between readings (the pump
    can't have run longer than the wall clock). A controller reboot that rolls
    the counter back shows up as a negative step, which is ignored; the counter
    then climbs again and those genuine increments are still counted -- so
    post-reboot dosing is captured correctly. A spurious high spike is clipped by
    the elapsed-time cap. Returns {'YYYY-MM-DD': litres}."""
    from datetime import timedelta
    col = CHEM[chem]["odo"]
    cutoff = (datetime.now(timezone.utc).astimezone() - timedelta(days=days)).isoformat()
    with db() as c:
        rows = c.execute(f"SELECT ts,{col} AS odo,debit FROM readings "
                         f"WHERE {col} IS NOT NULL AND ts>=? ORDER BY ts", (cutoff,)).fetchall()
    per_day = {}
    for iso, litres in _usage_increments(rows, chem):
        per_day[iso[:10]] = per_day.get(iso[:10], 0.0) + litres
    return per_day


def _usage_increments(rows, chem):
    """The one robust engine behind every 'how much product' figure. Given
    odometer rows (each with ts, odo, debit) in time order, return (iso_ts,
    litres) for each genuine positive counter step. It is immune to:
      * reboots that roll the counter backwards      -> negative steps ignored,
      * a single garbage low poll that recovers      -> that poll is dropped,
      * a stale counter that jumps in bursts         -> each jump is capped by how
        long the value had been sitting (a 'plateau'), the longest the pump could
        have run, which lets real catch-up jumps through but bounds a spike."""
    ph_rate = float(kv_get("ph_pump_lph", 1.5))
    TOL = 30.0
    pts = []
    for r in rows:
        try:
            pts.append([datetime.fromisoformat(r["ts"]).timestamp(), r["odo"],
                        r["debit"], str(r["ts"])])
        except (ValueError, TypeError):
            continue
    clean = []
    for i, p in enumerate(pts):
        if 0 < i < len(pts) - 1:
            prev_o, next_o = pts[i - 1][1], pts[i + 1][1]
            if p[1] < prev_o - TOL and next_o >= prev_o - TOL:
                continue                              # isolated dip (one bad poll) -> drop
        clean.append(p)
    out, prev_t, prev_o, plateau_t = [], None, None, None
    for t, odo, debit, iso in clean:
        if prev_o is None:
            plateau_t = t
        else:
            step = odo - prev_o
            if step > 0:
                cap = max(t - (plateau_t if plateau_t is not None else prev_t), 0.0) * 1.2 + 5.0
                run = min(step, cap)
                litres = (run * float(debit or 15.0) / 36000.0) if chem == "cl" else (run * ph_rate / 3600.0)
                out.append((iso, litres))
                plateau_t = t                         # a new value starts a new plateau
            elif step < 0:
                plateau_t = t                         # reboot/dip resets the plateau
            # step == 0: still sitting on the same value -> keep plateau_t
        prev_t, prev_o = t, odo
    return out


def _used_litres_since(chem, since_iso, baseline=None):
    """Total litres dosed since `since_iso` (e.g. a bottle's fitted time), summed
    from the recorded odometer climbs -- so it still counts runtime that a later
    reboot erased from the controller's own counter. Anchored at `baseline` (the
    odometer value when the bottle was fitted) so nothing before our first stored
    reading is missed."""
    col = CHEM[chem]["odo"]
    with db() as c:
        after = c.execute(f"SELECT ts,{col} AS odo,debit FROM readings "
                          f"WHERE {col} IS NOT NULL AND ts>=? ORDER BY ts", (since_iso,)).fetchall()
    rows = list(after)
    if baseline is not None:
        rows = [{"ts": since_iso, "odo": baseline, "debit": None}] + rows
    return sum(l for _, l in _usage_increments(rows, chem))


def today_dose_ml(chem):
    """mL of product dosed so far today (robust to power events -- see _daily_usage)."""
    col = CHEM[chem]["odo"]
    with db() as c:
        any_row = c.execute(f"SELECT 1 FROM readings WHERE {col} IS NOT NULL LIMIT 1").fetchone()
    if not any_row:
        return None
    today = datetime.now(timezone.utc).astimezone().date().isoformat()
    return _daily_usage(chem, days=2).get(today, 0.0) * 1000.0


def _check_bottle_alerts(chem, pool_name):
    meta = CHEM[chem]
    used = bottle_used_l(chem)
    if used is None:
        return
    size = float(kv_get(meta["bottle_l"]))
    warn_rem  = float(kv_get(meta["warn"]))
    final_rem = float(kv_get(meta["final"]))
    remaining = size - used
    label = meta["name"]

    def _alert(subject, body, flag):
        try:
            if send_email(subject, body):
                kv_set(flag, 1); kv_set(flag + "_at", now_iso())
        except Exception as e:
            print("[email] error:", e)

    if remaining <= final_rem:
        if not kv_get(meta["nfinal"]):
            _alert(f"Klereo: CHANGE the {label} bottle now",
                   f"Your pool '{pool_name}' has ~{max(remaining,0):.1f} L of {label} left "
                   f"(used {used:.1f} L of a {size:.0f} L bottle).\n\n"
                   f"Change the bottle now, then press 'Register new bottle' on the "
                   f"dashboard.\n", meta["nfinal"])
            kv_set(meta["nwarn"], 1)     # suppress the redundant earlier warning
    elif remaining <= warn_rem:
        if not kv_get(meta["nwarn"]):
            _alert(f"Klereo: {label} getting low - check you have a spare",
                   f"Your pool '{pool_name}' has ~{remaining:.1f} L of {label} left "
                   f"(used {used:.1f} L of a {size:.0f} L bottle).\n\n"
                   f"Make sure you have a spare bottle ready - you'll get a second email "
                   f"when it's time to actually change it.\n", meta["nwarn"])


COMMAND_NAMES = {9: "done", 13: "not allowed for your account", 15: "pod timeout",
                 17: "pod offline", 10: "error", 11: "bad parameter", 12: "unknown command"}


def set_filtration(action):
    """Drive the filter pump. action = 'on' (manual ON), 'off' (manual OFF),
    'auto' (hand back to Regulated). Returns the final command status code."""
    login = cfg_get("KLEREO_LOGIN"); password = cfg_get("KLEREO_PASSWORD")
    if not login or not password:
        raise KlereoError("Klereo credentials not set")
    k = Klereo(); k.login(login, password)
    pid = cfg_get("KLEREO_POOL_ID")
    if not pid:
        ids = k.pool_ids()
        if len(ids) != 1:
            raise KlereoError(f"Set KLEREO_POOL_ID (pools: {ids})")
        pid = ids[0]
    cur_status = out_status(k.pool(pid), SCHED_FILTRE) or 0
    if action == "on":
        nm, ns = 0, 1                      # MODE_MANU, on
    elif action == "off":
        nm, ns = 0, 0                      # MODE_MANU, off
    elif action == "auto":
        nm, ns = 3, (cur_status or 1)      # MODE_REGUL
    else:
        raise KlereoError("unknown action")
    b = k._post("php/SetOut.php", {"poolID": pid, "outIdx": SCHED_FILTRE,
                                   "newMode": nm, "newState": ns, "comMode": 1})
    r = b.get("response")
    cmd = r[0].get("cmdID") if isinstance(r, list) and r and isinstance(r[0], dict) else None
    if cmd is None:
        return None
    for _ in range(8):
        time.sleep(2)
        st = (k._post("php/WaitCommand.php", {"cmdID": cmd}).get("response") or {}).get("status")
        if st in (9, 10, 11, 12, 13, 15, 16, 17, 18, 19):
            return st
    return None


# --------------------------------------------------------------------------
# PoolLab / LabCom cloud (manual water tests)  -- read-only GraphQL
# --------------------------------------------------------------------------
LABCOM_URL = "https://backend.labcom.cloud/graphql"
LABCOM_QUERY = (
    "query { CloudAccount { id email last_change_time "
    "Accounts { id forename surname pooltext volume volume_unit "
    "Measurements { id scenario parameter parameter_id unit value formatted_value "
    "ideal_low ideal_high ideal_status timestamp operator_name comment device_serial "
    "} } } }")

# Map LabCom parameter names to a short key + display label + decimals.
# Anything not listed still gets stored/shown, just under its raw name.
LAB_PARAMS = {
    "PL pH":             {"key": "ph",   "label": "pH",           "dec": 2},
    "PL Chlorine Free":  {"key": "fc",   "label": "Free chlorine","dec": 2},
    "PL Chlorine Total": {"key": "tc",   "label": "Total chlorine","dec": 2},
    "PL Chlorine Combined": {"key": "cc","label": "Combined chlorine", "dec": 2},
    "PL Cyanuric Acid":  {"key": "cya",  "label": "Cyanuric acid (CYA)", "dec": 0},
    "PL T-Alka":         {"key": "alk",  "label": "Total alkalinity",    "dec": 0},
    "PL Calcium Hardness": {"key": "ch", "label": "Calcium hardness","dec": 0},
    "PL Salt":           {"key": "salt", "label": "Salt",         "dec": 0},
    "PL Bromine":        {"key": "br",   "label": "Bromine",      "dec": 2},
    "PL Phosphate":      {"key": "po4",  "label": "Phosphate",    "dec": 2},
}

# Sensible target bands used when LabCom has no ideal range set (ideal_* = -1).
# Standard outdoor liquid-chlorine pool targets; can be made configurable later.
LAB_DEFAULT_RANGES = {
    "ph":  (7.0, 7.6),
    "fc":  (1.0, 3.0),
    "tc":  (1.0, 3.0),
    "cc":  (0.0, 0.2),      # combined chlorine (chloramines): keep low
    "cya": (30.0, 50.0),
    "alk": (80.0, 120.0),
    "ch":  (200.0, 400.0),
}


LAB_TARGET_FIELDS = [("ph", "pH"), ("fc", "Free chlorine"), ("tc", "Total chlorine"),
                     ("cc", "Combined chlorine"), ("cya", "Cyanuric acid (CYA)"),
                     ("alk", "Total alkalinity"), ("ch", "Calcium hardness")]


def _lab_range(key, api_low, api_high):
    """Effective (low, high) target band, in priority order:
    1) a range the user set in our Settings (kv 'lab_range_<key>'),
    2) the LabCom ideal if it's actually configured (not -1),
    3) a sensible built-in default."""
    u = kv_get("lab_range_" + key)
    if (isinstance(u, (list, tuple)) and len(u) == 2
            and u[0] is not None and u[1] is not None and u[0] < u[1]):
        return float(u[0]), float(u[1])

    def _ok(x):
        return x is not None and x > -0.5
    if _ok(api_low) and _ok(api_high) and api_low < api_high:
        return api_low, api_high
    return LAB_DEFAULT_RANGES.get(key, (None, None))


def _latest_lab_value(param_key):
    with db() as c:
        r = c.execute("SELECT value FROM lab_tests WHERE param_key=? AND value IS NOT NULL "
                      "ORDER BY ts DESC LIMIT 1", (param_key,)).fetchone()
    return r["value"] if r else None


# Recommended re-test cadence per parameter (days). Editable in Settings.
LAB_CADENCE_DEFAULTS = {"ph": 7, "fc": 7, "tc": 7, "cc": 14,
                        "cya": 30, "alk": 30, "ch": 30}


def _lab_cadence(key):
    v = kv_get("lab_cadence_" + key)
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    return LAB_CADENCE_DEFAULTS.get(key)


# Editable target bands for the live Klereo tiles.
LIVE_RANGE_DEFAULTS = {"ph": (7.0, 7.6), "orp": (600.0, 800.0), "temp": (26.0, 30.0)}


def _live_range(key, seuil_min=None, seuil_max=None):
    """Effective (low, high) for a live tile: user override > Klereo seuils > default."""
    u = kv_get("live_range_" + key)
    if (isinstance(u, (list, tuple)) and len(u) == 2
            and u[0] is not None and u[1] is not None and u[0] < u[1]):
        return float(u[0]), float(u[1])
    if seuil_min is not None and seuil_max is not None and seuil_min < seuil_max:
        return float(seuil_min), float(seuil_max)
    return LIVE_RANGE_DEFAULTS.get(key, (None, None))


# Keyword fallbacks so naming variants across PoolLab firmware/locales still map
# to a canonical key (and therefore pick up a target band). First match wins;
# order matters (the specific chlorine phrases must precede the generic "chlor").
_PARAM_ALIASES = [
    ("cyanuric",       ("cya",  "Cyanuric acid (CYA)", 0)),
    ("alkalin",        ("alk",  "Total alkalinity",    0)),
    ("t-alka",         ("alk",  "Total alkalinity",    0)),
    ("calcium",        ("ch",   "Calcium hardness",    0)),
    ("hardness",       ("ch",   "Calcium hardness",    0)),
    ("phosph",         ("po4",  "Phosphate",           2)),
    ("bromin",         ("br",   "Bromine",             2)),
    ("combined chlor", ("cc",   "Combined chlorine",   2)),
    ("total chlor",    ("tc",   "Total chlorine",      2)),
    ("free chlor",     ("fc",   "Free chlorine",       2)),
    ("salt",           ("salt", "Salt",                0)),
]


def _param_meta(parameter):
    m = LAB_PARAMS.get(parameter)
    if m:
        return m["key"], m["label"], m["dec"]
    low = (parameter or "").lower()
    for kw, meta in _PARAM_ALIASES:
        if kw in low:
            return meta
    if re.search(r"\bph\b", low):            # whole word, so 'phosphate' isn't caught
        return ("ph", "pH", 2)
    if "chlor" in low:                       # unqualified chlorine -> free chlorine
        return ("fc", "Free chlorine", 2)
    # normalize an unknown parameter: strip "PL ", slug the key
    label = re.sub(r"^PL\s+", "", parameter or "").strip() or (parameter or "?")
    key = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_") or "param"
    return key, label, 2


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _ts_iso(v):
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v).astimezone().isoformat(timespec="seconds")
    return str(v) if v else None


def labcom_fetch(token):
    """POST the GraphQL query and return the CloudAccount dict (raises on error)."""
    r = requests.post(LABCOM_URL, json={"query": LABCOM_QUERY},
                      headers={"Authorization": token,
                               "Content-Type": "application/json"},
                      timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    b = r.json()
    if b.get("errors"):
        raise KlereoError("LabCom: " + json.dumps(b["errors"])[:200])
    data = (b.get("data") or {}).get("CloudAccount")
    if data is None:
        raise KlereoError("LabCom: no CloudAccount in response")
    return data


# PoolLab reports "OR" (outside range) as a huge sentinel value (1e6). No real
# pool parameter ever reaches this, so anything at/above it is discarded.
LAB_SENTINEL = 100000.0


def labcom_store(cloud):
    """Sync measurements to mirror the cloud: insert new ones (dedup on id),
    drop out-of-range sentinel readings, and remove rows the user deleted in the
    cloud. Returns count added."""
    added = removed = 0
    cloud_ids, acct_ids = [], []
    with db() as c:
        for acct in (cloud.get("Accounts") or []):
            aid = acct.get("id")
            acct_ids.append(str(aid))
            for m in (acct.get("Measurements") or []):
                mid = m.get("id")
                if mid is None:
                    continue
                cloud_ids.append(str(mid))
                val = _to_float(m.get("value"))
                if val is not None and abs(val) >= LAB_SENTINEL:
                    continue                          # "OR" / outside-range -> don't store
                key, _label, _dec = _param_meta(m.get("parameter"))
                cur = c.execute("INSERT OR IGNORE INTO lab_tests "
                                "(meas_id,ts,account_id,parameter,param_key,value,unit,"
                                " ideal_low,ideal_high,operator,comment,device_serial) "
                                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                                (str(mid), _ts_iso(m.get("timestamp")), str(aid),
                                 m.get("parameter"), key, val,
                                 m.get("unit"), _to_float(m.get("ideal_low")),
                                 _to_float(m.get("ideal_high")), m.get("operator_name"),
                                 m.get("comment"), m.get("device_serial")))
                added += cur.rowcount
        # Mirror deletions: remove local rows for the returned accounts whose id is
        # no longer in the cloud -- but only when we actually got measurements back,
        # so a bad/empty response can never wipe local history.
        if cloud_ids and acct_ids:
            qa = ",".join("?" * len(acct_ids))
            qi = ",".join("?" * len(cloud_ids))
            cur = c.execute(
                f"DELETE FROM lab_tests WHERE account_id IN ({qa}) "
                f"AND meas_id IS NOT NULL AND meas_id NOT IN ({qi})",
                acct_ids + cloud_ids)
            removed += cur.rowcount
        # Belt-and-braces: clear any sentinel rows stored by earlier versions.
        cur = c.execute("DELETE FROM lab_tests WHERE value IS NOT NULL AND ABS(value) >= ?",
                        (LAB_SENTINEL,))
        removed += cur.rowcount
    if removed:
        print(f"[labcom] removed {removed} stale/out-of-range lab row(s)")
    return added


def labcom_poll_once(force=False):
    """Fetch LabCom if a token is set and it's due (or forced). Stores new tests."""
    token = _clean(cfg_get("LABCOM_TOKEN"))
    if not token:
        return None
    cloud = labcom_fetch(token)
    lct = cloud.get("last_change_time")
    added = labcom_store(cloud)
    # keep a trimmed copy of the newest tests for the /api/lab-raw diagnostics view
    try:
        sample = []
        for acct in (cloud.get("Accounts") or [])[:2]:
            ms = sorted((acct.get("Measurements") or []),
                        key=lambda m: m.get("timestamp") or 0, reverse=True)[:12]
            sample.append({"account_id": acct.get("id"),
                           "pooltext": acct.get("pooltext"), "Measurements": ms})
        kv_set("lab_last_raw", {"last_change_time": lct, "Accounts": sample})
    except Exception:
        pass
    kv_set("lab_last_ok", now_iso())
    kv_set("lab_last_error", None)
    kv_set("lab_last_change", lct)
    if cloud.get("Accounts"):
        a0 = cloud["Accounts"][0]
        kv_set("lab_pool_name", a0.get("pooltext") or
               (f"{a0.get('forename','')} {a0.get('surname','')}").strip())
    return added


def _labcom_due():
    """True if enough time has passed since the last successful LabCom fetch."""
    last = kv_get("lab_last_ok")
    if not last:
        return True
    try:
        hrs = float(kv_get("labcom_poll_hours", 1.0))
        age = (datetime.now(timezone.utc).astimezone()
               - datetime.fromisoformat(last)).total_seconds()
        return age >= hrs * 3600
    except Exception:
        return True


WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

def _weather_due():
    last = kv_get("weather_ts")
    if not last:
        return True
    try:
        age = (datetime.now(timezone.utc).astimezone()
               - datetime.fromisoformat(last)).total_seconds()
    except Exception:
        return True
    return age >= 1800.0          # refresh at most every 30 minutes


def weather_poll_once(force=False):
    """Fetch current conditions from Open-Meteo (free, no key) and cache them."""
    if not kv_get("weather_enabled", 1):
        return None
    if not force and not _weather_due():
        return None
    lat, lon = kv_get("weather_lat"), kv_get("weather_lon")
    if lat is None or lon is None:
        return None
    resp = requests.get(WEATHER_URL, params={
        "latitude": lat, "longitude": lon,
        "current": "temperature_2m,weather_code,is_day",
        "daily": "temperature_2m_max,temperature_2m_min",
        "forecast_days": 1, "timezone": "auto"}, timeout=10)
    resp.raise_for_status()
    body = resp.json() or {}
    cur = body.get("current") or {}
    daily = body.get("daily") or {}

    def _first(seq):
        return seq[0] if isinstance(seq, list) and seq else None
    w = {"temp": cur.get("temperature_2m"),
         "code": cur.get("weather_code"),
         "is_day": cur.get("is_day"),
         "tmax": _first(daily.get("temperature_2m_max")),
         "tmin": _first(daily.get("temperature_2m_min")),
         "ts": now_iso()}
    kv_set("weather", w)
    kv_set("weather_ts", now_iso())
    return w


def poller_loop():
    try:                                  # refresh weather immediately on start/deploy
        weather_poll_once(force=True)
    except Exception as e:
        print("[weather] startup fetch error:", e)
    while True:
        try:
            with _lock:
                r = poll_once()
            print(f"[{r['ts']}] polled: pH={r['ph']} ORP={r['orp']} temp={r['temp']} "
                  f"used={r['used_l']}")
        except Exception as e:
            print("[poll] error:", e)
            kv_set("last_error", f"{now_iso()}: {e}")
        # LabCom lab tests change rarely; fetch on its own slower cadence.
        try:
            if cfg_get("LABCOM_TOKEN") and _labcom_due():
                n = labcom_poll_once()
                if n:
                    print(f"[labcom] {n} new lab test(s) stored")
        except Exception as e:
            print("[labcom] error:", e)
            kv_set("lab_last_error", f"{now_iso()}: {e}")
        try:
            weather_poll_once()
        except Exception as e:
            print("[weather] error:", e)
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

PAGE = b"""<!doctype html><html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="icon" href="/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="default">
<meta name="apple-mobile-web-app-title" content="Pool Stats">
<meta name="theme-color" content="#eef2f7" media="(prefers-color-scheme: light)">
<meta name="theme-color" content="#0b1220" media="(prefers-color-scheme: dark)">
<title>Pool Stats</title>
<script>(function(){try{var m=localStorage.getItem('themeMode')||'auto';var sys=matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light';document.documentElement.setAttribute('data-theme',m==='auto'?sys:m);}catch(e){}})();</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4"></script>
<style>
:root{--bg:#eef2f7;--card:#fff;--card2:#f5f8fc;--text:#0f172a;--muted:#64748b;--line:#e6ebf2;
 --shadow:0 6px 20px rgba(15,23,42,.06);--primary:#2563eb;--primarybg:#eaf1ff;
 --ok:#16a34a;--okbg:#e7f6ec;--warn:#d97706;--warnbg:#fdf3e3;--bad:#dc2626;--badbg:#fbe9e9;
 --ph:#3b82f6;--cl:#22c55e;--orp:#f59e0b;--temp:#8b5cf6}
[data-theme="dark"]{--bg:#0b1220;--card:#131c2e;--card2:#0f1830;--text:#e8eef7;--muted:#93a1b8;--line:#233149;
 --shadow:0 8px 24px rgba(0,0,0,.4);--primary:#3b82f6;--primarybg:#16233f;
 --ok:#4ade80;--okbg:#12331f;--warn:#fbbf24;--warnbg:#33280f;--bad:#f87171;--badbg:#3a1717;
 --ph:#60a5fa;--cl:#4ade80;--orp:#fbbf24;--temp:#a78bfa}
*{box-sizing:border-box}
html,body{overscroll-behavior-y:contain}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--text);transition:background .3s,color .3s}
.app{max-width:480px;margin:0 auto;min-height:100vh;padding:0 0 96px;position:relative}
.top{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;padding:calc(env(safe-area-inset-top,0px) + 16px) 16px 4px}
.top h1{font-size:21px;margin:0;letter-spacing:-.3px}
.top .titlewrap{min-width:0}
.top .sub{color:var(--muted);font-size:12.5px;margin-top:2px}
/* first-paint loading overlay */
#loader{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:var(--bg);z-index:200;transition:opacity .35s}
#loader.hide{opacity:0;pointer-events:none}
.spinner{width:40px;height:40px;border:3px solid var(--line);border-top-color:var(--primary);border-radius:50%;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
/* status strip (stale data / filtration off) */
.statusbar{display:flex;flex-wrap:wrap;gap:8px;padding:6px 15px 0}
.statusbar:empty{display:none}
.stbadge{display:inline-flex;align-items:center;gap:6px;font-size:12px;font-weight:700;padding:5px 11px;border-radius:999px}
.stbadge.warn{background:var(--warnbg);color:var(--warn)} .stbadge.bad{background:var(--badbg);color:var(--bad)}
.stbadge .d{width:7px;height:7px;border-radius:50%;background:currentColor}
.mtf{display:flex;flex-direction:column;gap:3px;font-size:12px;color:var(--muted)}
.mtf input{width:100%}
.iconbtn{width:38px;height:38px;border-radius:12px;border:0;background:var(--card);box-shadow:var(--shadow);color:var(--text);display:flex;align-items:center;justify-content:center;cursor:pointer}
.weather{display:flex;align-items:center;gap:5px;font-size:15px;font-weight:700;color:var(--text)}
.weather svg{color:var(--orp)}
.weather .whl{font-weight:500;font-size:11px;color:var(--muted);margin-left:1px}
.weather:empty{display:none}
.wrap{padding:6px 15px}
.sect{font-size:12px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:var(--muted);margin:16px 4px 9px}
.card{background:var(--card);border-radius:18px;box-shadow:var(--shadow);padding:16px;margin-bottom:12px}
.cardhead{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.cardhead .t{display:flex;align-items:center;gap:8px;font-weight:700;font-size:15px}
.cardhead .t svg{color:var(--primary)}
.mut{color:var(--muted)}
.wq{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}
.chip{background:var(--card2);border-radius:14px;padding:12px 12px 10px;text-align:left;cursor:pointer;border:2px solid transparent;transition:border-color .15s}
.chip.sel{border-color:var(--primary)}
.qlabel{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.03em;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.qval{font-size:25px;font-weight:800;letter-spacing:-1px;margin-top:4px;line-height:1}
.qval small{font-size:12px;color:var(--muted);font-weight:600;margin-left:2px}
.qsub{font-size:11.5px;margin-top:4px}
.qsub .w{font-weight:700}
.qage{font-size:10px;color:var(--muted);margin-top:3px}
.chip .dot{width:38px;height:38px;border-radius:50%;margin:0 auto 7px;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:700;font-size:12px}
.chip .v{font-size:20px;font-weight:800;letter-spacing:-.5px}
.chip .s{font-size:11px;font-weight:700;margin-top:1px}
.chip .age{color:var(--muted);font-size:9.5px;margin-top:5px;padding-top:5px;border-top:1px solid var(--line)}
.s.ok{color:var(--ok)} .s.warn{color:var(--warn)} .s.bad{color:var(--bad)}
.detail{display:flex;align-items:center;gap:14px}
.detail .big{font-size:28px;font-weight:800;letter-spacing:-1px}
.detail .big small{font-size:14px;color:var(--muted);font-weight:600}
.detail .lab{font-size:13px;font-weight:700}
.spark{flex:1;min-width:0;height:60px}
.bottles{display:grid;grid-template-columns:1fr;gap:12px}
.bottle{display:flex;gap:12px;align-items:center;padding-left:6px}
.bottle .info{flex:1;min-width:0}
.bhead{display:flex;align-items:center;gap:7px;margin-bottom:2px}
.bname{font-weight:800;font-size:14px}
.pct{font-weight:800;font-size:13px;padding:3px 10px;border-radius:999px;margin-left:auto}
.pct.ok{background:var(--okbg);color:var(--ok)} .pct.warn{background:var(--warnbg);color:var(--warn)} .pct.bad{background:var(--badbg);color:var(--bad)}
.rem{font-size:27px;font-weight:800;letter-spacing:-1px;margin-top:3px}
.rem small{font-size:13px;color:var(--muted);font-weight:600}
.row2{font-size:14px;font-weight:700;margin-top:5px}
.divider{height:1px;background:var(--line);margin:11px 0}
.statcols{display:flex;gap:16px}
.stat{display:flex;align-items:center;gap:8px}
.stat svg{color:var(--muted);flex:0 0 auto}
.stat b{font-size:14px;display:block;line-height:1.15}
.stat span{color:var(--muted);font-size:11px}
.btns{display:flex;gap:8px;margin-top:12px}
.btn{flex:1;border:0;border-radius:11px;padding:11px 6px;font-size:13.5px;font-weight:700;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;white-space:nowrap}
.btn.p{background:var(--primary);color:#fff}
.btn.s{background:var(--card2);color:var(--text);border:1px solid var(--line)}
.chartbox{height:150px;display:flex;align-items:flex-end;gap:5px}
.seg{display:inline-flex;background:var(--card2);border:1px solid var(--line);border-radius:9px;overflow:hidden}
.seg button{background:transparent;color:var(--muted);border:0;padding:6px 11px;font-size:12.5px;cursor:pointer;font-weight:600}
.seg button.active{background:var(--primary);color:#fff}
.grow{display:flex;justify-content:space-between;align-items:center;padding:11px 2px;border-top:1px solid var(--line);gap:10px}
.grow:first-child{border-top:0}
.grow .k{color:var(--muted);font-size:13px}
.grow .v{font-weight:700}
.pill2{font-size:10px;font-weight:700;padding:2px 8px;border-radius:999px;background:var(--badbg);color:var(--bad);text-transform:uppercase}
.balrow{border-top:1px solid var(--line);padding:11px 2px;cursor:pointer}
.balrow:first-child{border-top:0}
.ln1{display:flex;justify-content:space-between;gap:12px;align-items:baseline}
.ln1 .k{color:var(--text);font-size:14px}
.ln1 .v{font-weight:700;font-size:15px;text-align:right;white-space:nowrap}
.ln2{display:flex;justify-content:space-between;gap:12px;align-items:baseline;margin-top:2px;font-size:12px}
.ln2 .s{font-weight:700}
.darow{display:flex;gap:9px;align-items:flex-start;padding:9px 2px;border-top:1px solid var(--line)}
.darow:first-child{border-top:0}
.dadot{width:8px;height:8px;border-radius:50%;margin-top:5px;flex:0 0 auto}
.dadot.bad{background:var(--bad)} .dadot.warn{background:var(--warn)} .dadot.info{background:var(--primary)}
.dat{font-size:13px;font-weight:600}
.dad{font-size:11.5px;color:var(--muted);margin-top:1px}
.alert{display:flex;gap:12px;align-items:flex-start;padding:14px;border-radius:15px;margin-bottom:10px;background:var(--card);box-shadow:var(--shadow)}
.alert .ai{width:36px;height:36px;border-radius:11px;display:flex;align-items:center;justify-content:center;flex:0 0 auto}
.ai.bad{background:var(--badbg);color:var(--bad)} .ai.warn{background:var(--warnbg);color:var(--warn)} .ai.info{background:var(--primarybg);color:var(--primary)}
.alert .at{font-weight:700;font-size:14px}
.alert .ad{color:var(--muted);font-size:12.5px;margin-top:2px}
.tabbar{position:fixed;left:0;right:0;bottom:0;background:var(--card);border-top:1px solid var(--line);display:flex;max-width:480px;margin:0 auto;padding:8px 6px calc(8px + env(safe-area-inset-bottom));box-shadow:0 -4px 20px rgba(15,23,42,.06)}
.tab{flex:1;background:none;border:0;color:var(--muted);display:flex;flex-direction:column;align-items:center;gap:3px;font-size:11px;font-weight:600;cursor:pointer;padding:6px 0;position:relative}
.tab.active{color:var(--primary)}
.tab .badge{position:absolute;top:-1px;right:calc(50% - 22px);background:var(--bad);color:#fff;font-size:10px;font-weight:800;min-width:16px;height:16px;border-radius:8px;display:flex;align-items:center;justify-content:center;padding:0 4px}
.page{display:none} .page.active{display:block}
input{background:var(--card2);border:1px solid var(--line);color:var(--text);border-radius:8px;padding:8px;font-size:14px}
.toast{position:fixed;left:50%;bottom:104px;transform:translateX(-50%) translateY(80px);background:var(--card);border:1px solid var(--line);color:var(--text);padding:11px 17px;border-radius:11px;font-size:14px;box-shadow:var(--shadow);opacity:0;transition:all .25s;z-index:60;max-width:90%;text-align:center}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.toast.ok{border-color:var(--ok)} .toast.bad{border-color:var(--bad)}
.modal{position:fixed;inset:0;background:rgba(15,23,42,.55);display:none;align-items:center;justify-content:center;z-index:70;padding:18px}
.modal.show{display:flex}
.modalcard{background:var(--card);border-radius:16px;padding:18px;width:360px;max-width:100%}
.err{background:var(--badbg);color:var(--bad);padding:9px 12px;border-radius:10px;font-size:13px;margin:8px 0}
a{color:var(--primary)}
.liquid{transition:transform 1.2s cubic-bezier(.25,.9,.3,1)}
/* ---- responsive: widen + multi-column on tablet/desktop (mobile unchanged) ---- */
@media (min-width:800px){
 .app{max-width:1080px;padding-bottom:104px}
 .top{padding:22px 22px 6px}
 .wrap{display:grid;grid-template-columns:1fr 1fr;gap:14px;align-items:start;padding:8px 20px}
 .wrap>.card{margin-bottom:0}
 .wrap>.sect{grid-column:1/-1;margin:12px 4px 0}
 .wrap>.span,.wrap>.bottles,.wrap>#alertsBody,.wrap>#wqCard,.wrap>.mut{grid-column:1/-1}
 .wrap>.bottles{grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
 /* rows share height for a uniform grid */
 #detailCard,#balanceCard,#filtCard,#dashAlerts{align-self:stretch}
 #balanceCard{display:flex;flex-direction:column;margin-bottom:0}
 #balanceBody{flex:1;display:flex;flex-direction:column;justify-content:center}
 #errbox{max-width:1080px;margin:0 auto}
 .tabbar{max-width:660px;border-radius:16px 16px 0 0}
 .wq{grid-template-columns:repeat(4,1fr)}
}
</style></head><body>
<div id="loader"><div class="spinner"></div></div>
<div id="toast" class="toast"></div>
<div id="ptr" style="position:fixed;top:0;left:0;right:0;text-align:center;padding:8px;color:var(--muted);font-size:13px;transform:translateY(-40px);transition:transform .15s;z-index:6">&#8595; pull to refresh</div>

<div id="bottleModal" class="modal"><div class="modalcard">
  <div style="font-size:16px;font-weight:700;margin-bottom:12px" id="bmTitle">Register new bottle</div>
  <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer"><input type="checkbox" id="bmNow" checked onchange="bmToggle()"> Fitted now</label>
  <div id="bmWhen" style="margin-top:12px;display:none">
    <div class="mut" style="font-size:13px;margin-bottom:5px">Date &amp; time fitted (past only):</div>
    <input type="datetime-local" id="bmTime" style="width:100%">
  </div>
  <div style="display:flex;gap:10px;margin-top:16px">
    <button type="button" class="btn s" onclick="closeBottle()">Cancel</button>
    <button type="button" class="btn p" onclick="confirmBottle()">Confirm</button>
  </div>
</div></div>
<div id="histModal" class="modal"><div class="modalcard" style="width:420px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <div style="font-size:16px;font-weight:700" id="histTitle">Bottle history</div>
    <button type="button" class="btn s" style="flex:0;padding:5px 11px" onclick="closeHistory()">Close</button>
  </div>
  <div id="histBody" style="max-height:60vh;overflow:auto">loading...</div>
</div></div>
<div id="labGridModal" class="modal"><div class="modalcard" style="width:420px">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
    <div style="font-size:16px;font-weight:700" id="labGridTitle">Readings</div>
    <button type="button" class="btn s" style="flex:0;padding:5px 11px" onclick="closeLabGrid()">Close</button>
  </div>
  <div id="labGridBody" style="max-height:60vh;overflow:auto">loading...</div>
</div></div>
<div id="manualModal" class="modal"><div class="modalcard" style="width:400px">
  <div style="font-size:16px;font-weight:700;margin-bottom:10px">Add manual test</div>
  <div class="mut" style="font-size:12px;margin-bottom:4px">Date &amp; time (past only)</div>
  <input type="datetime-local" id="mt_time" style="width:100%;margin-bottom:12px">
  <div class="mut" style="font-size:12px;margin-bottom:8px">Enter only the values you measured &mdash; leave the rest blank.</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:9px">
    <label class="mtf">pH<input id="mt_ph" type="number" step="0.01" inputmode="decimal"></label>
    <label class="mtf">Free chlorine (mg/l)<input id="mt_fc" type="number" step="0.01" inputmode="decimal"></label>
    <label class="mtf">Total chlorine (mg/l)<input id="mt_tc" type="number" step="0.01" inputmode="decimal"></label>
    <label class="mtf">Combined chlorine (mg/l)<input id="mt_cc" type="number" step="0.01" inputmode="decimal"></label>
    <label class="mtf">Cyanuric acid / CYA<input id="mt_cya" type="number" step="1" inputmode="decimal"></label>
    <label class="mtf">Total alkalinity<input id="mt_alk" type="number" step="1" inputmode="decimal"></label>
    <label class="mtf">Calcium hardness<input id="mt_ch" type="number" step="1" inputmode="decimal"></label>
    <label class="mtf">Salt (mg/l)<input id="mt_salt" type="number" step="1" inputmode="decimal"></label>
  </div>
  <div style="display:flex;gap:10px;margin-top:16px">
    <button type="button" class="btn s" onclick="closeManual()">Cancel</button>
    <button type="button" class="btn p" onclick="saveManual()">Save test</button>
  </div>
</div></div>

<div class="app">
 <div class="top">
   <div class="titlewrap"><h1>Pool Stats <span id="nick" style="font-size:14px;color:var(--muted);font-weight:400"></span></h1>
     <div class="sub" id="sub">loading...</div></div>
   <div style="display:flex;align-items:center;gap:12px;flex:0 0 auto">
     <div class="weather" id="weather"></div>
     <button class="iconbtn" id="themeBtn" onclick="cycleTheme()" title="Theme"></button></div>
 </div>
 <div class="statusbar" id="statusbar"></div>
 <div id="errbox"></div>

 <!-- DASHBOARD -->
 <div class="page active" id="p-dash"><div class="wrap">
   <div class="card" id="wqCard">
     <div class="cardhead"><div class="t">
       <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11z"/></svg>
       Water quality</div><span class="mut" style="font-size:12px">tap a metric</span></div>
     <div class="wq" id="wq"></div>
   </div>
   <div class="card" id="detailCard" style="display:none">
     <div class="cardhead"><div class="t"><span id="dIcon"></span><span id="dTitle"></span></div><span class="mut" id="dAge"></span></div>
     <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:8px">
       <div class="big"><span id="dVal">-</span><small id="dUnit"></small></div>
       <div class="lab" id="dStatus"></div>
       <div class="mut" id="dRange" style="font-size:12px;margin-left:auto"></div>
     </div>
     <div style="position:relative;height:180px"><canvas id="dChart"></canvas></div>
   </div>

   <div class="card" id="balanceCard" style="display:none">
     <div class="cardhead"><div class="t">Balance <span class="mut" style="font-weight:400;font-size:12px;text-transform:none;letter-spacing:0">(lab tests)</span></div></div>
     <div id="balanceBody"></div>
   </div>

   <div class="sect">Bottles</div>
   <div class="bottles" id="bottles"></div>

   <div class="sect">Filtration &amp; alerts</div>
   <div class="card" id="filtCard">
     <div class="cardhead"><div class="t"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v4M12 18v4M4.9 4.9l2.8 2.8M16.3 16.3l2.8 2.8M2 12h4M18 12h4M4.9 19.1l2.8-2.8M16.3 7.7l2.8-2.8"/></svg>Filtration</div><span class="mut" id="filtState"></span></div>
     <div class="mut" style="font-size:12px;margin-bottom:8px">Daily runtime (last 10 days)</div>
     <div style="position:relative;height:150px"><canvas id="chartFilt"></canvas></div>
     <div style="display:flex;gap:14px;justify-content:flex-end;font-size:11px;color:var(--muted);margin:8px 2px 0">
       <span><span style="display:inline-block;width:9px;height:9px;border-radius:2px;background:var(--primary);margin-right:4px"></span>off-peak</span>
       <span><span style="display:inline-block;width:9px;height:9px;border-radius:2px;background:var(--orp);margin-right:4px"></span>peak</span></div>
     <div class="grow" id="filtSummary"><span class="k">Last 7 days</span><span class="v">-</span></div>
   </div>
   <div class="card" id="dashAlerts">
     <div class="cardhead"><div class="t"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9z"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/></svg>Alerts</div><a onclick="go('alerts')" style="cursor:pointer;font-size:12px">View all &rarr;</a></div>
     <div id="dashAlertsBody"></div>
   </div>
 </div></div>

 <!-- HISTORY -->
 <div class="page" id="p-hist"><div class="wrap">
   <div class="card">
     <div class="cardhead"><div class="t">pH &amp; Redox</div>
       <div class="seg"><button id="hr1" class="active" onclick="setRange(1)">24h</button><button id="hr7" onclick="setRange(7)">7d</button><button id="hr30" onclick="setRange(30)">30d</button></div></div>
     <div style="position:relative;height:190px"><canvas id="chartPhOrp"></canvas></div>
   </div>
   <div class="card">
     <div class="cardhead"><div class="t">Product used</div>
       <div style="display:flex;gap:6px;flex-wrap:wrap"><div class="seg"><button id="uc_cl" class="active" onclick="setUsageChem('cl')">Cl</button><button id="uc_ph" onclick="setUsageChem('ph')">pH</button></div>
       <div class="seg"><button id="up_day" class="active" onclick="setUsage('day')">Day</button><button id="up_week" onclick="setUsage('week')">Week</button><button id="up_month" onclick="setUsage('month')">Month</button></div></div></div>
     <canvas id="chartUsage" height="120"></canvas>
     <div class="mut" id="usageSummary" style="font-size:12.5px;margin-top:8px"></div>
   </div>
   <div class="card" id="corrCard">
     <div class="cardhead"><div class="t">Correlation</div>
       <div style="display:flex;gap:6px;flex-wrap:wrap"><div class="seg" id="corrParamSeg"></div>
       <div class="seg"><button id="cp_orp" class="active" onclick="setCorrProbe('orp')">Redox</button><button id="cp_ph" onclick="setCorrProbe('ph')">pH</button><button id="cp_temp" onclick="setCorrProbe('temp')">Temp</button></div></div></div>
     <div style="position:relative;height:200px"><canvas id="chartCorr"></canvas></div>
     <div class="mut" id="corrSummary" style="font-size:12.5px;margin-top:8px"></div>
   </div>
   <div class="card" id="labListCard">
     <div class="cardhead"><div class="t">Lab tests</div><button class="btn s" style="flex:0;padding:6px 11px;font-size:12.5px" onclick="openManual()">+ Add manual test</button></div>
     <div class="mut" id="labWhen" style="font-size:12px;margin:-4px 0 8px"></div>
     <div id="labList"></div>
     <div class="mut" style="font-size:12px;margin-top:8px">Tap a test to see every reading &rarr;</div>
   </div>
 </div></div>

 <!-- ALERTS -->
 <div class="page" id="p-alerts"><div class="wrap"><div id="alertsBody"></div></div></div>

 <!-- SETTINGS -->
 <div class="page" id="p-set"><div class="wrap">
   <div class="card">
     <div class="grow"><span class="k">Open full settings</span><a href="/config" class="v">Settings &rarr;</a></div>
     <div class="grow"><span class="k">Force a refresh now</span><button class="btn s" style="flex:0;padding:7px 12px" onclick="refresh()">Refresh</button></div>
     <div class="grow"><span class="k">Sync PoolLab now</span><button class="btn s" style="flex:0;padding:7px 12px" onclick="labSync()">Sync</button></div>
     <div class="grow"><span class="k">Add a manual water test</span><button class="btn s" style="flex:0;padding:7px 12px" onclick="openManual()">Add test</button></div>
   </div>
   <div class="mut" style="text-align:center;font-size:12px;margin-top:6px">Pool Stats v2.10.0</div>
 </div></div>

 <div class="tabbar">
   <button class="tab active" data-p="dash" onclick="go('dash')"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 11.5 12 4l9 7.5"/><path d="M5 10v10h14V10"/></svg>Dashboard</button>
   <button class="tab" data-p="hist" onclick="go('hist')"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M7 14l3-3 3 2 5-6"/></svg>History</button>
   <button class="tab" data-p="alerts" onclick="go('alerts')"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9z"/><path d="M13.7 21a2 2 0 0 1-3.4 0"/></svg>Alerts<span class="badge" id="alertBadge" style="display:none">0</span></button>
   <button class="tab" data-p="set" onclick="go('set')"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-2.7 1.1V21a2 2 0 0 1-4 0v-.1A1.6 1.6 0 0 0 7 19.4a1.6 1.6 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0-1.1-2.7H1a2 2 0 0 1 0-4h.1A1.6 1.6 0 0 0 4.6 7a1.6 1.6 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 1.8.3H9a1.6 1.6 0 0 0 1-1.5V1a2 2 0 0 1 4 0v.1a1.6 1.6 0 0 0 1 1.5 1.6 1.6 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0-.3 1.8V7a1.6 1.6 0 0 0 1.5 1H23a2 2 0 0 1 0 4h-.1a1.6 1.6 0 0 0-1.5 1z"/></svg>Settings</button>
 </div>
</div>

<script>
var S=null, LAB=null;
// ---------- theme ----------
var mode=localStorage.getItem('themeMode')||'auto';
function eff(){var sys=matchMedia('(prefers-color-scheme: dark)').matches?'dark':'light';return mode==='auto'?sys:mode;}
function applyTheme(){document.documentElement.setAttribute('data-theme',eff());
 var sun='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4 12H2M22 12h-2M5 5 3.5 3.5M20.5 20.5 19 19M19 5l1.5-1.5M3.5 20.5 5 19"/></svg>';
 var moon='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';
 var auto='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 3a9 9 0 0 0 0 18z" fill="currentColor" stroke="none"/></svg>';
 document.getElementById('themeBtn').innerHTML=mode==='auto'?auto:(eff()==='dark'?moon:sun);}
function cycleTheme(){mode=mode==='auto'?'light':(mode==='light'?'dark':'auto');localStorage.setItem('themeMode',mode);applyTheme();}
matchMedia('(prefers-color-scheme: dark)').addEventListener('change',applyTheme); applyTheme();
// ---------- weather ----------
function wIcon(t){var s='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">';
 var g={sun:'<circle cx="12" cy="12" r="4.5"/><path d="M12 2v2M12 20v2M4 12H2M22 12h-2M5 5 3.6 3.6M20.4 20.4 19 19M19 5l1.4-1.4M3.6 20.4 5 19"/>',
  pcloud:'<circle cx="8" cy="8" r="3"/><path d="M8 2v1M2 8h1M4 4l.7.7M12 4l-.7.7"/><path d="M17.5 20H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 20z"/>',
  cloud:'<path d="M17.5 18H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 18z"/>',
  fog:'<path d="M17.5 14H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 14z"/><path d="M5 18h14M7 21h10"/>',
  rain:'<path d="M17.5 13H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 13z"/><path d="M8 17l-1 3M12 17l-1 3M16 17l-1 3"/>',
  snow:'<path d="M17.5 13H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 13z"/><path d="M8 18h.01M12 18h.01M16 18h.01M10 21h.01M14 21h.01"/>',
  storm:'<path d="M17.5 12H7a4 4 0 0 1 0-8 5 5 0 0 1 9.6 1.3A3.5 3.5 0 0 1 17.5 12z"/><path d="M13 12l-3 5h3l-2 4"/>'};
 return s+(g[t]||g.cloud)+'</svg>';}
function wmo(code){var m={0:['sun','Clear'],1:['sun','Mainly clear'],2:['pcloud','Partly cloudy'],3:['cloud','Overcast'],
 45:['fog','Fog'],48:['fog','Rime fog'],51:['rain','Light drizzle'],53:['rain','Drizzle'],55:['rain','Heavy drizzle'],
 56:['rain','Freezing drizzle'],57:['rain','Freezing drizzle'],61:['rain','Light rain'],63:['rain','Rain'],65:['rain','Heavy rain'],
 66:['rain','Freezing rain'],67:['rain','Freezing rain'],71:['snow','Light snow'],73:['snow','Snow'],75:['snow','Heavy snow'],
 77:['snow','Snow grains'],80:['rain','Showers'],81:['rain','Showers'],82:['rain','Heavy showers'],
 85:['snow','Snow showers'],86:['snow','Snow showers'],95:['storm','Thunderstorm'],96:['storm','Thunderstorm'],99:['storm','Thunderstorm']};
 return m[code]||['cloud','--'];}
function renderWeather(w,place){var el=document.getElementById('weather');if(!el)return;
 if(!w||w.temp==null){el.innerHTML='';el.title='';return;}
 var wi=wmo(w.code);
 var hl=(w.tmin!=null&&w.tmax!=null)?('<small class="whl">(L '+Math.round(w.tmin)+'&deg; / H '+Math.round(w.tmax)+'&deg;)</small>'):'';
 el.innerHTML=wIcon(wi[0])+'<span>'+Math.round(w.temp)+'&deg;</span>'+hl;
 el.title=wi[1]+(place?(' at '+place):'');}
function go(p){document.querySelectorAll('.page').forEach(function(x){x.classList.remove('active')});
 document.getElementById('p-'+p).classList.add('active');
 document.querySelectorAll('.tab').forEach(function(t){t.classList.toggle('active',t.dataset.p===p)});window.scrollTo(0,0);}

// ---------- helpers ----------
function zone(v,lo,hi){if(v==null||lo==null||hi==null)return '';if(v<lo||v>hi)return 'bad';var m=(hi-lo)*0.12;return (v<=lo+m||v>=hi-m)?'warn':'ok';}
function statusInfo(v,lo,hi){if(v==null||lo==null||hi==null)return {sc:'',txt:'--'};
 if(v<lo)return {sc:'bad',txt:'Too low'};if(v>hi)return {sc:'bad',txt:'Too high'};
 var m=(hi-lo)*0.12;if(v<=lo+m)return {sc:'warn',txt:'Low'};if(v>=hi-m)return {sc:'warn',txt:'High'};
 return {sc:'ok',txt:'Ideal'};}
function scColor(sc){return sc==='bad'?'var(--bad)':(sc==='warn'?'var(--warn)':'var(--ok)');}
function cssVar(n){return getComputedStyle(document.documentElement).getPropertyValue(n).trim();}
function resolveColor(c){if(c&&c.indexOf('var(')===0){return cssVar(c.slice(4,-1).trim())||c;}return c;}
function fmtDate(iso){var d=new Date((''+iso).length<=10?iso+'T00:00:00':iso);if(isNaN(d))return iso;return d.toLocaleDateString([],{day:'numeric',month:'short'});}
function friendlyTime(iso){if(!iso)return 'never';var d=new Date(iso);if(isNaN(d))return iso;var now=new Date(),s=(now-d)/1000;var hm=d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
 if(s<60)return 'just now';if(s<3600){var m=Math.floor(s/60);return m+' minute'+(m===1?'':'s')+' ago';}
 if(now.toDateString()===d.toDateString()){var h=Math.floor(s/3600);return h+' hour'+(h===1?'':'s')+' ago';}
 var y=new Date(now);y.setDate(now.getDate()-1);if(y.toDateString()===d.toDateString())return 'yesterday at '+hm;
 var days=Math.floor(s/86400);if(days<=7)return days+' days ago at '+hm;
 var opt={day:'numeric',month:'short'};if(d.getFullYear()!==now.getFullYear())opt.year='numeric';return d.toLocaleDateString([],opt)+' at '+hm;}
function labTime(iso){if(!iso)return 'never';var d=new Date(iso);if(isNaN(d))return iso;var now=new Date();var hm=d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
 if(now.toDateString()===d.toDateString())return 'today at '+hm;
 var y=new Date(now);y.setDate(now.getDate()-1);if(y.toDateString()===d.toDateString())return 'yesterday at '+hm;
 var days=Math.floor((now-d)/86400000);if(days<14)return days+' days ago';
 var weeks=Math.round(days/7);if(weeks<9)return weeks+' wks ago';
 var months=Math.round(days/30.4);return months+' month'+(months===1?'':'s')+' ago';}
function modeName(m){var n={0:'Manual',1:'Scheduled',2:'Timer',3:'Regulated',4:'Cloned',5:'Special',6:'Test',8:'Pulse'};return m==null?'':(n[m]||('mode '+m));}
function showToast(msg,ok){var t=document.getElementById('toast');t.textContent=msg;t.className='toast show '+(ok===false?'bad':'ok');clearTimeout(window._tt);window._tt=setTimeout(function(){t.className='toast';},3200);}
function post(url,body){var opt={method:'POST'};if(body){opt.headers={'Content-Type':'application/x-www-form-urlencoded'};opt.body=body;}return fetch(url,opt).then(function(r){return r.json();}).catch(function(){return {ok:false,message:'Network error'};});}

// ---------- bottle SVG (approved) ----------
function bottle(pct,c1,c2,topLbl,midLbl){pct=Math.max(0,Math.min(100,pct));
 var by0=55,by1=305,fillTop=90,off=(by1-(by1-fillTop)*pct/100)-by0;var id='b'+Math.random().toString(36).slice(2,7);
 return '<svg viewBox="0 0 210 341" width="90" height="146" xmlns="http://www.w3.org/2000/svg">'
 +'<defs><linearGradient id="liq'+id+'" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="'+c1+'"/><stop offset="1" stop-color="'+c2+'"/></linearGradient>'
 +'<mask id="m'+id+'"><rect x="87" y="64" width="107" height="232" rx="13" fill="#fff"/></mask>'
 +'<filter id="s'+id+'" x="-40%" y="-40%" width="180%" height="180%"><feDropShadow dx="0" dy="5" stdDeviation="6" flood-color="#46597a" flood-opacity="0.28"/></filter></defs>'
 +'<rect x="104" y="53" width="28" height="8" rx="4" fill="#f6f9fc" stroke="#ced6e0" stroke-width="2"/>'
 +'<rect x="98" y="30" width="41" height="26" rx="8" fill="#3c434c"/>'
 +'<g stroke="#606a76" stroke-width="2" opacity=".8"><line x1="104" y1="35" x2="104" y2="51"/><line x1="109" y1="35" x2="109" y2="51"/><line x1="114" y1="35" x2="114" y2="51"/><line x1="119" y1="35" x2="119" y2="51"/><line x1="124" y1="35" x2="124" y2="51"/><line x1="129" y1="35" x2="129" y2="51"/><line x1="134" y1="35" x2="134" y2="51"/></g>'
 +'<rect x="78" y="55" width="125" height="250" rx="17" fill="#f6f9fc" filter="url(#s'+id+')"/>'
 +'<g mask="url(#m'+id+')"><g class="liquid" data-final="'+off.toFixed(1)+'" style="transform:translateY(250px)"><rect x="78" y="55" width="125" height="250" fill="url(#liq'+id+')"/></g></g>'
 +'<rect x="78" y="55" width="125" height="250" rx="17" fill="none" stroke="#ced6e0" stroke-width="2"/>'
 +'<g font-family="-apple-system,Segoe UI,Roboto,sans-serif" font-size="15">'
 +'<g stroke="#96a0ad" stroke-width="2"><line x1="71" y1="98" x2="77" y2="98"/><line x1="71" y1="190" x2="77" y2="190"/><line x1="71" y1="284" x2="77" y2="284"/></g>'
 +'<g stroke="#b2bac5" stroke-width="2"><line x1="74" y1="144" x2="77" y2="144"/><line x1="74" y1="236" x2="77" y2="236"/></g>'
 +'<g fill="#828d9b" text-anchor="end"><text x="66" y="103">'+(topLbl||'20 L')+'</text><text x="66" y="195">'+(midLbl||'10 L')+'</text><text x="66" y="289">0 L</text></g></g></svg>';}
function animateLiquid(){requestAnimationFrame(function(){requestAnimationFrame(function(){
 document.querySelectorAll('.liquid').forEach(function(el){el.style.transform='translateY('+el.dataset.final+'px)';});});});}

// ---------- load ----------
function hideLoader(){var l=document.getElementById('loader');if(l)l.classList.add('hide');}
function pollAgeMin(){if(!S||!S.last_ok)return null;var d=new Date(S.last_ok);if(isNaN(d))return null;return (Date.now()-d.getTime())/60000;}
function isStale(){var a=pollAgeMin();if(a==null)return false;var pm=(S&&S.poll_minutes)||15;return a>pm*1.8+3;}
function buildStatus(){var el=document.getElementById('statusbar');if(!el)return;var r=(S&&S.reading)||{};var h='';
 if(isStale())h+='<span class="stbadge bad"><span class="d"></span>Readings '+friendlyTime(S.last_ok)+'</span>';
 if(r.filtration===0)h+='<span class="stbadge warn"><span class="d"></span>Filtration off</span>';
 el.innerHTML=h;}
function load(){
 return Promise.all([fetch('/api/status').then(function(r){return r.json();}),
   fetch('/api/lab-latest').then(function(r){return r.json();}).catch(function(){return {tests:[],configured:false};})])
 .then(function(res){S=res[0];LAB=res[1];render();}).catch(function(e){document.getElementById('sub').textContent='connection error';hideLoader();});}
function render(){
 var r=(S&&S.reading)||{};
 document.getElementById('nick').textContent=r.nickname||'';
 var sub=document.getElementById('sub');
 sub.textContent='Updated '+friendlyTime(S.last_ok)+'  |  every '+S.poll_minutes+' min';
 sub.style.color=isStale()?'var(--warn)':'';
 document.getElementById('errbox').innerHTML=S.last_error?('<div class="wrap"><div class="err">Last error: '+S.last_error+'</div></div>'):'';
 renderWeather(S.weather,S.weather_place);
 buildStatus();
 buildWQ(); buildBalance(); buildBottles(); buildFilt(); buildLabList(); buildAlerts(); buildDashAlerts();
 if(curDetail)showDetail(curDetail);
 animateLiquid();
 drawPhOrp(); loadUsage(); loadCorr();
 hideLoader();
}

// ---------- water quality ----------
function labTest(k){if(!LAB||!LAB.tests)return null;for(var i=0;i<LAB.tests.length;i++)if(LAB.tests[i].key===k)return LAB.tests[i];return null;}
function metrics(){var r=(S&&S.reading)||{};var fc=labTest('fc');
 var m={orp:{title:'Redox (ORP)',gname:'Redox',dot:'ORP',color:'var(--orp)',val:r.orp,unit:'mV',dec:0,lo:(S.orp_range||[])[0],hi:(S.orp_range||[])[1],live:true,src:'orp'},
   ph:{title:'pH',gname:'pH',dot:'pH',color:'var(--ph)',val:r.ph,unit:'',dec:2,lo:(S.ph_range||[])[0],hi:(S.ph_range||[])[1],live:true,src:'ph'},
   cl:{title:'Free chlorine',gname:'Free Cl',dot:'Cl',color:'var(--cl)',val:fc?fc.value:null,unit:fc?(fc.unit||''):'',dec:2,lo:fc?fc.ideal_low:null,hi:fc?fc.ideal_high:null,live:false,ts:fc?fc.ts:null,src:'lab:fc'},
   temp:{title:'Water temp',gname:'Temp',dot:'T',color:'var(--temp)',val:r.temp,unit:'C',dec:1,lo:(S.temp_range||[])[0],hi:(S.temp_range||[])[1],live:true,src:'temp'}};
 return m;}
var THERMO='<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 14.76V5a2 2 0 0 0-4 0v9.76a4 4 0 1 0 4 0z"/></svg>';
function buildWQ(){var m=metrics();var order=['orp','ph','cl','temp'];
 document.getElementById('wq').innerHTML=order.map(function(k){var x=m[k];var si=statusInfo(x.val,x.lo,x.hi);var col=scColor(si.sc);
  var val=(x.val==null?'-':(+x.val).toFixed(x.dec));
  var unit=(x.unit||'').split(' ')[0];
  var rng=(x.lo!=null&&x.hi!=null)?((+x.lo)+'-'+(+x.hi)):'';
  var stale=(x.live&&isStale());
  var age=x.live?(stale?friendlyTime(S.last_ok):'live'):(x.ts?labTime(x.ts):'');
  return '<div class="chip" data-k="'+k+'" onclick="showDetail(this.dataset.k)">'
    +'<div class="qlabel">'+x.gname+'</div>'
    +'<div class="qval">'+val+(unit?('<small>'+unit+'</small>'):'')+'</div>'
    +'<div class="qsub"><span class="w" style="color:'+col+'">'+si.txt+'</span>'+(rng?(' <span class="mut">&middot; '+rng+'</span>'):'')+'</div>'
    +(age?('<div class="qage"'+(stale?' style="color:var(--warn);font-weight:700"':'')+'>'+age+'</div>'):'')
    +'</div>';
 }).join('');
 if(!curDetail)curDetail='orp';}
// ---------- balance (slow lab-tested chemistry) ----------
var BAL_ORDER=['cya','alk','ch','tc','cc','salt','br','po4'];
var BAL_COLOR={cya:'#8b5cf6',alk:'#14b8a6',ch:'#a16207',tc:'#0ea5e9',cc:'#f43f5e',salt:'#0891b2',br:'#7c3aed',po4:'#65a30d'};
function buildBalance(){var card=document.getElementById('balanceCard');
 var items=(LAB&&LAB.tests)?LAB.tests.filter(function(t){return t.key!=='fc';}):[];
 if(!LAB||!LAB.configured||!items.length){card.style.display='none';return;}
 items.sort(function(a,b){return (BAL_ORDER.indexOf(a.key)+99)%100-(BAL_ORDER.indexOf(b.key)+99)%100;});
 card.style.display='';
 document.getElementById('balanceBody').innerHTML=items.map(function(t){var si=statusInfo(t.value,t.ideal_low,t.ideal_high);
  return '<div class="balrow" data-k="'+t.key+'" onclick="showDetail(this.dataset.k)">'
   +'<div class="ln1"><span class="k">'+t.label+'</span><span class="v">'+(t.value==null?'-':(+t.value).toFixed(t.dec))+(t.unit?(' '+t.unit):'')+'</span></div>'
   +'<div class="ln2"><span class="s '+si.sc+'">'+si.txt+'</span><span class="mut">'+labTime(t.ts)+'</span></div></div>';
 }).join('');}
function metricFor(k){var pr=metrics()[k];if(pr)return pr;var t=labTest(k);if(!t)return null;
 return {title:t.label,color:(BAL_COLOR[k]||'var(--cl)'),val:t.value,unit:t.unit||'',dec:t.dec,lo:t.ideal_low,hi:t.ideal_high,live:false,ts:t.ts,src:'lab:'+k};}
var curDetail=null, detailChart=null;
function showDetail(k){curDetail=k;var m=metricFor(k);if(!m)return;
 document.querySelectorAll('.chip').forEach(function(c){c.classList.toggle('sel',c.dataset.k===k);});
 document.getElementById('detailCard').style.display='block';
 document.getElementById('dIcon').innerHTML=k==='temp'?THERMO:'<span style="display:inline-block;width:13px;height:13px;border-radius:50%;background:'+m.color+'"></span>';
 document.getElementById('dTitle').textContent=m.title;
 document.getElementById('dVal').textContent=(m.val==null?'-':(+m.val).toFixed(m.dec));
 document.getElementById('dUnit').innerHTML=m.unit?(' '+m.unit):'';
 var si=statusInfo(m.val,m.lo,m.hi);var ds=document.getElementById('dStatus');ds.textContent=si.txt;ds.style.color=scColor(si.sc);
 document.getElementById('dRange').textContent=(m.lo!=null&&m.hi!=null)?('target '+(+m.lo)+'-'+(+m.hi)+(m.unit?(' '+m.unit):'')):'';
 if(k==='cl'){var cya=labTest('cya');if(cya&&cya.value!=null){var dr=document.getElementById('dRange');dr.textContent=(dr.textContent?dr.textContent+'  |  ':'')+'CYA '+(+cya.value).toFixed(0);}}
 document.getElementById('dAge').textContent=m.live?'live':(m.ts?('measured '+labTime(m.ts)):'');
 var isLab=m.src.indexOf('lab:')===0;
 var url=isLab?('/api/lab-history?param='+m.src.slice(4)):('/api/history?days=7');
 fetch(url).then(function(r){return r.json();}).then(function(h){
   var series=isLab?h.map(function(x){return {t:x.ts,v:x.value};})
                   :h.map(function(x){return {t:x.ts,v:x[m.src]};}).filter(function(x){return x.v!=null;});
   drawDetailChart(series,m.color,m.unit,m.lo,m.hi,isLab);
 }).catch(function(){drawDetailChart([],m.color,m.unit,m.lo,m.hi,isLab);});}
function drawDetailChart(series,color,unit,lo,hi,isLab){
 var tc=getComputedStyle(document.documentElement).getPropertyValue('--muted');
 var gc=getComputedStyle(document.documentElement).getPropertyValue('--line');
 if(detailChart){detailChart.destroy();detailChart=null;}
 var cv=document.getElementById('dChart');
 if(!series.length){var ctx=cv.getContext('2d');ctx.clearRect(0,0,cv.width,cv.height);return;}
 var labels=series.map(function(x){var d=new Date(x.t);if(isNaN(d))return x.t;
   return isLab? d.toLocaleDateString([],{day:'numeric',month:'short'})
              : d.toLocaleString([],{hour:'2-digit',minute:'2-digit',day:'numeric',month:'short'});});
 var col=resolveColor(color);
 var ds=[{label:unit||'value',data:series.map(function(x){return x.v;}),borderColor:col,backgroundColor:col,borderWidth:2.5,tension:.25,pointRadius:isLab?3:0,fill:false}];
 if(lo!=null)ds.push({label:'min',data:series.map(function(){return lo;}),borderColor:tc,borderDash:[5,4],borderWidth:1,pointRadius:0});
 if(hi!=null)ds.push({label:'max',data:series.map(function(){return hi;}),borderColor:tc,borderDash:[5,4],borderWidth:1,pointRadius:0});
 var opts={responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
   plugins:{legend:{display:false},tooltip:{enabled:true,callbacks:{label:function(c){return c.parsed.y;}}}},
   scales:{y:{grid:{color:gc},ticks:{color:tc}},x:{ticks:{maxTicksLimit:6,color:tc,maxRotation:0,autoSkip:true},grid:{display:false}}}};
 detailChart=new Chart(cv,{type:'line',data:{labels:labels,datasets:ds},options:opts});}

// ---------- bottles ----------
var CHEM={cl:{name:'Chlorine',c1:'#63aef5',c2:'#3778d4'},ph:{name:'pH-minus',c1:'#f7c65e',c2:'#e79328'}};
function bottleCard(chem,fc,warn,fin){var meta=CHEM[chem];var has=fc&&fc.remaining!=null;
 var pct=has?Math.round(fc.pct):0;var pillcls=pct<=15?'bad':(pct<=35?'warn':'ok');
 var svg='<div>'+bottle(pct,meta.c1,meta.c2,(fc&&fc.size?fc.size:20)+' L',((fc&&fc.size?fc.size:20)/2)+' L')+'</div>';
 var days=(has&&fc.days_left!=null)?('<div class="row2" style="color:var(--ok)">&asymp; '+Math.round(fc.days_left)+' days left</div><div class="mut" style="font-size:12px">Est. empty: '+(fc.est_empty?fmtDate(fc.est_empty):'-')+'</div>')
   :'<div class="row2" style="color:var(--primary)">Estimate unavailable</div><div class="mut" style="font-size:12px">'+(has?'building forecast':'no baseline yet')+'</div>';
 var ut=(has&&fc.used_today!=null)?(fc.used_today<1?((fc.used_today*1000).toFixed(0)+' mL'):(fc.used_today.toFixed(2)+' L')):'-';
 var av=(has&&fc.avg_per_day!=null)?(fc.avg_per_day.toFixed(2)+' L/day'):'-';
 var rem=has?fc.remaining.toFixed(1):'--';
 return '<div class="card"><div class="bottle">'+svg+'<div class="info">'
  +'<div class="bhead"><span class="bname">'+meta.name+'</span><span class="pct '+pillcls+'">'+(has?pct+'%':'-')+'</span></div>'
  +'<div class="rem">'+rem+' <small>L remaining</small></div>'+days
  +'<div class="divider"></div><div class="statcols">'
  +'<div class="stat"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3s6 6.5 6 11a6 6 0 0 1-12 0c0-4.5 6-11 6-11z"/></svg><div><b>'+ut+'</b><span>used today</span></div></div>'
  +'<div class="stat"><svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M7 14l3-3 3 2 5-6"/></svg><div><b>'+av+'</b><span>avg use</span></div></div></div>'
  +'<div class="btns"><button class="btn p" data-chem="'+chem+'" onclick="openBottle(this.dataset.chem)">Register new</button><button class="btn s" data-chem="'+chem+'" onclick="openHistory(this.dataset.chem)">History</button></div>'
  +'</div></div></div>';}
function buildBottles(){document.getElementById('bottles').innerHTML=
  bottleCard('cl',S.cl_forecast,S.warn_remaining_l,S.final_remaining_l)+
  bottleCard('ph',S.ph_forecast,S.ph_warn_remaining_l,S.ph_final_remaining_l);}

// ---------- filtration ----------
var filtData=[],filtChart=null;
function buildFilt(){var r=S.reading||{};
 document.getElementById('filtState').textContent=r.filtration==null?'':(r.filtration?('Running'+(r.filt_mode!=null?(' - '+modeName(r.filt_mode)):'')):'Off');
 fetch('/api/filter-usage').then(function(x){return x.json();}).then(function(d){filtData=d||[];drawFilt();}).catch(function(){filtData=[];drawFilt();});}
function drawFilt(){var days=filtData.slice(-10);var cv=document.getElementById('chartFilt');
 var kw=S.pump_kw||0.9,pk=S.price_kwh||0.182,op=S.price_offpeak||0.142,cur=S.currency||'';
 if(filtChart){filtChart.destroy();filtChart=null;}
 if(!days.length){var ctx=cv.getContext('2d');ctx.clearRect(0,0,cv.width,cv.height);document.getElementById('filtSummary').innerHTML='<span class="k">Last 7 days</span><span class="v">-</span>';return;}
 var tc=getComputedStyle(document.documentElement).getPropertyValue('--muted');var gc=getComputedStyle(document.documentElement).getPropertyValue('--line');
 var offC=resolveColor('var(--primary)'),pkC=resolveColor('var(--orp)');
 var labels=days.map(function(d){return (''+(d.date||'')).slice(5);});
 var data={labels:labels,datasets:[
  {label:'off-peak',data:days.map(function(d){return +(d.offpeak_hours||0);}),backgroundColor:offC,borderRadius:3,stack:'h'},
  {label:'peak',data:days.map(function(d){return +(d.peak_hours||0);}),backgroundColor:pkC,borderRadius:3,stack:'h'}]};
 var opts={responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},
  plugins:{legend:{display:false},tooltip:{callbacks:{
    label:function(c){return c.dataset.label+': '+(+c.parsed.y).toFixed(1)+' h';},
    footer:function(items){var d=days[items[0].dataIndex];var tot=(+(d.peak_hours||0))+(+(d.offpeak_hours||0));
      var cost=(+(d.peak_hours||0))*kw*pk+(+(d.offpeak_hours||0))*kw*op;
      return 'total '+tot.toFixed(1)+' h  '+cur+cost.toFixed(2);}}}},
  scales:{x:{stacked:true,grid:{display:false},ticks:{color:tc,maxRotation:0,autoSkip:true,maxTicksLimit:10}},
    y:{stacked:true,beginAtZero:true,grid:{color:gc},ticks:{color:tc},title:{display:true,text:'hours',color:tc}}}};
 filtChart=new Chart(cv,{type:'bar',data:data,options:opts});
 var last7=filtData.slice(-7);var p7=last7.reduce(function(s,d){return s+(+(d.peak_hours||0));},0),o7=last7.reduce(function(s,d){return s+(+(d.offpeak_hours||0));},0);
 var cost7=p7*kw*pk+o7*kw*op;
 document.getElementById('filtSummary').innerHTML='<span class="k">Last 7 days</span><span class="v">'+(p7+o7).toFixed(1)+' h &middot; '+cur+cost7.toFixed(2)+'</span>';}

// ---------- bottle modals ----------
var bmChem='cl';
function bmToggle(){document.getElementById('bmWhen').style.display=document.getElementById('bmNow').checked?'none':'block';}
function localNowStr(){var d=new Date();function p(n){return String(n).padStart(2,'0');}return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+'T'+p(d.getHours())+':'+p(d.getMinutes());}
function openBottle(chem){bmChem=chem;document.getElementById('bmTitle').textContent='Register new '+CHEM[chem].name+' bottle';document.getElementById('bmNow').checked=true;var t=document.getElementById('bmTime');t.value=localNowStr();t.max=localNowStr();bmToggle();document.getElementById('bottleModal').classList.add('show');}
function closeBottle(){document.getElementById('bottleModal').classList.remove('show');}
function confirmBottle(){var body='chem='+bmChem;if(!document.getElementById('bmNow').checked){var t=document.getElementById('bmTime').value;if(!t){showToast('Pick a date and time.',false);return;}var c=new Date(t).getTime();if(c>Date.now()){showToast('Date must be in the past.',false);return;}body+='&at_ms='+c;}closeBottle();post('/new-bottle',body).then(function(r){showToast(r.message,r.ok);load();});}
var histChem='cl';
function openHistory(chem){histChem=chem;var nm=CHEM[chem].name;document.getElementById('histTitle').textContent=nm+' bottle history';document.getElementById('histBody').innerHTML='loading...';document.getElementById('histModal').classList.add('show');renderHistory();}
function closeHistory(){document.getElementById('histModal').classList.remove('show');}
function fmtDur(a,b){var s=new Date(a),e=b?new Date(b):new Date();if(isNaN(s))return '';var days=Math.max(0,Math.round((e-s)/86400000));return days+' day'+(days===1?'':'s');}
function renderHistory(){fetch('/api/bottles?chem='+histChem).then(function(r){return r.json();}).then(function(rows){var b=document.getElementById('histBody');b.innerHTML=rows.length?rows.map(rowHtml).join(''):'<div class="mut">No bottles recorded yet.</div>';}).catch(function(){});}
function rowHtml(b){var dur=fmtDur(b.fitted_at,b.replaced_at);var used=(b.litres_used==null?'-':b.litres_used.toFixed(1)+' L');var tag=b.current?'<span style="color:var(--ok);font-size:11px"> (current)</span>':'';var iso=(b.fitted_at||'').replace(/"/g,'');
 return '<div class="histrow" data-id="'+b.id+'" data-iso="'+iso+'" data-size="'+(b.size_l||'')+'" style="border-top:1px solid var(--line);padding:10px 0"><div style="display:flex;justify-content:space-between;gap:8px;align-items:baseline"><div><b>'+friendlyTime(b.fitted_at)+'</b>'+tag+'<div class="mut" style="font-size:12px">lasted '+dur+'  |  used '+used+' of '+(b.size_l==null?'-':b.size_l+' L')+'</div></div><div style="display:flex;gap:8px"><button class="btn s" style="flex:0;padding:5px 9px" onclick="histEdit('+b.id+')">Edit</button><button class="btn s" style="flex:0;padding:5px 9px;color:var(--bad)" onclick="histDelete('+b.id+')">Delete</button></div></div></div>';}
function isoToLocal(iso){var d=new Date(iso);if(isNaN(d))return localNowStr();function p(n){return String(n).padStart(2,'0');}return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+'T'+p(d.getHours())+':'+p(d.getMinutes());}
function histEdit(id){var row=document.querySelector('.histrow[data-id="'+id+'"]');if(!row)return;var iso=row.dataset.iso||'',size=row.dataset.size||'';
 row.innerHTML='<div style="padding:4px 0"><div class="mut" style="font-size:12px;margin-bottom:4px">Date &amp; time fitted (past only):</div><input type="datetime-local" id="he_t_'+id+'" value="'+isoToLocal(iso)+'" max="'+localNowStr()+'" style="width:100%"><div style="display:flex;gap:8px;align-items:center;margin-top:6px"><span class="mut">Size</span><input type="number" step="1" id="he_s_'+id+'" value="'+size+'" style="width:80px"><span class="mut">L</span></div><div style="display:flex;gap:8px;margin-top:10px"><button class="btn p" onclick="histSave('+id+')">Save</button><button class="btn s" onclick="renderHistory()">Cancel</button></div></div>';}
function histSave(id){var t=document.getElementById('he_t_'+id).value,sz=document.getElementById('he_s_'+id).value;var body='id='+id;if(t){var ms=new Date(t).getTime();if(ms>Date.now()){showToast('Date must be in the past.',false);return;}body+='&at_ms='+ms;}if(sz)body+='&size_l='+sz;post('/api/bottle-edit',body).then(function(r){showToast(r.message,r.ok);renderHistory();load();});}
function histDelete(id){post('/api/bottle-delete','id='+id).then(function(r){showToast(r.message,r.ok);renderHistory();load();});}

// ---------- lab list + grid ----------
function buildLabList(){var c=document.getElementById('labListCard');if(!LAB||!LAB.configured||!LAB.tests||!LAB.tests.length){c.style.display='none';return;}c.style.display='block';
 document.getElementById('labWhen').textContent=(LAB.last_ok?('updated '+friendlyTime(LAB.last_ok)):'')+(LAB.due_count?('  |  '+LAB.due_count+' due'):'');
 document.getElementById('labList').innerHTML=LAB.tests.map(function(t){var sc=zone(t.value,t.ideal_low,t.ideal_high);var due=t.overdue?(' <span class="pill2">'+overdueTxt(t)+'</span>'):'';
  return '<div class="grow" data-k="'+t.key+'" onclick="openLabGrid(this.dataset.k)" style="cursor:pointer"><span class="k">'+t.label+due+'</span><span class="v" style="color:'+(sc?scColor(sc):'inherit')+'">'+(t.value==null?'-':(+t.value).toFixed(t.dec))+' <small class="mut">'+labTime(t.ts)+'</small></span></div>';}).join('');}
function overdueTxt(t){if(t.days_since==null||t.cadence==null)return 'due';var d=Math.round(t.days_since-t.cadence);return d>0?(d+' day'+(d===1?'':'s')+' overdue'):'due';}
var curGridKey=null;
function openLabGrid(k){curGridKey=k;var t=labTest(k)||{};document.getElementById('labGridTitle').textContent=(t.label||k)+' - all readings';document.getElementById('labGridBody').innerHTML='loading...';document.getElementById('labGridModal').classList.add('show');renderLabGrid(t);}
function renderLabGrid(t){var k=curGridKey;
 fetch('/api/lab-history?param='+encodeURIComponent(k)).then(function(r){return r.json();}).then(function(rows){rows=(rows||[]).slice().reverse();var b=document.getElementById('labGridBody');if(!rows.length){b.innerHTML='<div class="mut">No readings.</div>';return;}var dec=t.dec==null?2:t.dec,unit=t.unit||'';
  b.innerHTML=rows.map(function(x){var man=(x.account_id==='manual');
   return '<div style="display:flex;justify-content:space-between;align-items:center;gap:10px;border-top:1px solid var(--line);padding:8px 0"><span class="mut">'+labTime(x.ts)+(man?' <span style="color:var(--primary);font-size:11px">&middot; manual</span>':'')+'</span><span style="display:flex;align-items:center;gap:10px"><b>'+(x.value==null?'-':(+x.value).toFixed(dec))+' '+unit+'</b>'+(man?('<button class="btn s" style="flex:0;padding:3px 8px;color:var(--bad)" data-id="'+x.id+'" onclick="delManual(this.dataset.id)">Delete</button>'):'')+'</span></div>';}).join('');}).catch(function(){});}
function delManual(id){post('/api/lab-manual-delete','id='+id).then(function(r){showToast(r.message,r.ok);renderLabGrid(labTest(curGridKey)||{});load();});}
function closeLabGrid(){document.getElementById('labGridModal').classList.remove('show');}

// ---------- history charts ----------
var phOrpChart=null,chartRange=1;
function setRange(d){chartRange=d;[[1,'hr1'],[7,'hr7'],[30,'hr30']].forEach(function(a){document.getElementById(a[1]).classList.toggle('active',a[0]===d);});drawPhOrp();}
function drawPhOrp(){var r=(S&&S.reading)||{};fetch('/api/history?days='+chartRange).then(function(x){return x.json();}).then(function(h){
  var fmt=function(x){var d=new Date(x.ts);return chartRange<=1?d.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'}):(d.getDate()+'/'+(d.getMonth()+1));};
  var data={labels:h.map(fmt),datasets:[{label:'pH',data:h.map(function(x){return x.ph;}),yAxisID:'y1',borderColor:'#38bdf8',borderWidth:2,tension:.3,pointRadius:0},{label:'ORP',data:h.map(function(x){return x.orp;}),yAxisID:'y2',borderColor:'#a78bfa',borderWidth:2,tension:.3,pointRadius:0}]};
  var gc=getComputedStyle(document.documentElement).getPropertyValue('--line');var tc=getComputedStyle(document.documentElement).getPropertyValue('--muted');
  var y1={position:'left',title:{display:true,text:'pH',color:tc},grid:{color:gc},ticks:{color:tc}};var y2={position:'right',title:{display:true,text:'mV',color:tc},grid:{display:false},ticks:{color:tc}};
  var pr=S.ph_range||[],orr=S.orp_range||[];if(pr[0]!=null){y1.min=+(pr[0]*0.95).toFixed(2);y1.max=+(pr[1]*1.05).toFixed(2);}if(orr[0]!=null){y2.min=Math.round(orr[0]*0.9);y2.max=Math.round(orr[1]*1.1);}
  var opts={responsive:true,maintainAspectRatio:false,interaction:{mode:'index',intersect:false},scales:{y1:y1,y2:y2,x:{ticks:{maxTicksLimit:6,color:tc,maxRotation:0,autoSkip:true},grid:{display:false}}},plugins:{legend:{labels:{color:tc,boxWidth:12}}}};
  if(phOrpChart)phOrpChart.destroy();phOrpChart=new Chart(document.getElementById('chartPhOrp'),{type:'line',data:data,options:opts});}).catch(function(){});}
var usageChart=null,usagePeriod='day',usageChem='cl',usageData=[];
function setUsageChem(c){usageChem=c;document.getElementById('uc_cl').classList.toggle('active',c==='cl');document.getElementById('uc_ph').classList.toggle('active',c==='ph');loadUsage();}
function setUsage(p){usagePeriod=p;['day','week','month'].forEach(function(x){document.getElementById('up_'+x).classList.toggle('active',x===p);});drawUsage();}
function loadUsage(){fetch('/api/usage?chem='+usageChem).then(function(r){return r.json();}).then(function(d){usageData=d||[];drawUsage();}).catch(function(){usageData=[];drawUsage();});}
function bucketUsage(){if(usagePeriod==='day')return usageData.slice(-30).map(function(d){return {label:d.date.slice(5),litres:d.litres};});var map={};usageData.forEach(function(d){var key;if(usagePeriod==='month')key=d.date.slice(0,7);else{var dt=new Date(d.date+'T00:00:00');var off=(dt.getDay()+6)%7;var mon=new Date(dt);mon.setDate(dt.getDate()-off);key=mon.toISOString().slice(0,10);}map[key]=(map[key]||0)+d.litres;});var keys=Object.keys(map).sort();var sl=usagePeriod==='month'?keys.slice(-12):keys.slice(-16);return sl.map(function(k){return {label:usagePeriod==='week'?k.slice(5):k,litres:+map[k].toFixed(2)};});}
function drawUsage(){var b=bucketUsage();var tc=getComputedStyle(document.documentElement).getPropertyValue('--muted');var gc=getComputedStyle(document.documentElement).getPropertyValue('--line');
 var total=b.reduce(function(s,x){return s+x.litres;},0);var nm=usageChem==='ph'?'pH-minus':'chlorine';
 document.getElementById('usageSummary').textContent=b.length?(nm+' - total shown '+total.toFixed(1)+' L | latest '+usagePeriod+' '+(b[b.length-1].litres.toFixed(2))+' L'):'builds up as it runs (needs 2+ days)';
 var color=usageChem==='ph'?'#e79328':'#f59e0b';
 var data={labels:b.map(function(x){return x.label;}),datasets:[{data:b.map(function(x){return x.litres;}),backgroundColor:color,borderRadius:4}]};
 var opts={responsive:true,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,grid:{color:gc},ticks:{color:tc}},x:{ticks:{maxTicksLimit:10,color:tc},grid:{display:false}}}};
 if(usageChart)usageChart.destroy();usageChart=new Chart(document.getElementById('chartUsage'),{type:'bar',data:data,options:opts});}
var corrChart=null,corrParam='fc',corrProbe='orp';
var PROBEL={orp:'Redox (mV)',ph:'pH',temp:'Temp (C)'};
function corrStrength(r){var a=Math.abs(r);return a>=0.7?'strong':a>=0.4?'moderate':a>=0.2?'weak':'little';}
function buildCorrSeg(){var keys=(LAB&&LAB.tests?LAB.tests.filter(function(t){return ['fc','tc','cc','cya'].indexOf(t.key)>=0;}):[]);if(!keys.length){document.getElementById('corrCard').style.display='none';return;}document.getElementById('corrCard').style.display='block';
 if(keys.map(function(t){return t.key;}).indexOf(corrParam)<0)corrParam=keys[0].key;
 document.getElementById('corrParamSeg').innerHTML=keys.map(function(t){return '<button data-k="'+t.key+'" class="'+(t.key===corrParam?'active':'')+'" onclick="setCorrParam(this.dataset.k)">'+t.label.split(' ')[0]+'</button>';}).join('');}
function setCorrParam(k){corrParam=k;buildCorrSeg();drawCorr();}
function setCorrProbe(p){corrProbe=p;['orp','ph','temp'].forEach(function(x){document.getElementById('cp_'+x).classList.toggle('active',x===p);});drawCorr();}
function loadCorr(){buildCorrSeg();drawCorr();}
function drawCorr(){if(document.getElementById('corrCard').style.display==='none')return;fetch('/api/lab-correlation?param='+corrParam+'&probe='+corrProbe).then(function(r){return r.json();}).then(function(d){
 var tc=getComputedStyle(document.documentElement).getPropertyValue('--muted');var gc=getComputedStyle(document.documentElement).getPropertyValue('--line');
 var lt=(labTest(corrParam)||{}).label||corrParam.toUpperCase();var pts=(d.points||[]).map(function(p){return {x:p.x,y:p.y};});
 var rtxt=(d.r==null)?('not enough paired data yet ('+(d.n||0)+' points; needs 3+ lab tests taken while the monitor is running)'):('r = '+d.r.toFixed(2)+' (n='+d.n+', '+corrStrength(d.r)+')');
 document.getElementById('corrSummary').textContent=lt+' vs '+PROBEL[corrProbe]+' - '+rtxt;
 var data={datasets:[{data:pts,backgroundColor:'#38bdf8',pointRadius:4}]};
 var opts={responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{title:{display:true,text:lt,color:tc},grid:{color:gc},ticks:{color:tc}},y:{title:{display:true,text:PROBEL[corrProbe],color:tc},grid:{color:gc},ticks:{color:tc}}}};
 if(corrChart)corrChart.destroy();corrChart=new Chart(document.getElementById('chartCorr'),{type:'scatter',data:data,options:opts});}).catch(function(){});}

// ---------- alerts ----------
function computeAlerts(){var al=[];
 if(S.last_error)al.push({sev:'bad',cat:'a',t:'Controller error',d:S.last_error});
 // lab out of range
 if(LAB&&LAB.tests)LAB.tests.forEach(function(t){var sc=zone(t.value,t.ideal_low,t.ideal_high);if(sc==='bad'){var lowhi=(t.value<t.ideal_low)?'low':'high';al.push({sev:t.key==='cc'?'bad':'warn',cat:'a',t:t.label+' '+lowhi,d:(t.value==null?'':(+t.value).toFixed(t.dec))+(t.unit?(' '+t.unit):'')+' (target '+(+t.ideal_low)+'-'+(+t.ideal_high)+')'});}});
 // low bottles
 [['cl',S.cl_forecast,S.warn_remaining_l,'Chlorine'],['ph',S.ph_forecast,S.ph_warn_remaining_l,'pH-minus']].forEach(function(x){var fc=x[1];if(fc&&fc.remaining!=null&&fc.remaining<=(x[2]||5)){al.push({sev:'warn',cat:'r',t:x[3]+' getting low',d:fc.remaining.toFixed(1)+' L left'+(fc.days_left!=null?(' (~'+Math.round(fc.days_left)+' days)'):'')});}});
 // overdue tests
 if(LAB&&LAB.tests)LAB.tests.forEach(function(t){if(t.overdue)al.push({sev:'info',cat:'r',t:t.label+' test overdue',d:'last '+friendlyTime(t.ts)+' - '+overdueTxt(t)});});
 return al;}
function buildDashAlerts(){var al=computeAlerts();var body=document.getElementById('dashAlertsBody');if(!body)return;
 if(!al.length){body.innerHTML='<div class="mut" style="font-size:13px;padding:8px 2px">All good - nothing needs attention.</div>';return;}
 body.innerHTML=al.slice(0,5).map(function(a){return '<div class="darow"><span class="dadot '+a.sev+'"></span><div><div class="dat">'+a.t+'</div><div class="dad">'+a.d+'</div></div></div>';}).join('')
  +(al.length>5?('<div class="mut" style="font-size:11.5px;padding:8px 2px 0">+ '+(al.length-5)+' more</div>'):'');}
function buildAlerts(){var al=computeAlerts();
 var badge=document.getElementById('alertBadge');if(al.length){badge.style.display='flex';badge.textContent=al.length;}else badge.style.display='none';
 var need=al.filter(function(a){return a.cat==='a';}),rem=al.filter(function(a){return a.cat==='r';});
 function ic(sev){var w='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/></svg>';var i='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>';return sev==='info'?i:w;}
 function row(a){return '<div class="alert"><div class="ai '+a.sev+'">'+ic(a.sev)+'</div><div><div class="at">'+a.t+'</div><div class="ad">'+a.d+'</div></div></div>';}
 var html='';
 if(need.length)html+='<div class="sect">Needs attention</div>'+need.map(row).join('');
 if(rem.length)html+='<div class="sect">Reminders</div>'+rem.map(row).join('');
 if(!al.length)html='<div class="card" style="text-align:center;color:var(--muted)">All good - nothing needs attention.</div>';
 document.getElementById('alertsBody').innerHTML=html;}

// ---------- misc ----------
function labSync(){showToast('Syncing PoolLab...');post('/lab-refresh').then(function(r){showToast(r.message,r.ok);load();});}
var MT_KEYS=['ph','fc','tc','cc','cya','alk','ch','salt'];
function openManual(){var t=document.getElementById('mt_time');t.value=localNowStr();t.max=localNowStr();
 MT_KEYS.forEach(function(k){document.getElementById('mt_'+k).value='';});
 document.getElementById('manualModal').classList.add('show');}
function closeManual(){document.getElementById('manualModal').classList.remove('show');}
function saveManual(){var body='';var t=document.getElementById('mt_time').value;
 if(t){var ms=new Date(t).getTime();if(ms>Date.now()){showToast('Date must be in the past.',false);return;}body='at_ms='+ms;}
 var any=false;MT_KEYS.forEach(function(k){var v=document.getElementById('mt_'+k).value;
  if(v!==''&&v!=null){body+=(body?'&':'')+k+'='+encodeURIComponent(v);any=true;}});
 if(!any){showToast('Enter at least one value.',false);return;}
 closeManual();post('/api/lab-manual',body).then(function(r){showToast(r.message,r.ok);load();});}
function refresh(){var ptr=document.getElementById('ptr');ptr.textContent='Refreshing...';ptr.style.transform='translateY(0)';
 fetch('/poll-now',{method:'POST'}).catch(function(){}).then(function(){return load();}).then(function(){ptr.style.transform='translateY(-40px)';setTimeout(function(){ptr.textContent='\\u2193 pull to refresh';},300);});}
var ptrStart=null;
function atTop(){return (window.scrollY||(document.scrollingElement||document.documentElement).scrollTop||0)<=0;}
addEventListener('touchstart',function(e){ptrStart=atTop()?e.touches[0].clientY:null;},{passive:true});
addEventListener('touchmove',function(e){if(ptrStart==null)return;var dy=e.touches[0].clientY-ptrStart;if(dy>0)document.getElementById('ptr').style.transform='translateY('+Math.min(dy-40,12)+'px)';},{passive:true});
addEventListener('touchend',function(e){if(ptrStart==null)return;var dy=e.changedTouches[0].clientY-ptrStart;if(dy>70)refresh();else document.getElementById('ptr').style.transform='translateY(-40px)';ptrStart=null;},{passive:true});
load();setInterval(load,60000);
</script></body></html>
"""


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
    ph_bottle = kv_get("ph_bottle_l", 20.0)
    ph_warn   = kv_get("ph_warn_remaining_l", 5.0)
    ph_final  = kv_get("ph_final_remaining_l", 0.5)
    ph_pump   = kv_get("ph_pump_lph", 1.5)
    labhrs = kv_get("labcom_poll_hours", 1.0)
    poll   = kv_get("poll_minutes", POLL_MINUTES)
    gpl    = kv_get("liquid_cl_gpl", 48.0)
    pump   = kv_get("pump_kw", 0.9)
    price  = kv_get("price_kwh", 0.182)
    priceoff = kv_get("price_offpeak", 0.142)
    import html as _h
    offwin = _h.escape(str(kv_get("offpeak_window", "22:00-06:00")), quote=True)
    curr   = _h.escape(str(kv_get("currency", "EUR")), quote=True)
    port   = val("SMTP_PORT") or "587"
    latest_cya = _latest_lab_value("cya")
    cya_js = "null" if latest_cya is None else repr(round(float(latest_cya), 1))
    avgdays = kv_get("bottle_avg_days", 14)
    _labrows, _cadrows = [], []
    for _k, _lbl in LAB_TARGET_FIELDS:
        _lo, _hi = _lab_range(_k, None, None)
        _lo = "" if _lo is None else (f"{_lo:g}")
        _hi = "" if _hi is None else (f"{_hi:g}")
        _labrows.append(
            f'<div class="setrow"><label>{_lbl}</label><span>'
            f'<input id="lab_lo_{_k}" type="number" step="0.1" value="{_lo}" style="width:64px">'
            f'<span class="u">to</span>'
            f'<input id="lab_hi_{_k}" type="number" step="0.1" value="{_hi}" style="width:64px"></span></div>')
        _cad = _lab_cadence(_k)
        _cad = "" if _cad is None else (f"{_cad:g}")
        _cadrows.append(
            f'<div class="setrow"><label>{_lbl}</label><span>'
            f'<input id="lab_cad_{_k}" type="number" step="1" min="1" value="{_cad}" style="width:70px">'
            f'<span class="u">days</span></span></div>')
    labrows = "\n".join(_labrows)
    cadrows = "\n".join(_cadrows)
    _liverows = []
    for _k, _lbl in (("ph", "pH"), ("orp", "ORP / Redox"), ("temp", "Water temp")):
        _lo, _hi = _live_range(_k)
        _lo = "" if _lo is None else (f"{_lo:g}")
        _hi = "" if _hi is None else (f"{_hi:g}")
        _liverows.append(
            f'<div class="setrow"><label>{_lbl}</label><span>'
            f'<input id="live_lo_{_k}" type="number" step="0.1" value="{_lo}" style="width:64px">'
            f'<span class="u">to</span>'
            f'<input id="live_hi_{_k}" type="number" step="0.1" value="{_hi}" style="width:64px"></span></div>')
    liverows = "\n".join(_liverows)
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
<div class="panel"><div class="lbl2">Chlorine bottle &amp; polling</div>
 <div class="setrow"><label>Bottle size</label><span><input id="bottle_l" type="number" step="1" value="{bottle}"><span class="u">L</span></span></div>
 <div class="setrow"><label>1st alert at</label><span><input id="warn" type="number" step="0.5" value="{warn}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>Final alert at</label><span><input id="final" type="number" step="0.1" value="{final}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>Check every</label><span><input id="poll" type="number" step="1" min="1" value="{poll}"><span class="u">min</span></span></div>
 <div class="setrow"><label>Liquid Cl strength</label><span><input id="gpl" type="number" step="1" value="{gpl}"><span class="u">g/L</span></span></div>
 <div class="setrow"><label>Forecast avg window</label><span><input id="bottle_avg_days" type="number" step="1" min="1" value="{avgdays}"><span class="u">days</span></span></div>
 <div class="hint">Salt strength converts the cell output to a liquid-Cl mL equivalent. Forecast window = days of history used to predict how long each bottle lasts.</div>
</div>
<div class="panel"><div class="lbl2">pH-minus bottle</div>
 <div class="setrow"><label>Bottle size</label><span><input id="ph_bottle_l" type="number" step="1" value="{ph_bottle}"><span class="u">L</span></span></div>
 <div class="setrow"><label>1st alert at</label><span><input id="ph_warn" type="number" step="0.5" value="{ph_warn}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>Final alert at</label><span><input id="ph_final" type="number" step="0.1" value="{ph_final}"><span class="u">L left</span></span></div>
 <div class="setrow"><label>pH pump flow</label><span><input id="ph_pump_lph" type="number" step="0.1" value="{ph_pump}"><span class="u">L/h</span></span></div>
 <div class="hint">pH usage = pump run-time x this flow rate. Calibrate it so "pH dosed today" matches the Klereo app, then usage &amp; alerts stay accurate.</div>
</div>
<div class="panel"><div class="lbl2">Live reading targets (pH / ORP / temp)</div>
{liverows}
 <div class="hint">Colour bands for the live dashboard tiles. Leave to use sensible defaults; overrides the Klereo regulation limits.</div>
</div>
<div class="panel"><div class="lbl2">Filtration cost</div>
 <div class="setrow"><label>Pump power</label><span><input id="pump" type="number" step="0.1" value="{pump}"><span class="u">kW</span></span></div>
 <div class="setrow"><label>Day (peak) price</label><span><input id="price" type="number" step="0.001" value="{price}"><span class="u">/kWh</span></span></div>
 <div class="setrow"><label>Night (off-peak) price</label><span><input id="price_off" type="number" step="0.001" value="{priceoff}"><span class="u">/kWh</span></span></div>
 <div class="setrow"><label>Off-peak hours</label><span><input id="offwin" type="text" value="{offwin}" style="width:120px"></span></div>
 <div class="setrow"><label>Currency symbol</label><span><input id="currency" type="text" value="{curr}" style="width:60px"></span></div>
 <div class="hint">Prices incl. VAT. Off-peak like "22:00-06:00" (comma-separate multiple). Flat tariff? Set both prices the same. Variable-speed pump? Use an average kW.</div>
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
<div class="panel"><div class="lbl2">PoolLab / LabCom (lab tests)</div>
 <div class="setrow"><label>API token</label><input id="LABCOM_TOKEN" type="password" placeholder="unchanged" autocomplete="off"></div>
 <div class="setrow"><label>Sync every</label><span><input id="labcom_poll_hours" type="number" step="0.5" min="0.5" value="{labhrs}"><span class="u">h</span></span></div>
 <div class="hint">Get a read-only token at <a href="https://labcom.cloud/pages/user-setting" target="_blank">labcom.cloud &rarr; user settings</a>. Your PoolLab must sync to the LabCom cloud. Manual tests are pulled in and shown on the dashboard.</div>
 <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
   <button class="ghost" type="button" onclick="labSync()">Sync now</button>
   <a href="/api/lab-raw" target="_blank" style="align-self:center">View raw lab data</a>
 </div>
</div>
<div class="panel"><div class="lbl2">Lab test targets (colour bands)</div>
{labrows}
 <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:12px;align-items:center">
   <button class="ghost" type="button" onclick="suggestFC()">Suggest FC from CYA</button>
   <span class="hint" id="cyaHint" style="margin:0"></span>
 </div>
 <div class="hint">Used to colour the lab tiles and draw the target lines on charts. These override any range set in LabCom. Free-chlorine suggestion uses the FC/CYA ratio (min 7.5%, up to ~15% of CYA).</div>
</div>
<div class="panel"><div class="lbl2">Lab test reminders (retest cadence)</div>
{cadrows}
 <div class="hint">How often each test should be re-run. The dashboard flags a metric when it's overdue.</div>
</div>
<script>window.LATEST_CYA={cya_js};</script>
<div class="panel"><div class="lbl2">Filtration control (test)</div>
 <div class="hint" style="margin-top:0">Drive the pump directly. Start/Stop force it into manual; Regulated hands control back to Klereo. Each takes a few seconds.</div>
 <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:10px">
   <button type="button" onclick="filterCtl('on')">Start (manual)</button>
   <button type="button" class="ghost" onclick="filterCtl('off')">Stop (manual)</button>
   <button type="button" class="ghost" onclick="filterCtl('auto')">Regulated (auto)</button>
 </div>
</div>
<button type="button" onclick="saveConfig()" style="width:100%;margin-top:14px">Save settings</button>
<div class="hint" style="text-align:center;margin-top:8px">Saved on the server; secrets are never shown back here.</div>
<div class="panel"><div class="lbl2">Diagnostics</div>
 <a href="/api/raw" target="_blank">View raw Klereo chemistry fields (/api/raw)</a>
 <div class="hint">Handy for checking values like the salt cell (Elec_GramDone).</div>
</div>
<div style="text-align:center;color:#64748b;font-size:12px;margin:18px 0 8px">Pool Stats v{APP_VERSION}</div>
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
async function labSync(){ showToast('Syncing PoolLab...'); const r=await postAction('/lab-refresh'); showToast(r.message, r.ok); }
function suggestFC(){
 const cya=window.LATEST_CYA;
 if(cya==null){ showToast('No CYA reading yet - sync PoolLab first.', false); return; }
 const lo=Math.round(cya*0.075*10)/10, hi=Math.round(cya*0.15*10)/10;
 document.getElementById('lab_lo_fc').value=lo;
 document.getElementById('lab_hi_fc').value=hi;
 showToast('FC target set to '+lo+'-'+hi+' from CYA '+cya+' (not yet saved)', true);
}
(function(){ const h=document.getElementById('cyaHint');
 if(h) h.textContent = (window.LATEST_CYA==null)? 'no CYA reading yet' : ('latest CYA: '+window.LATEST_CYA); })();
async function filterCtl(a){ showToast('Sending filtration command...'); const r=await postAction('/api/filter-control','action='+a); showToast(r.message, r.ok); }
async function saveConfig(){
 const ids=['bottle_l','warn','final','bottle_avg_days','ph_bottle_l','ph_warn','ph_final','ph_pump_lph','labcom_poll_hours','poll','gpl','pump','price','price_off','offwin','currency','KLEREO_LOGIN','KLEREO_POOL_ID','KLEREO_PASSWORD','SMTP_HOST','SMTP_PORT','SMTP_USER','ALERT_TO','SMTP_PASS','LABCOM_TOKEN'];
 const p=new URLSearchParams();
 ids.forEach(id=>{const el=document.getElementById(id); if(el && el.value!=='') p.append(id, el.value);});
 document.querySelectorAll('[id^="lab_lo_"],[id^="lab_hi_"],[id^="lab_cad_"],[id^="live_lo_"],[id^="live_hi_"]').forEach(el=>{ if(el.value!=='') p.append(el.id, el.value); });
 const r=await postAction('/config', p.toString()); showToast(r.message, r.ok);
 ['KLEREO_PASSWORD','SMTP_PASS'].forEach(id=>document.getElementById(id).value='');
}
</script></body></html>""")
    return head + fields + tail


def status_payload():
    ph_b = current_bottle("ph")
    r = kv_get("last_reading") or {}
    return {
        "reading": kv_get("last_reading"),
        "cl_forecast": bottle_forecast("cl"),
        "ph_forecast": bottle_forecast("ph"),
        "ph_range": list(_live_range("ph", r.get("ph_min"), r.get("ph_max"))),
        "orp_range": list(_live_range("orp", r.get("orp_min"), r.get("orp_max"))),
        "temp_range": list(_live_range("temp")),
        "last_ok": kv_get("last_ok"),
        "last_error": kv_get("last_error"),
        "bottle_l": kv_get("bottle_l", DEF_BOTTLE),
        "warn_remaining_l": kv_get("warn_remaining_l", 5.0),
        "final_remaining_l": kv_get("final_remaining_l", 0.5),
        "baseline_total_time": kv_get("baseline_total_time"),
        "bottle_fitted_at": kv_get("bottle_fitted_at"),
        "notified_warn": kv_get("notified_warn"),
        "notified_final": kv_get("notified_final"),
        # pH-minus bottle
        "ph_bottle_l": kv_get("ph_bottle_l", 20.0),
        "ph_warn_remaining_l": kv_get("ph_warn_remaining_l", 5.0),
        "ph_final_remaining_l": kv_get("ph_final_remaining_l", 0.5),
        "ph_bottle_fitted_at": (ph_b["fitted_at"] if ph_b else None),
        "ph_notified_warn": kv_get("ph_notified_warn"),
        "ph_notified_final": kv_get("ph_notified_final"),
        "ph_pump_lph": kv_get("ph_pump_lph", 1.5),
        "used_ph": bottle_used_l("ph"),
        "poll_minutes": kv_get("poll_minutes", POLL_MINUTES),
        "cover_ts": kv_get("cover_ts"),
        "cover_state": kv_get("cover_state"),
        "pump_kw": kv_get("pump_kw", 0.9),
        "price_kwh": kv_get("price_kwh", 0.182),
        "price_offpeak": kv_get("price_offpeak", 0.142),
        "offpeak_window": kv_get("offpeak_window", "22:00-06:00"),
        "currency": kv_get("currency", "EUR"),
        "weather": kv_get("weather"),
        "weather_place": kv_get("weather_place", ""),
    }


def lab_latest_payload():
    """Latest lab measurement per parameter + connection status."""
    with db() as c:
        rows = [dict(r) for r in c.execute(
            "SELECT t.* FROM lab_tests t JOIN "
            "(SELECT param_key, MAX(ts) AS mx FROM lab_tests GROUP BY param_key) g "
            "ON t.param_key=g.param_key AND t.ts=g.mx ORDER BY t.parameter").fetchall()]
    order = {k: i for i, k in enumerate(
        ["ph", "fc", "tc", "cc", "cya", "alk", "ch", "salt", "br", "po4"])}
    now = datetime.now(timezone.utc).astimezone()
    tests, due = [], 0
    for r in rows:
        _key, label, dec = _param_meta(r["parameter"])
        lo, hi = _lab_range(r["param_key"], r["ideal_low"], r["ideal_high"])
        days_since = None
        try:
            days_since = (now - datetime.fromisoformat(r["ts"])).total_seconds() / 86400.0
        except (ValueError, TypeError):
            pass
        cad = _lab_cadence(r["param_key"])
        overdue = bool(cad is not None and days_since is not None and days_since > cad)
        if overdue:
            due += 1
        tests.append({"key": r["param_key"], "parameter": r["parameter"], "label": label,
                      "dec": dec, "value": r["value"], "unit": r["unit"],
                      "ideal_low": lo, "ideal_high": hi,
                      "ts": r["ts"], "operator": r["operator"], "comment": r["comment"],
                      "days_since": days_since, "cadence": cad, "overdue": overdue})
    tests.sort(key=lambda t: order.get(t["key"], 99))
    # show the lab panels if PoolLab is connected OR any manual tests exist
    return {"tests": tests, "configured": bool(cfg_get("LABCOM_TOKEN")) or bool(tests),
            "last_ok": kv_get("lab_last_ok"), "last_error": kv_get("lab_last_error"),
            "pool_name": kv_get("lab_pool_name"), "due_count": due,
            "poll_hours": kv_get("labcom_poll_hours", 1.0)}


def lab_history_payload(param_key, days=730):
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc).astimezone() - timedelta(days=days)).isoformat()
    with db() as c:
        rows = c.execute("SELECT rowid AS id,ts,value,ideal_low,ideal_high,operator,account_id FROM lab_tests "
                         "WHERE param_key=? AND ts>=? AND value IS NOT NULL ORDER BY ts",
                         (param_key, cutoff)).fetchall()
    lo, hi = _lab_range(param_key, None, None)   # default band for the guide lines
    out = []
    for r in rows:
        d = dict(r)
        elo, ehi = _lab_range(param_key, d.get("ideal_low"), d.get("ideal_high"))
        d["ideal_low"], d["ideal_high"] = elo, ehi
        out.append(d)
    return out


def lab_raw_payload():
    return kv_get("lab_last_raw") or {}


# Manually-entered water tests (e.g. strip/kit) -- stored in lab_tests under a
# dedicated 'manual' account so the LabCom sync never touches them.
MANUAL_PARAMS = {
    "ph":   ("pH", "", 2),
    "fc":   ("Free chlorine", "mg/l", 2),
    "tc":   ("Total chlorine", "mg/l", 2),
    "cc":   ("Combined chlorine", "mg/l", 2),
    "cya":  ("Cyanuric acid (CYA)", "mg/l", 0),
    "alk":  ("Total alkalinity", "mg/l", 0),
    "ch":   ("Calcium hardness", "mg/l", 0),
    "salt": ("Salt", "mg/l", 0),
}


def add_manual_test(form):
    """Insert one manual reading row per supplied parameter. Returns count added."""
    at = form.get("at_ms")
    try:
        ts = (datetime.fromtimestamp(float(at) / 1000).astimezone().isoformat(timespec="seconds")
              if at else now_iso())
    except (TypeError, ValueError):
        ts = now_iso()
    added = 0
    with db() as c:
        for key, (label, unit, _dec) in MANUAL_PARAMS.items():
            raw = form.get(key)
            if raw is None or str(raw).strip() == "":
                continue
            try:
                val = float(raw)
            except ValueError:
                continue
            if abs(val) >= LAB_SENTINEL:
                continue
            mid = "manual-" + hashlib.md5(("%s|%s|%f" % (ts, key, time.time())).encode()).hexdigest()[:16]
            c.execute("INSERT INTO lab_tests "
                      "(meas_id,ts,account_id,parameter,param_key,value,unit,"
                      " ideal_low,ideal_high,operator,comment) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                      (mid, ts, "manual", label, key, val, unit, -1, -1,
                       "manual", "manual entry"))
            added += 1
    return added


def delete_manual_test(row_id):
    with db() as c:
        cur = c.execute("DELETE FROM lab_tests WHERE rowid=? AND account_id='manual'", (row_id,))
    return cur.rowcount


def _pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / (sxx ** 0.5 * syy ** 0.5)


LAB_CORR_PROBES = {"orp": "orp", "ph": "ph", "temp": "temp"}


def lab_correlation_payload(lab_key="fc", probe="orp", window_h=12.0):
    """Pair each lab measurement with the nearest Klereo reading (within window_h
    hours) and return the scatter points + Pearson r. x = lab value, y = probe."""
    import bisect
    col = LAB_CORR_PROBES.get(probe, "orp")
    with db() as c:
        labs = c.execute("SELECT ts,value FROM lab_tests WHERE param_key=? AND value IS NOT NULL "
                         "ORDER BY ts", (lab_key,)).fetchall()
        reads = c.execute(f"SELECT ts,{col} AS pv FROM readings WHERE {col} IS NOT NULL "
                          "ORDER BY ts").fetchall()
    R = []
    for r in reads:
        try:
            R.append((datetime.fromisoformat(r["ts"]).timestamp(), r["pv"]))
        except (ValueError, TypeError):
            pass
    Rt = [x[0] for x in R]
    pts = []
    for l in labs:
        try:
            lt = datetime.fromisoformat(l["ts"]).timestamp()
        except (ValueError, TypeError):
            continue
        i = bisect.bisect_left(Rt, lt)
        best, bestd = None, None
        for j in (i - 1, i):
            if 0 <= j < len(R):
                d = abs(R[j][0] - lt)
                if bestd is None or d < bestd:
                    bestd, best = d, R[j]
        if best is not None and bestd is not None and bestd <= window_h * 3600:
            pts.append({"ts": l["ts"], "x": l["value"], "y": best[1]})
    r = _pearson([p["x"] for p in pts], [p["y"] for p in pts]) if len(pts) >= 3 else None
    return {"points": pts, "r": r, "n": len(pts), "lab_key": lab_key, "probe": probe}


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
    pat = re.compile(r"(elec|chlor|salt|sel|prod|hyb|trait|redox|orp|couv|cover|conso|"
                     r"\bph\b|ph_|_ph|pompe|debit|acid|correc|volet|dose)", re.I)
    out = {}
    for src, vals in (("params", p), ("ExtraParams", e)):
        if isinstance(vals, dict):
            for k, v in vals.items():
                if pat.search(k):
                    out[f"{src}.{k}"] = v
    # Full outputs list, so we can identify which output is the pH dosing pump
    # and how its run-time (totalTime/todayTime) is reported.
    out["outs"] = kv_get("last_outs") or []
    return out


def _parse_windows(spec):
    """'22:00-06:00' or comma-separated ranges -> list of (start_min, end_min)."""
    wins = []
    for part in str(spec or "").split(","):
        part = part.strip()
        if "-" not in part:
            continue
        a, b = part.split("-", 1)
        try:
            ah, am = (int(x) for x in a.split(":"))
            bh, bm = (int(x) for x in b.split(":"))
            wins.append((ah * 60 + am, bh * 60 + bm))
        except ValueError:
            continue
    return wins


def _is_offpeak(dt, wins):
    m = dt.hour * 60 + dt.minute
    for s, e in wins:
        if s <= e:
            if s <= m < e:
                return True
        elif m >= s or m < e:      # window wraps past midnight
            return True
    return False


def filter_usage_payload():
    """Filtration hours per calendar day, split into peak / off-peak by when the
    filter actually ran (from consecutive odometer readings + timestamps).
    Returns [{date, hours, peak_hours, offpeak_hours}]."""
    wins = _parse_windows(kv_get("offpeak_window", "22:00-06:00"))
    with db() as c:
        rows = c.execute("SELECT ts, filt_total FROM readings "
                         "WHERE filt_total IS NOT NULL ORDER BY ts").fetchall()
    agg = {}   # date -> [peak_seconds, offpeak_seconds]
    prev = None
    for r in rows:
        try:
            t = datetime.fromisoformat(r["ts"])
        except (ValueError, TypeError):
            prev = None
            continue
        ft = r["filt_total"]
        if prev is not None:
            pft, pt = prev
            delta = (ft or 0) - (pft or 0)
            if delta > 0:
                mid = pt + (t - pt) / 2          # attribute to the interval midpoint
                key = mid.date().isoformat()
                bucket = agg.setdefault(key, [0.0, 0.0])
                bucket[1 if _is_offpeak(mid, wins) else 0] += delta
        prev = (ft, t)
    out = []
    for d in sorted(agg):
        pk, off = agg[d]
        out.append({"date": d, "peak_hours": round(pk / 3600.0, 2),
                    "offpeak_hours": round(off / 3600.0, 2),
                    "hours": round((pk + off) / 3600.0, 2)})
    return out[-120:]


def usage_payload(chem="cl"):
    """Product used per calendar day, robust to power events (see _daily_usage).
    Returns [{date, litres}]."""
    if chem not in CHEM:
        chem = "cl"
    du = _daily_usage(chem)
    return [{"date": d, "litres": round(du[d], 3)} for d in sorted(du)][-120:]


def diag_usage_payload(chem="cl"):
    """Diagnostics for 'used today': shows whether the counter rolled back or the
    poller went stale, and both ways of computing today, plus recent readings."""
    if chem not in CHEM:
        chem = "cl"
    col = CHEM[chem]["odo"]
    now = datetime.now(timezone.utc).astimezone()
    today = now.date().isoformat()
    extra = kv_get("last_extra") or {}
    lr = kv_get("last_reading") or {}
    with db() as c:
        recent = [dict(r) for r in c.execute(
            f"SELECT ts,{col} AS odo FROM readings WHERE {col} IS NOT NULL "
            f"ORDER BY ts DESC LIMIT 40").fetchall()][::-1]
        y_end = c.execute(f"SELECT MAX({col}) AS v FROM readings "
                          f"WHERE date(ts)<? AND {col} IS NOT NULL", (today,)).fetchone()["v"]
        latest = c.execute(f"SELECT ts,{col} AS odo FROM readings "
                           f"WHERE {col} IS NOT NULL ORDER BY ts DESC LIMIT 1").fetchone()
    factor = _chem_factor(chem)
    last_ok = kv_get("last_ok")
    mins_since = None
    try:
        mins_since = round((now - datetime.fromisoformat(last_ok)).total_seconds() / 60.0, 1)
    except (ValueError, TypeError):
        pass
    simple_ml = None
    if latest and latest["odo"] is not None and y_end is not None:
        simple_ml = round(max(latest["odo"] - y_end, 0.0) * factor * 1000.0, 1)
    incr = today_dose_ml(chem)
    # count backward steps in recent history (a sign of reboot rollbacks/glitches)
    backsteps = sum(1 for i in range(1, len(recent))
                    if recent[i]["odo"] is not None and recent[i - 1]["odo"] is not None
                    and recent[i]["odo"] < recent[i - 1]["odo"])
    return {
        "chem": chem, "now": now.isoformat(),
        "last_ok": last_ok, "minutes_since_last_poll": mins_since,
        "poll_minutes": kv_get("poll_minutes", POLL_MINUTES),
        "odo_column": col,
        "current_reading_odo": lr.get(col),
        "latest_stored_odo": (latest["odo"] if latest else None),
        "latest_stored_ts": (latest["ts"] if latest else None),
        "yesterday_end_odo": y_end,
        "today_via_current_minus_yesterday_ml": simple_ml,
        "today_via_increment_sum_ml": (round(incr, 1) if incr is not None else None),
        "HybChl_TotalTime": extra.get("HybChl_TotalTime"),
        "HybChl_TodayTime": extra.get("HybChl_TodayTime"),
        "HybChl_TodayTime_as_ml": (round(extra.get("HybChl_TodayTime") * (lr.get("debit") or 15.0) / 36.0, 1)
                                   if extra.get("HybChl_TodayTime") is not None else None),
        "backward_steps_in_last_40": backsteps,
        "recent_readings": [{"ts": r["ts"], "odo": r["odo"]} for r in recent],
    }


def _odo_at(chem, at_ms):
    """(odometer, debit, fitted_iso) nearest to a past time; None target -> now."""
    col = CHEM[chem]["odo"]
    if at_ms is None:
        r = kv_get("last_reading") or {}
        odo = r.get(col)
        if odo is None:
            with _lock:
                r = poll_once()
            odo = r.get(col)
        return odo, r.get("debit"), now_iso()
    target = at_ms / 1000.0
    with db() as c:
        rows = c.execute(f"SELECT ts,{col} AS odo,debit FROM readings "
                         f"WHERE {col} IS NOT NULL ORDER BY ts").fetchall()
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
    if chosen is None:
        return None, None, None
    fitted = datetime.fromtimestamp(target).astimezone().isoformat(timespec="minutes")
    return chosen["odo"], chosen["debit"], fitted


def _factor_for(chem, debit):
    if chem == "cl":
        return (float(debit) / 36000.0) if debit is not None else (15.0 / 36000.0)
    return float(kv_get("ph_pump_lph", 1.5)) / 3600.0


def do_new_bottle(chem="cl", at_ms=None, size_l=None):
    """Register a new bottle for a chemical. at_ms (epoch ms) backdates to the
    odometer reading nearest that time; None means 'now'."""
    if chem not in CHEM:
        chem = "cl"
    odo, debit, fitted = _odo_at(chem, at_ms)
    if odo is None:                       # no history at all -> use now
        if at_ms is not None:
            return do_new_bottle(chem, None, size_l)
        return
    factor = _factor_for(chem, debit)
    size = float(size_l) if size_l is not None else float(kv_get(CHEM[chem]["bottle_l"]))
    with db() as c:
        c.execute("INSERT INTO bottles(chem,fitted_at,baseline,factor,size_l) "
                  "VALUES(?,?,?,?,?)", (chem, fitted, odo, factor, size))
    kv_set(CHEM[chem]["nwarn"], 0)
    kv_set(CHEM[chem]["nfinal"], 0)
    if chem == "cl":
        kv_set("baseline_total_time", odo)
        kv_set("debit_at_baseline", debit)
        kv_set("bottle_fitted_at", fitted)
    try:
        with _lock:
            poll_once()
    except Exception:
        pass


def edit_bottle(bid, at_ms=None, size_l=None):
    """Change a bottle's fitted date (recomputing its baseline) and/or size."""
    with db() as c:
        row = c.execute("SELECT * FROM bottles WHERE id=?", (bid,)).fetchone()
        if not row:
            return False
        chem = row["chem"]
    updates = {}
    if at_ms is not None:
        odo, debit, fitted = _odo_at(chem, at_ms)
        if fitted is not None:
            updates["fitted_at"] = fitted
            if odo is not None:
                updates["baseline"] = odo
                updates["factor"] = _factor_for(chem, debit)
    if size_l is not None:
        updates["size_l"] = float(size_l)
    if updates:
        sets = ",".join(f"{k}=?" for k in updates)
        with db() as c:
            c.execute(f"UPDATE bottles SET {sets} WHERE id=?", (*updates.values(), bid))
    if chem == "cl":
        _resync_cl_legacy()
    try:
        with _lock:
            poll_once()
    except Exception:
        pass
    return True


def delete_bottle(bid):
    with db() as c:
        row = c.execute("SELECT chem FROM bottles WHERE id=?", (bid,)).fetchone()
        c.execute("DELETE FROM bottles WHERE id=?", (bid,))
    if row and row["chem"] == "cl":
        _resync_cl_legacy()
    try:
        with _lock:
            poll_once()
    except Exception:
        pass
    return True


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
            q = parse_qs(urlparse(self.path).query)
            chem = q.get("chem", ["cl"])[0]
            self._json(usage_payload(chem))
        elif path == "/api/bottles":
            q = parse_qs(urlparse(self.path).query)
            chem = q.get("chem", ["cl"])[0]
            self._json(bottle_history(chem if chem in CHEM else "cl"))
        elif path == "/api/filter-usage":
            self._json(filter_usage_payload())
        elif path == "/api/lab-latest":
            self._json(lab_latest_payload())
        elif path == "/api/lab-history":
            q = parse_qs(urlparse(self.path).query)
            self._json(lab_history_payload(q.get("param", ["ph"])[0]))
        elif path == "/api/lab-raw":
            self._json(lab_raw_payload())
        elif path == "/api/lab-correlation":
            q = parse_qs(urlparse(self.path).query)
            self._json(lab_correlation_payload(q.get("param", ["fc"])[0],
                                               q.get("probe", ["orp"])[0]))
        elif path == "/api/raw":
            self._json(raw_payload())
        elif path == "/api/diag-usage":
            q = parse_qs(urlparse(self.path).query)
            self._json(diag_usage_payload(q.get("chem", ["cl"])[0]))
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
                chem = form.get("chem", "cl")
                chem = chem if chem in CHEM else "cl"
                size = form.get("size_l")
                do_new_bottle(chem, int(at) if at else None,
                              float(size) if size else None)
                self._json({"ok": True, "message":
                            f"New {CHEM[chem]['name']} bottle recorded"
                            + (" (backdated)." if at else ".")})
            elif path == "/api/bottle-edit":
                bid = int(form["id"])
                at = form.get("at_ms")
                size = form.get("size_l")
                ok = edit_bottle(bid, int(at) if at else None,
                                 float(size) if size else None)
                self._json({"ok": bool(ok),
                            "message": "Bottle updated." if ok else "Bottle not found."})
            elif path == "/api/bottle-delete":
                bid = int(form["id"])
                delete_bottle(bid)
                self._json({"ok": True, "message": "Bottle removed."})
            elif path == "/config":
                if form.get("bottle_l"): kv_set("bottle_l", float(form["bottle_l"]))
                if form.get("warn"): kv_set("warn_remaining_l", max(0.0, float(form["warn"])))
                if form.get("final"): kv_set("final_remaining_l", max(0.0, float(form["final"])))
                if form.get("ph_bottle_l"): kv_set("ph_bottle_l", float(form["ph_bottle_l"]))
                if form.get("ph_warn"): kv_set("ph_warn_remaining_l", max(0.0, float(form["ph_warn"])))
                if form.get("ph_final"): kv_set("ph_final_remaining_l", max(0.0, float(form["ph_final"])))
                if form.get("ph_pump_lph"): kv_set("ph_pump_lph", max(0.0, float(form["ph_pump_lph"])))
                if form.get("labcom_poll_hours"): kv_set("labcom_poll_hours", max(0.5, float(form["labcom_poll_hours"])))
                if form.get("bottle_avg_days"): kv_set("bottle_avg_days", max(1, int(float(form["bottle_avg_days"]))))
                for _k, _lbl in LAB_TARGET_FIELDS:
                    _lo, _hi = form.get("lab_lo_" + _k), form.get("lab_hi_" + _k)
                    if _lo not in (None, "") and _hi not in (None, ""):
                        try:
                            a, b = float(_lo), float(_hi)
                            if a < b:
                                kv_set("lab_range_" + _k, [a, b])
                        except ValueError:
                            pass
                    _cad = form.get("lab_cad_" + _k)
                    if _cad not in (None, ""):
                        try:
                            kv_set("lab_cadence_" + _k, max(1.0, float(_cad)))
                        except ValueError:
                            pass
                for _k in ("ph", "orp", "temp"):
                    _lo, _hi = form.get("live_lo_" + _k), form.get("live_hi_" + _k)
                    if _lo not in (None, "") and _hi not in (None, ""):
                        try:
                            a, b = float(_lo), float(_hi)
                            if a < b:
                                kv_set("live_range_" + _k, [a, b])
                        except ValueError:
                            pass
                if form.get("poll"): kv_set("poll_minutes", max(1.0, float(form["poll"])))
                if form.get("gpl"): kv_set("liquid_cl_gpl", max(1.0, float(form["gpl"])))
                if form.get("pump"): kv_set("pump_kw", max(0.0, float(form["pump"])))
                if form.get("price"): kv_set("price_kwh", max(0.0, float(form["price"])))
                if form.get("price_off"): kv_set("price_offpeak", max(0.0, float(form["price_off"])))
                if form.get("offwin") is not None: kv_set("offpeak_window", form["offwin"].strip())
                if form.get("currency"): kv_set("currency", form["currency"][:4])
                for key in CFG_KEYS:
                    if key in form and form[key].strip() != "":
                        cfg_set(key, form[key].strip())
                self._json({"ok": True, "message": "Settings saved."})
            elif path == "/poll-now":
                with _lock:
                    poll_once()
                self._json({"ok": True, "message": "Updated from the controller."})
            elif path == "/lab-refresh":
                try:
                    n = labcom_poll_once(force=True)
                    if n is None:
                        msg = "No LabCom token set (add it in Settings)."
                        ok = False
                    else:
                        msg = f"LabCom synced - {n} new test(s)." if n else "LabCom synced - no new tests."
                        ok = True
                    self._json({"ok": ok, "message": msg})
                except Exception as e:
                    kv_set("lab_last_error", f"{now_iso()}: {e}")
                    self._json({"ok": False, "message": f"LabCom sync failed: {e}"})
            elif path == "/api/lab-manual":
                n = add_manual_test(form)
                self._json({"ok": bool(n), "message": (f"Saved {n} manual reading(s)." if n
                            else "Nothing saved - enter at least one value.")})
            elif path == "/api/lab-manual-delete":
                try:
                    n = delete_manual_test(int(form.get("id", "0")))
                except (TypeError, ValueError):
                    n = 0
                self._json({"ok": bool(n), "message": ("Reading deleted." if n
                            else "Could not delete (manual readings only).")})
            elif path == "/api/filter-control":
                action = form.get("action", "")
                try:
                    with _lock:
                        st = set_filtration(action)
                    label = {"on": "Start", "off": "Stop", "auto": "Regulated"}.get(action, action)
                    if st == 9:
                        msg = f"Filtration -> {label}: done."
                    elif st is None:
                        msg = f"Filtration -> {label}: sent (result pending)."
                    else:
                        msg = f"Filtration -> {label}: {COMMAND_NAMES.get(st, 'code ' + str(st))}."
                    self._json({"ok": st in (9, None), "message": msg})
                except Exception as e:
                    self._json({"ok": False, "message": f"Filtration failed: {e}"})
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
