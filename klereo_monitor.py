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
            temp REAL, salt REAL, filtration INTEGER, used_l REAL)""")
    if kv_get("threshold_l")  is None: kv_set("threshold_l", DEF_THRESHOLD)
    if kv_get("bottle_l")     is None: kv_set("bottle_l", DEF_BOTTLE)
    if kv_get("notified")     is None: kv_set("notified", 0)
    if kv_get("poll_minutes") is None: kv_set("poll_minutes", POLL_MINUTES)


def kv_get(k, default=None):
    with db() as c:
        r = c.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return json.loads(r["v"]) if r else default


def kv_set(k, v):
    with db() as c:
        c.execute("INSERT INTO kv(k,v) VALUES(?,?) "
                  "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, json.dumps(v)))


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


# --------------------------------------------------------------------------
# Poll + alert
# --------------------------------------------------------------------------
def send_email(subject, body):
    host = os.environ.get("SMTP_HOST"); user = os.environ.get("SMTP_USER")
    pw = os.environ.get("SMTP_PASS"); to = os.environ.get("ALERT_TO")
    if not all([host, user, pw, to]):
        print("[email] not configured; skipping"); return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("ALERT_FROM", user)
    msg["To"] = to
    msg.set_content(body)
    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", "587")), timeout=HTTP_TIMEOUT) as s:
        s.starttls(); s.login(user, pw); s.send_message(msg)
    print(f"[email] sent to {to}")
    return True


def poll_once():
    login = os.environ.get("KLEREO_LOGIN"); password = os.environ.get("KLEREO_PASSWORD")
    if not login or not password:
        raise KlereoError("KLEREO_LOGIN / KLEREO_PASSWORD not set")
    k = Klereo(); k.login(login, password)
    pid = os.environ.get("KLEREO_POOL_ID")
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
        "filtration": out_status(pool, SCHED_FILTRE),
        "treatment": out_status(pool, SCHED_TRAIT),
        "used_l": used,
        "suspended": pool.get("suspended"),
    }
    with db() as c:
        c.execute("INSERT INTO readings VALUES (?,?,?,?,?,?,?,?,?)",
                  (reading["ts"], total, debit, reading["ph"], reading["orp"],
                   reading["temp"], reading["salt"], reading["filtration"], used))
    kv_set("last_reading", reading)
    kv_set("last_ok", now_iso())
    kv_set("last_error", None)

    threshold = float(kv_get("threshold_l", DEF_THRESHOLD))
    if used is not None and used >= threshold and not kv_get("notified"):
        bottle = float(kv_get("bottle_l", DEF_BOTTLE))
        body = (f"Your Klereo pool '{reading['nickname']}' has used {used:.1f} L of "
                f"liquid chlorine from the current bottle (threshold {threshold:.0f} L, "
                f"~{max(bottle-used,0):.1f} L left of a {bottle:.0f} L bottle).\n\n"
                f"Time to fit a new bottle. When you do, press 'New bottle fitted' on "
                f"the dashboard.\n")
        try:
            if send_email("Klereo: time to buy a new chlorine bottle", body):
                kv_set("notified", 1); kv_set("notified_at", now_iso())
        except Exception as e:
            print("[email] error:", e)
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
 "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")

PAGE = b"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="icon" href="/apple-touch-icon.png">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="Pool">
<meta name="theme-color" content="#0f172a">
<title>Pool</title>
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
 .bar{height:14px;background:#334155;border-radius:8px;overflow:hidden;margin-top:8px}
 .bar>div{height:100%;background:linear-gradient(90deg,#22c55e,#eab308,#ef4444)}
 .panel{background:#1e293b;border-radius:12px;padding:16px;margin-top:16px}
 button{background:#2563eb;color:#fff;border:0;border-radius:8px;padding:9px 14px;font-size:14px;cursor:pointer}
 button.ghost{background:#334155}
 input{background:#0f172a;border:1px solid #334155;color:#e2e8f0;border-radius:6px;padding:7px;width:90px}
 label{font-size:13px;color:#cbd5e1;margin-right:6px}
 .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:8px}
 .setrow{display:flex;justify-content:space-between;align-items:center;margin:10px 0}
 .setrow label{margin:0} .setrow input{width:70px;text-align:right} .setrow .u{color:#94a3b8;margin-left:6px}
 .err{background:#7f1d1d;color:#fecaca;padding:8px 12px;border-radius:8px;margin-top:12px;font-size:13px}
</style></head><body>
 <div id="ptr" style="position:fixed;top:0;left:0;right:0;text-align:center;padding:8px;color:#94a3b8;font-size:13px;transform:translateY(-40px);transition:transform .15s;z-index:6">&#8595; pull to refresh</div>
 <div class="wrap">
 <button id="refbtn" class="ghost" title="Refresh" style="position:fixed;top:10px;right:10px;z-index:5;font-size:18px;line-height:1;padding:8px 12px" onclick="refresh()">&#8635;</button>
 <h1 id="title">Klereo Monitor</h1>
 <div class="sub" id="sub">loading...</div>
 <div id="err"></div>
 <div class="grid">
  <div class="card"><div class="lbl">pH</div><div class="val" id="ph">-</div><div class="unit">target 6.6-8.0</div></div>
  <div class="card"><div class="lbl">ORP / Redox</div><div class="val" id="orp">-</div><div class="unit">mV</div></div>
  <div class="card"><div class="lbl">Water temp</div><div class="val" id="temp">-</div><div class="unit">&deg;C</div></div>
  <div class="card"><div class="lbl">Filtration</div><div class="val" id="filt">-</div></div>
  <div class="card"><div class="lbl">Dosed today</div><div class="val" id="today">-</div><div class="unit">mL liquid Cl</div></div>
 </div>
 <div class="panel">
  <div class="lbl" style="color:#94a3b8;font-size:12px;text-transform:uppercase">Liquid chlorine bottle</div>
  <div class="val" style="font-size:30px;font-weight:700"><span id="used">-</span> <span class="unit">L used</span>
     &nbsp; <span class="unit">/ <span id="rem">-</span> L left of <span id="bottle">-</span> L</span></div>
  <div class="bar"><div id="barfill" style="width:0%"></div></div>
  <div class="sub" id="bottleinfo" style="margin-top:10px"></div>
  <form method="post" action="/new-bottle" onsubmit="return confirm('Reset the baseline to now? Do this only when you have fitted a NEW bottle.')">
     <button type="submit">New bottle fitted (reset baseline)</button>
  </form>
 </div>
 <div class="panel"><canvas id="chart" height="120"></canvas></div>
 <div class="panel">
  <form method="post" action="/settings">
    <div class="setrow"><label>Bottle size</label><span><input name="bottle_l" id="bo" type="number" step="1"><span class="u">L</span></span></div>
    <div class="setrow"><label>Alert when remaining</label><span><input name="remaining_l" id="rem_in" type="number" step="0.5"><span class="u">L</span></span></div>
    <div class="setrow"><label>Check every</label><span><input name="poll_minutes" id="pm" type="number" step="1" min="1"><span class="u">min</span></span></div>
    <button type="submit" style="width:100%;margin-top:6px">Save settings</button>
  </form>
  <div class="row">
    <form method="post" action="/poll-now"><button class="ghost" type="submit">Poll now</button></form>
    <form method="post" action="/test-email"><button class="ghost" type="submit">Send test email</button></form>
  </div>
 </div>
</div>
<script>
async function load(){
 const s = await (await fetch('/api/status')).json();
 const r = s.reading || {};
 document.getElementById('title').textContent = 'Klereo Monitor - ' + (r.nickname||'');
 document.getElementById('sub').textContent =
    'last update: ' + (s.last_ok||'never') + '  |  polling every ' + s.poll_minutes + ' min'
    + (s.notified ? '  |  ALERT SENT for this bottle' : '');
 document.getElementById('err').innerHTML = s.last_error ? '<div class="err">Last error: '+s.last_error+'</div>' : '';
 const set=(id,v,d)=>document.getElementById(id).textContent=(v==null?'-':(typeof v==='number'?v.toFixed(d):v));
 set('ph', r.ph, 2); set('orp', r.orp, 0); set('temp', r.temp, 1); set('today', r.today_ml, 0);
 const f=document.getElementById('filt');
 if(r.filtration==null){f.textContent='-';f.className='val';}
 else{f.textContent=r.filtration? 'ON':'OFF'; f.className='val '+(r.filtration?'on':'off');}
 document.getElementById('bottle').textContent=(s.bottle_l==null?'-':s.bottle_l);
 document.getElementById('bo').value=s.bottle_l;
 document.getElementById('rem_in').value=(s.bottle_l - s.threshold_l).toFixed(1);
 document.getElementById('pm').value=s.poll_minutes;
 if(r.used_l!=null){
   const used=r.used_l, bottle=s.bottle_l||20, rem=Math.max(bottle-used,0);
   document.getElementById('used').textContent=used.toFixed(1);
   document.getElementById('rem').textContent=rem.toFixed(1);
   document.getElementById('barfill').style.width=Math.min(100,used/bottle*100)+'%';
   document.getElementById('bottleinfo').textContent=
      'fitted: '+(s.bottle_fitted_at||'-')+'   |   alert at '+(s.bottle_l - s.threshold_l).toFixed(1)+' L remaining';
 } else {
   document.getElementById('used').textContent='no baseline';
   document.getElementById('bottleinfo').textContent='Press "New bottle fitted" to start tracking this bottle.';
 }
 const h = await (await fetch('/api/history')).json();
 drawChart(h);
}
let chart;
function drawChart(h){
 const labels=h.map(x=>x.ts.replace('T',' ').slice(5,16));
 const ctx=document.getElementById('chart');
 const data={labels, datasets:[
   {label:'pH', data:h.map(x=>x.ph), yAxisID:'y1', borderColor:'#38bdf8', tension:.3, pointRadius:0},
   {label:'ORP (mV)', data:h.map(x=>x.orp), yAxisID:'y2', borderColor:'#a78bfa', tension:.3, pointRadius:0},
   {label:'Chlorine used (L)', data:h.map(x=>x.used_l), yAxisID:'y2', borderColor:'#f59e0b', tension:.3, pointRadius:0},
 ]};
 const opts={responsive:true, interaction:{mode:'index',intersect:false},
   scales:{y1:{position:'left',title:{display:true,text:'pH'},grid:{color:'#334155'}},
           y2:{position:'right',grid:{display:false}},
           x:{ticks:{maxTicksLimit:8,color:'#94a3b8'},grid:{color:'#1e293b'}}},
   plugins:{legend:{labels:{color:'#cbd5e1'}}}};
 if(chart) chart.destroy();
 chart=new Chart(ctx,{type:'line',data,options:opts});
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


def status_payload():
    return {
        "reading": kv_get("last_reading"),
        "last_ok": kv_get("last_ok"),
        "last_error": kv_get("last_error"),
        "threshold_l": kv_get("threshold_l", DEF_THRESHOLD),
        "bottle_l": kv_get("bottle_l", DEF_BOTTLE),
        "baseline_total_time": kv_get("baseline_total_time"),
        "bottle_fitted_at": kv_get("bottle_fitted_at"),
        "notified": kv_get("notified"),
        "poll_minutes": kv_get("poll_minutes", POLL_MINUTES),
    }


def history_payload():
    with db() as c:
        rows = c.execute("SELECT ts,ph,orp,temp,used_l FROM readings "
                         "ORDER BY ts DESC LIMIT 500").fetchall()
    return list(reversed([dict(r) for r in rows]))


def do_new_bottle():
    r = kv_get("last_reading") or {}
    total = r.get("total_time")
    if total is None:
        with _lock:
            r = poll_once()
        total = r.get("total_time")
    kv_set("baseline_total_time", total)
    kv_set("debit_at_baseline", r.get("debit"))
    kv_set("bottle_fitted_at", now_iso())
    kv_set("notified", 0)
    # Re-poll so the dashboard immediately shows ~0 L used against the new baseline
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
        elif path == "/api/status":
            self._json(status_payload())
        elif path == "/api/history":
            self._json(history_payload())
        else:
            self._send(404, "not found")

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", "0") or 0)
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
                do_new_bottle(); self._redirect("/")
            elif path == "/settings":
                bo = float(form["bottle_l"])
                rem = float(form["remaining_l"])       # alert when this many L remain
                kv_set("bottle_l", bo)
                kv_set("threshold_l", max(0.0, bo - rem))   # stored internally as L used
                if form.get("poll_minutes"):
                    kv_set("poll_minutes", max(1.0, float(form["poll_minutes"])))
                self._redirect("/")
            elif path == "/poll-now":
                with _lock:
                    poll_once()
                self._redirect("/")
            elif path == "/test-email":
                try:
                    ok = send_email("Klereo Monitor: test email",
                                    "This is a test from your Klereo Monitor. "
                                    "If you received it, email alerts are working.")
                    msg = ("Test email sent - check your inbox (and spam)." if ok
                           else "Email is NOT configured. Set SMTP_HOST / SMTP_USER / "
                                "SMTP_PASS / ALERT_TO in klereo.env, then restart.")
                except Exception as e:
                    msg = f"Email FAILED: {e}"
                self._send(200, "<!doctype html><meta charset=utf-8>"
                           "<body style='font-family:sans-serif;background:#0f172a;"
                           "color:#e2e8f0;padding:24px'>"
                           f"<p>{msg}</p><p><a style='color:#93c5fd' href='/'>&larr; "
                           "Back to dashboard</a></p>")
            else:
                self._send(404, "not found")
        except Exception as e:
            kv_set("last_error", f"{now_iso()}: {e}")
            self._send(500, f"error: {e}")


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
