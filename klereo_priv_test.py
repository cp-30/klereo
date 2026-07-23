#!/usr/bin/env python3
"""
Klereo privilege test  --  is the "suspend/resume treatment" check server-side?
==============================================================================
This sends ONE privileged command to YOUR OWN pool and reports exactly how the
server responds, to answer a single question:

    Does Klereo enforce the installer-privilege requirement on the server,
    or does the app merely hide the button from a standard (PRIV_USER) login?

The command used is the app's own treatment-suspend mechanism:
    POST php/TraitHold.php {poolID, delay, comMode:1}
    delay = 0            -> "SuspOff": cancel any hold / resume treatment   (SAFE DEFAULT)
    delay = 12/24/36/48  -> suspend treatment for that many HOURS

DEFAULT behaviour (no --delay) sends delay=0. If your treatment is not currently
suspended, delay=0 is a physical no-op, yet it still exercises the exact
privileged endpoint. So:
    * If the server rejects it (error / COMMAND_BADACCESS 13) -> privilege is
      enforced server-side. That is the server correctly saying no; we stop.
    * If the server accepts it (status ok -> cmdID -> COMMAND_DONE 9) -> the
      check was UI-only and you can control treatment with your own token.

It asks you to type 'yes' before sending, prints the raw responses verbatim,
polls the command result a few times (no aggressive looping), and reads the
pool state before and after so you can see nothing got stuck.

Usage
-----
    export KLEREO_LOGIN='your-login'
    export KLEREO_PASSWORD='your-password'
    # optional: export KLEREO_POOL_ID='156682'

    python3 klereo_priv_test.py               # safe: sends delay=0 (resume)
    python3 klereo_priv_test.py --delay 12    # actually suspend 12h (asks to confirm)
    python3 klereo_priv_test.py --resume      # same as delay 0 (cancel any hold)
    python3 klereo_priv_test.py --yes         # skip the interactive confirmation
"""

import os
import sys
import time
import json
import hashlib
import argparse

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run:  pip3 install requests")

BASE_URL = "https://connect.klereo.fr/"
APP, VERSION, LANG, TIMEOUT = "Web", "3-W", "en", 30

COMMAND = {
    0: "COMMAND_WAIT (queued)",
    1: "COMMAND_SENT (sent to pod)",
    9: "COMMAND_DONE (success)",
    10: "COMMAND_ERROR (generic error)",
    11: "COMMAND_BADPARAM (bad parameter)",
    12: "COMMAND_UNKNOWN (unknown command)",
    13: "COMMAND_BADACCESS (insufficient privilege)",
    15: "COMMAND_TIMEOUT (pod did not answer)",
    16: "COMMAND_ABORT (aborted)",
    17: "COMMAND_NOTCONNECTED (pod offline)",
    18: "COMMAND_NOSERVICE (service unavailable)",
    19: "COMMAND_UPDATEREQ (firmware update required)",
}


class KlereoError(Exception):
    pass


class Klereo:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Mozilla/5.0 (KlereoPrivTest)",
                               "X-Requested-With": "XMLHttpRequest"})
        self.token = None

    def _post(self, path, data=None, auth=True):
        headers = {}
        if auth:
            headers["Authorization"] = "Bearer " + self.token
        r = self.s.post(BASE_URL + path, data=data or {}, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        h = r.headers.get("authorization")
        if h and h.lower().startswith("bearer "):
            self.token = h[7:]
        try:
            return r.json()
        except ValueError:
            raise KlereoError(f"{path}: non-JSON: {r.text[:200]!r}")

    def login(self, login, password):
        b = self._post("php/GetJWT.php",
                       {"login": login, "password": hashlib.sha1(password.encode()).hexdigest(),
                        "version": VERSION, "app": APP}, auth=False)
        if b.get("status") != "ok":
            raise KlereoError(f"login failed: {b}")
        if not self.token:
            raise KlereoError("login ok but no JWT header")
        return b

    def pool(self, pool_id):
        b = self._post("php/GetPoolDetails.php", {"poolID": pool_id, "lang": LANG})
        r = b.get("response")
        return r[0] if isinstance(r, list) and r else r

    def pool_ids(self):
        b = self._post("php/GetIndex.php", {"S": "", "max": 100, "start": 0})
        r = b.get("response") or []
        return [it.get("idSystem") for it in r if isinstance(it, dict) and it.get("idSystem")]


def snapshot(pool):
    """Print state relevant to a treatment hold."""
    if not isinstance(pool, dict):
        print("   (could not read pool state)")
        return
    print(f"   pool '{pool.get('poolNickname')}'  suspended={pool.get('suspended')}")
    # surface any hold/suspend-ish fields for visibility
    for bag in ("params", "ExtraParams"):
        d = pool.get(bag) or {}
        hits = {k: v for k, v in d.items()
                if any(w in k.lower() for w in ("hold", "susp", "hyb", "trait"))}
        if hits:
            print(f"   {bag}: " + ", ".join(f"{k}={v}" for k, v in sorted(hits.items())))


def main():
    ap = argparse.ArgumentParser(description="Klereo server-side privilege test (TraitHold)")
    ap.add_argument("--delay", type=int, default=0,
                    help="hours to suspend treatment (0 = resume/cancel hold; default 0)")
    ap.add_argument("--resume", action="store_true", help="alias for --delay 0")
    ap.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = ap.parse_args()
    delay = 0 if args.resume else args.delay

    login = os.environ.get("KLEREO_LOGIN") or input("Klereo login: ").strip()
    import getpass
    password = os.environ.get("KLEREO_PASSWORD") or getpass.getpass("Klereo password: ")

    k = Klereo()
    print("Authenticating ...")
    info = k.login(login, password)
    print(f"  OK. access={info.get('access')}  (10=USER 16=ADVUSER 20=PRO 30=SAV 40=BE)")

    pool_id = os.environ.get("KLEREO_POOL_ID")
    if not pool_id:
        ids = k.pool_ids()
        if len(ids) != 1:
            sys.exit(f"Set KLEREO_POOL_ID (found pools: {ids})")
        pool_id = ids[0]

    print("\nPool state BEFORE:")
    snapshot(k.pool(pool_id))

    action = ("RESUME treatment / cancel any hold (delay=0, no-op if not suspended)"
              if delay == 0 else
              f"SUSPEND treatment for {delay} HOURS")
    print("\n" + "=" * 64)
    print("About to send ONE command to pool", pool_id)
    print("  POST php/TraitHold.php  {poolID:%s, delay:%d, comMode:1}" % (pool_id, delay))
    print("  Meaning:", action)
    if delay != 0:
        print("  !! This really pauses disinfection. To undo, run:")
        print("       python3 klereo_priv_test.py --resume")
    print("=" * 64)
    if not args.yes:
        if input("Type 'yes' to send: ").strip().lower() != "yes":
            sys.exit("Aborted. Nothing sent.")

    print("\nSending ...")
    resp = k._post("php/TraitHold.php", {"poolID": pool_id, "delay": delay, "comMode": 1})
    print("RAW initial response:")
    print("  " + json.dumps(resp, ensure_ascii=False))

    status = resp.get("status")
    if status == "error":
        print("\nRESULT: server REJECTED the request at the API layer.")
        print("  detail:", resp.get("detail"))
        print("  -> Privilege appears ENFORCED SERVER-SIDE. This is the server saying no.")
        return
    if status != "ok":
        print(f"\nRESULT: unexpected status {status!r}. See raw above.")
        return

    # Accepted -> we should have a cmdID to poll.
    r = resp.get("response")
    cmd_id = None
    if isinstance(r, list) and r and isinstance(r[0], dict):
        cmd_id = r[0].get("cmdID")
    print(f"  Accepted. cmdID = {cmd_id}")

    if cmd_id is not None:
        print("\nPolling WaitCommand.php (a few times) ...")
        final = None
        for _ in range(8):
            time.sleep(2)
            w = k._post("php/WaitCommand.php", {"cmdID": cmd_id})
            st = (w.get("response") or {}).get("status") if isinstance(w.get("response"), dict) else None
            print(f"  status={st}  ({COMMAND.get(st, 'unknown')})")
            if st in (9, 10, 11, 12, 13, 15, 16, 17, 18, 19):
                final = st
                break
        print("\nRESULT:")
        if final == 9:
            print("  COMMAND_DONE -> the command was ACCEPTED and EXECUTED with your own token.")
            print("  -> The installer-privilege check for this action is UI-ONLY.")
            print("     You can suspend/resume treatment via the API.")
        elif final == 13:
            print("  COMMAND_BADACCESS -> privilege ENFORCED SERVER-SIDE (server said no).")
        elif final == 17:
            print("  COMMAND_NOTCONNECTED -> pod offline right now; retry later.")
        elif final is not None:
            print(f"  {COMMAND.get(final)} -> see meaning above.")
        else:
            print("  Still pending after polling; check the app, then re-run.")

    print("\nPool state AFTER:")
    snapshot(k.pool(pool_id))
    if delay != 0:
        print("\nReminder: to resume treatment now, run:")
        print("   python3 klereo_priv_test.py --resume")


if __name__ == "__main__":
    try:
        main()
    except KlereoError as e:
        sys.exit(f"Error: {e}")
    except requests.RequestException as e:
        sys.exit(f"Network error: {e}")
