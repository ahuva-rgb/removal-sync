"""Run by the Render cron job: asks the removal-sync service to run its daily syncs.

1. Inventory ledger            (POST /ledger/sync)
2. Reimbursements + returns    (POST /claims/sync)   - skipped when CLAIMS_SYNC=0

Env vars: REMOVAL_SYNC_URL, LEDGER_SYNC_TOKEN, optional LEDGER_MODE / CLAIMS_MODE
(auto|daily|backfill), optional CLAIMS_SYNC (0 to skip).
"""
import os
import sys

import requests

url = os.environ.get("REMOVAL_SYNC_URL", "https://removal-sync.onrender.com").rstrip("/")
headers = {"X-Sync-Token": os.environ["LEDGER_SYNC_TOKEN"]}
ok = True

r = requests.post(f"{url}/ledger/sync", params={"mode": os.environ.get("LEDGER_MODE", "auto")},
                  headers=headers, timeout=120)
print("ledger:", r.status_code, r.text)
ok = ok and r.ok

if os.environ.get("CLAIMS_SYNC", "1") != "0":
    try:
        r = requests.post(f"{url}/claims/sync", params={"mode": os.environ.get("CLAIMS_MODE", "auto")},
                          headers=headers, timeout=120)
        print("claims:", r.status_code, r.text)
        ok = ok and r.ok
    except requests.RequestException as exc:  # never let this hide the ledger result
        print("claims: request failed:", exc)
        ok = False

sys.exit(0 if ok else 1)
