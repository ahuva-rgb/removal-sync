"""Run by the Render cron job: asks the removal-sync service to run its daily syncs.

1. Inventory ledger            (POST /ledger/sync)
2. Reimbursements + returns    (POST /claims/sync)   - skipped when CLAIMS_SYNC=0
3. Payment accuracy agent      (POST /agents/payment-accuracy/run) - skipped when PAYMENT_AGENT=0.
   It waits for 1 and 2 to finish, then creates Asana tasks for underpaid reimbursements.

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

if os.environ.get("PAYMENT_AGENT", "1") != "0":
    try:
        r = requests.post(f"{url}/agents/payment-accuracy/run", headers=headers, timeout=120)
        print("payment agent:", r.status_code, r.text)
        ok = ok and r.ok
    except requests.RequestException as exc:
        print("payment agent: request failed:", exc)
        ok = False

if os.environ.get("SHIPMENT_AGENT", "1") != "0":
    try:   # test mode: lists short shipments, creates nothing. Its result never fails the cron run.
        r = requests.post(f"{url}/agents/shipments/run", headers=headers, timeout=120)
        print("shipments agent:", r.status_code, r.text)
    except requests.RequestException as exc:
        print("shipments agent: request failed:", exc)

if os.environ.get("REFUND_AGENT", "1") != "0":
    try:   # reads refunds only; creates nothing. Its result never fails the cron run.
        r = requests.post(f"{url}/agents/refunds/run", headers=headers, timeout=120)
        print("refunds agent:", r.status_code, r.text)
    except requests.RequestException as exc:
        print("refunds agent: request failed:", exc)

try:   # the Monday report: saves a preview every day, creates the task on Mondays when it is switched on
    r = requests.post(f"{url}/agents/report/run", headers=headers, timeout=120)
    print("weekly report:", r.status_code, r.text)
except requests.RequestException as exc:
    print("weekly report: request failed:", exc)

sys.exit(0 if ok else 1)
