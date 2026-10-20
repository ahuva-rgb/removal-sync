"""Run by the Render cron job: asks the removal-sync service to run the ledger sync.

Env vars: REMOVAL_SYNC_URL, LEDGER_SYNC_TOKEN, optional LEDGER_MODE (daily|backfill).
"""
import os
import sys

import requests

url = os.environ.get("REMOVAL_SYNC_URL", "https://removal-sync.onrender.com").rstrip("/")
mode = os.environ.get("LEDGER_MODE", "daily")
r = requests.post(f"{url}/ledger/sync", params={"mode": mode},
                  headers={"X-Sync-Token": os.environ["LEDGER_SYNC_TOKEN"]}, timeout=120)
print(r.status_code, r.text)
sys.exit(0 if r.ok else 1)
