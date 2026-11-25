"""Agent 2, job 2: refunds with no return (step 1: read the refunds).

Reads refund transactions from Amazon's Finances data (read-only) into the database, so each refund can be
checked against the returns report and the reimbursements report. This step creates no Asana tasks.
"""
from __future__ import annotations

import json
import os
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests
from fastapi import APIRouter, Header, HTTPException

import ledger

router = APIRouter(prefix="/agents/refunds", tags=["agent: refunds"])
LOOKBACK_DAYS = int(os.environ.get("REFUND_LOOKBACK_DAYS", "150"))
WINDOW_DAYS = int(os.environ.get("REFUND_WINDOW_DAYS", "10"))
MAX_PAGES = int(os.environ.get("REFUND_MAX_PAGES", "1500"))
RUN_MINUTES = int(os.environ.get("REFUND_RUN_MINUTES", "90"))
_run_lock = threading.Lock()


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS refund_events (
                transaction_id text PRIMARY KEY, posted_at timestamptz, order_id text, transaction_type text,
                description text, status text, total_amount numeric(14,2), skus text, units int, raw jsonb,
                loaded_at timestamptz NOT NULL DEFAULT now());
            CREATE INDEX IF NOT EXISTS refund_events_order ON refund_events (order_id);
            CREATE TABLE IF NOT EXISTS refund_windows (
                start_at timestamptz PRIMARY KEY, end_at timestamptz, transactions int, refunds int,
                loaded_at timestamptz NOT NULL DEFAULT now());
            CREATE TABLE IF NOT EXISTS refund_agent_runs (
                id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
                status text NOT NULL, summary jsonb, error text);
        """)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON refund_events, refund_windows, refund_agent_runs TO ledger_reader")


def _get(token: str, params: dict) -> dict:
    import main
    for attempt in range(6):
        r = requests.get(f"{main.SPAPI_ENDPOINT}/finances/2024-06-19/transactions",
                         headers={"x-amz-access-token": token}, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(3 + attempt * 3)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"Amazon {r.status_code}: {r.text[:300]}")
        return r.json().get("payload") or r.json()
    raise RuntimeError("Amazon kept answering 429 (too many requests)")


def is_refund(t: dict) -> bool:
    text = f"{t.get('transactionType', '')} {t.get('description', '')}".lower()
    return "refund" in text


def flatten(t: dict) -> dict:
    """Pure: the fields we keep from one Finances transaction."""
    order_id = next((r.get("relatedIdentifierValue") for r in t.get("relatedIdentifiers") or []
                     if r.get("relatedIdentifierName") == "ORDER_ID"), None)
    skus, units = [], 0
    for item in t.get("items") or []:
        for c in item.get("contexts") or []:
            if c.get("sku"):
                skus.append(c["sku"])
            try:
                units += int(c.get("quantityShipped") or 0)
            except (TypeError, ValueError):
                pass
    amount = (t.get("totalAmount") or {}).get("currencyAmount")
    return {"transaction_id": t.get("transactionId"), "posted_at": t.get("postedDate"), "order_id": order_id,
            "transaction_type": t.get("transactionType"), "description": t.get("description"),
            "status": t.get("transactionStatus"), "total_amount": amount,
            "skus": ", ".join(dict.fromkeys(skus)) or None, "units": units or None}


def run() -> dict:
    import main
    conn = ledger._db()
    ensure_tables(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE refund_agent_runs SET status='interrupted', finished_at=now() WHERE status='running'")
        cur.execute("INSERT INTO refund_agent_runs (status) VALUES ('running') RETURNING id")
        run_id = cur.fetchone()[0]
    summary = {"lookback_days": LOOKBACK_DAYS, "windows_read": 0, "transactions": 0, "refunds": 0, "pages": 0}

    def progress(step: str):
        with conn.cursor() as cur:
            cur.execute("UPDATE refund_agent_runs SET summary=%s WHERE id=%s",
                        (json.dumps({**summary, "step": step}, default=str), run_id))
    try:
        token, token_at = main.lwa_token(), time.time()
        now = datetime.now(timezone.utc) - timedelta(minutes=10)
        day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        with conn.cursor() as cur:   # closed windows (ended over 3 days ago) are final and never read again
            cur.execute("SELECT start_at FROM refund_windows WHERE end_at < now() - interval '3 days'")
            done = {r[0] for r in cur.fetchall()}
        deadline = time.time() + RUN_MINUTES * 60
        samples, types = [], {}
        start = day0 - timedelta(days=LOOKBACK_DAYS)
        while start < now and time.time() < deadline and summary["pages"] < MAX_PAGES:
            end = min(start + timedelta(days=WINDOW_DAYS), now)
            if start in done:
                start = end
                continue
            if time.time() - token_at > 45 * 60:
                token, token_at = main.lwa_token(), time.time()
            params = {"postedAfter": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "postedBefore": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                      "marketplaceId": main.MARKETPLACE_ID}
            n_tx = n_ref = 0
            while True:
                p = _get(token, params)
                summary["pages"] += 1
                rows = []
                for t in p.get("transactions") or []:
                    n_tx += 1
                    key = f"{t.get('transactionType')} / {t.get('description')}"
                    types[key] = types.get(key, 0) + 1
                    if not is_refund(t):
                        continue
                    n_ref += 1
                    f = flatten(t)
                    if len(samples) < 3:
                        samples.append(t)
                    if f["transaction_id"]:
                        rows.append((f["transaction_id"], f["posted_at"], f["order_id"], f["transaction_type"],
                                     f["description"], f["status"], f["total_amount"], f["skus"], f["units"], json.dumps(t)))
                if rows:
                    with conn.cursor() as cur:
                        cur.executemany("""INSERT INTO refund_events (transaction_id, posted_at, order_id, transaction_type,
                                             description, status, total_amount, skus, units, raw)
                                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                           ON CONFLICT (transaction_id) DO UPDATE SET status=EXCLUDED.status,
                                             total_amount=EXCLUDED.total_amount, raw=EXCLUDED.raw, loaded_at=now()""", rows)
                if summary["pages"] % 5 == 0:
                    progress(f"reading refunds: {start:%b %-d} to {end:%b %-d}, page {summary['pages']}")
                nxt = p.get("nextToken")
                if not nxt or summary["pages"] >= MAX_PAGES:
                    break
                params = {"postedAfter": params["postedAfter"], "postedBefore": params["postedBefore"],
                          "marketplaceId": main.MARKETPLACE_ID, "nextToken": nxt}
                time.sleep(2.1)
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO refund_windows (start_at, end_at, transactions, refunds) VALUES (%s,%s,%s,%s)
                               ON CONFLICT (start_at) DO UPDATE SET end_at=EXCLUDED.end_at, transactions=EXCLUDED.transactions,
                                 refunds=EXCLUDED.refunds, loaded_at=now()""", (start, end, n_tx, n_ref))
            summary["windows_read"] += 1
            summary["transactions"] += n_tx
            summary["refunds"] += n_ref
            start = end
            time.sleep(2.1)
        summary["reached"] = str(start)[:16]
        summary["complete"] = start >= now
        summary["types_seen"] = dict(sorted(types.items(), key=lambda kv: -kv[1])[:25])
        summary["sample_refunds"] = samples
        with conn.cursor() as cur:
            cur.execute("UPDATE refund_agent_runs SET status='ok', finished_at=now(), summary=%s WHERE id=%s",
                        (json.dumps(summary, default=str), run_id))
        return {"run_id": run_id, **{k: v for k, v in summary.items() if k != "sample_refunds"}}
    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute("UPDATE refund_agent_runs SET status='error', finished_at=now(), summary=%s, error=%s WHERE id=%s",
                        (json.dumps(summary, default=str), f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1000:]}",
                         run_id))
        raise
    finally:
        conn.close()


def _background():
    try:
        run()
    except Exception as exc:
        print(f"refunds agent failed: {exc}")
    finally:
        _run_lock.release()


@router.post("/run")
def trigger(x_sync_token: str = Header("")):
    if not ledger.LEDGER_SYNC_TOKEN or x_sync_token != ledger.LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    if not _run_lock.acquire(blocking=False):
        return {"started": False, "reason": "the refunds agent is already running"}
    threading.Thread(target=_background, daemon=True).start()
    return {"started": True}
