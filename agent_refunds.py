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


# ------------------------------------------------------------- watch list ---
WAIT_DAYS = int(os.environ.get("REFUND_WAIT_DAYS", "60"))        # a refund becomes a candidate this long after it was issued
FULL_SHARE = 0.98                                                # refunded product amount / price paid, to count as a full refund
MAX_ORDER_LOOKUPS = int(os.environ.get("REFUND_MAX_ORDER_LOOKUPS", "150"))


def product_amount(t: dict, sku: str | None = None) -> float:
    """Pure: the product (principal) amount in one transaction, for one SKU or for all items. Refunds are negative."""
    total = 0.0
    for item in t.get("items") or []:
        skus = [c.get("sku") for c in item.get("contexts") or [] if c.get("sku")]
        if sku and sku not in skus:
            continue
        for b in item.get("breakdowns") or []:
            if b.get("breakdownType") == "ProductCharges":
                total += float((b.get("breakdownAmount") or {}).get("currencyAmount") or 0)
    return round(total, 2)


def classify_refund(refunded: float, paid: float | None) -> str:
    """Pure. Rule from Cassie: anything that is not a full refund was issued by us and cannot be filed."""
    if refunded >= -0.005:
        return "not_a_product_refund"            # shipping or tax only
    if paid is None or paid <= 0:
        return "price_unknown"
    return "full_refund" if -refunded >= paid * FULL_SHARE else "partial_refund"


def build_watch_list(conn, token: str, progress) -> dict:
    """Full product refunds older than WAIT_DAYS with no return and no reimbursement. Creates nothing."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS refund_watch (
                order_id text NOT NULL, sku text NOT NULL, refund_date date, refunded numeric(14,2), paid numeric(14,2),
                units int, kind text, returned boolean, reimbursed boolean, verdict text, checked_at timestamptz DEFAULT now(),
                asana_task_gid text, PRIMARY KEY (order_id, sku))""")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON refund_watch TO ledger_reader")
        cur.execute("""
            SELECT e.order_id, e.skus, min(e.posted_at)::date, coalesce(max(e.units), 1), jsonb_agg(e.raw)
            FROM refund_events e
            WHERE e.transaction_type = 'Refund' AND e.order_id IS NOT NULL AND e.skus IS NOT NULL
              AND e.posted_at < now() - make_interval(days => %s)
              AND NOT EXISTS (SELECT 1 FROM fba_customer_returns c WHERE c.order_id = e.order_id)
              AND NOT EXISTS (SELECT 1 FROM fba_reimbursements f WHERE f.amazon_order_id = e.order_id)
              AND NOT EXISTS (SELECT 1 FROM refund_watch w WHERE w.order_id = e.order_id AND w.sku = e.skus
                                AND w.kind IN ('partial_refund', 'not_a_product_refund'))
            GROUP BY 1, 2""", (WAIT_DAYS,))
        rows = cur.fetchall()
    counts, lookups, listed = {}, 0, []
    for order_id, sku, day, units, raws in rows:
        refunded = round(sum(product_amount(r, sku.split(", ")[0]) for r in raws), 2)
        paid = None
        if refunded < -0.005:
            if lookups >= MAX_ORDER_LOOKUPS:
                counts["left_for_next_run"] = counts.get("left_for_next_run", 0) + 1
                continue
            lookups += 1
            if lookups % 10 == 1:
                progress(f"checking refunds against the price paid: {lookups} orders looked up")
            try:
                import main
                p = _get(token, {"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": order_id,
                                 "marketplaceId": main.MARKETPLACE_ID,
                                 "postedAfter": (datetime.now(timezone.utc) - timedelta(days=540)).strftime("%Y-%m-%dT%H:%M:%SZ")})
                paid = round(sum(product_amount(t, sku.split(", ")[0]) for t in p.get("transactions") or []
                                 if not is_refund(t) and product_amount(t, sku.split(", ")[0]) > 0), 2) or None
                time.sleep(2.1)
            except Exception as exc:
                counts["lookup_errors"] = counts.get("lookup_errors", 0) + 1
                counts.setdefault("first_lookup_error", str(exc)[:160])
        kind = classify_refund(refunded, paid)
        verdict = "candidate" if kind == "full_refund" else ("check_by_hand" if kind == "price_unknown" else "not_fileable")
        counts[kind] = counts.get(kind, 0) + 1
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO refund_watch (order_id, sku, refund_date, refunded, paid, units, kind, returned, reimbursed, verdict)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,false,false,%s)
                           ON CONFLICT (order_id, sku) DO UPDATE SET refunded=EXCLUDED.refunded, paid=coalesce(EXCLUDED.paid, refund_watch.paid),
                             kind=EXCLUDED.kind, verdict=EXCLUDED.verdict, checked_at=now()""",
                        (order_id, sku, day, refunded, paid, units, kind, verdict))
        if verdict == "candidate":
            listed.append({"order": order_id, "sku": sku, "refunded_on": str(day), "refunded": refunded, "paid": paid})
    with conn.cursor() as cur:   # anything on the list that has since come back or been paid is settled
        cur.execute("""UPDATE refund_watch w SET verdict='settled', checked_at=now(),
                         returned = EXISTS (SELECT 1 FROM fba_customer_returns c WHERE c.order_id = w.order_id),
                         reimbursed = EXISTS (SELECT 1 FROM fba_reimbursements f WHERE f.amazon_order_id = w.order_id)
                       WHERE w.verdict IN ('candidate', 'check_by_hand')
                         AND (EXISTS (SELECT 1 FROM fba_customer_returns c WHERE c.order_id = w.order_id)
                              OR EXISTS (SELECT 1 FROM fba_reimbursements f WHERE f.amazon_order_id = w.order_id))""")
    return {"refunds_checked": len(rows), "counts": counts, "candidates": len(listed),
            "candidate_amount": round(-sum(x["refunded"] for x in listed), 2), "sample": listed[:15]}


# ------------------------------------------------------------ weekly task ---
FILE_BOARD = os.environ.get("REFUND_TASK_PROJECT", "1220620863326159")          # Reimbursements to File
FILE_SECTION_NAME = "To file: Refunds with no return (weekly)"
CALVIN = os.environ.get("REFUND_TASK_ASSIGNEE", "1208840834774221")
TASK_AFTER_DAYS = int(os.environ.get("REFUND_TASK_AFTER_DAYS", "75"))           # Amazon pays by itself until about here
CLAIM_WINDOW_DAYS = 120
SKIP_SKU = [x.strip().upper() for x in os.environ.get("REFUND_SKIP_SKU", "ROP").split(",") if x.strip()]


def tasks_live() -> bool:
    return os.environ.get("REFUND_TASK_LIVE") == "1"


def weekly_task_fields(rows: list[dict], today) -> dict:
    """Pure: one task for Calvin listing this week's refunds. rows: order, sku, refund_date, refunded, units."""
    lines = [f"{r['order']} | {r['sku']} | {r['refund_date']:%b %-d, %Y} | ${-float(r['refunded']):,.2f}" for r in rows]
    late = [r["order"] for r in rows if (today - r["refund_date"]).days > CLAIM_WINDOW_DAYS]
    total = -sum(float(r["refunded"]) for r in rows)
    wording = ["Hello,", "",
               "We are writing about the customer refunds on the FBA orders listed below. Each order was refunded in "
               f"full more than {TASK_AFTER_DAYS} days ago. The unit has not been returned to our inventory and we have "
               "not received a reimbursement.", "",
               "1. Please confirm for each order whether the refund was initiated by the seller.",
               "2. For every order where the refund was NOT seller-initiated, please reimburse us for the unit under the "
               "FBA customer returns policy.", "",
               "Order ID | SKU | Refund date | Amount refunded", *lines, "", "Thank you."]
    notes = ["STEPS",
             "1. Open ONE Amazon case for all the orders below (FBA > customer returns / refund with no return).",
             "2. Paste the case wording below.",
             "3. Put the Amazon case number in the Case ID field.",
             "4. When Amazon answers, write in a comment which orders were seller-initiated (nothing to claim) and "
             "which Amazon agreed to reimburse.", "",
             f"Orders: {len(rows)}    Total refunded: ${total:,.2f}",
             "These are full refunds only. Partial and shipping-only refunds were issued by us and are not listed.",
             *([f"Note: {len(late)} of these were refunded more than {CLAIM_WINDOW_DAYS} days ago, so Amazon may say the "
                "claim window has passed: " + ", ".join(late)] if late else []), "",
             "CASE WORDING (copy and paste)", *wording]
    return {"name": f"Refunds with no return | week of {today:%b %-d, %Y} | {len(rows)} order(s) | ${total:,.2f}",
            "notes": "\n".join(notes), "due_on": str(today + timedelta(days=1))}


def _asana(method: str, path: str, token: str, **kw):
    r = requests.request(method, f"https://app.asana.com/api/1.0{path}", headers={"Authorization": f"Bearer {token}"},
                         timeout=90, **kw)
    if not r.ok:
        raise RuntimeError(f"Asana {r.status_code}: {r.text[:300]}")
    return r.json()["data"]


def weekly_task(conn, asana_token: str, apply: bool, now: datetime | None = None) -> dict:
    """Once a week (Monday), one task for Calvin with the refunds Amazon has not returned or paid."""
    from zoneinfo import ZoneInfo
    today = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("America/New_York")).date()
    with conn.cursor() as cur:
        cur.execute("""SELECT order_id, sku, refund_date, refunded, units FROM refund_watch
                       WHERE verdict = 'candidate' AND asana_task_gid IS NULL AND refund_date <= %s
                       ORDER BY refund_date""", (today - timedelta(days=TASK_AFTER_DAYS),))
        rows = [dict(zip(["order", "sku", "refund_date", "refunded", "units"], r)) for r in cur.fetchall()]
    rows = [r for r in rows if not any(k in r["sku"].upper() for k in SKIP_SKU)]
    out = {"live": apply, "orders_waiting": len(rows), "is_monday": today.weekday() == 0}
    if not rows:
        return out
    # never list an order that already appears on the board
    seen_text, params = "", {"limit": 100, "opt_fields": "name,notes"}
    while True:
        r = requests.get(f"https://app.asana.com/api/1.0/projects/{FILE_BOARD}/tasks", params=params, timeout=60,
                         headers={"Authorization": f"Bearer {asana_token}"})
        r.raise_for_status()
        j = r.json()
        seen_text += " ".join(f"{t.get('name', '')} {t.get('notes', '')}" for t in j["data"])
        if not (j.get("next_page") or {}).get("offset"):
            break
        params["offset"] = j["next_page"]["offset"]
    already = [r for r in rows if r["order"] in seen_text]
    rows = [r for r in rows if r["order"] not in seen_text]
    f = weekly_task_fields(rows, today) if rows else None
    out.update(already_on_board=len(already), orders_for_task=len(rows), task=f["name"] if f else None,
               orders=[f"{r['order']} | {r['sku']} | {r['refund_date']} | {-float(r['refunded']):.2f}" for r in rows][:60])
    if not (apply and rows and today.weekday() == 0):
        return out
    secs = _asana("GET", f"/projects/{FILE_BOARD}/sections", asana_token)
    sec = next((x["gid"] for x in secs if x["name"].strip().lower().startswith("to file: refunds with no return")), None)
    if not sec:
        sec = _asana("POST", f"/projects/{FILE_BOARD}/sections", asana_token, json={"data": {"name": FILE_SECTION_NAME}})["gid"]
    data = {**f, "projects": [FILE_BOARD], "memberships": [{"project": FILE_BOARD, "section": sec}]}
    if CALVIN and CALVIN.lower() != "none":
        data["assignee"] = CALVIN
    gid = _asana("POST", "/tasks", asana_token, json={"data": data})["gid"]
    with conn.cursor() as cur:
        cur.executemany("UPDATE refund_watch SET asana_task_gid=%s WHERE order_id=%s AND sku=%s",
                        [(gid, r["order"], r["sku"]) for r in rows])
    out["task_created"] = gid
    return out


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
        if summary["complete"]:
            try:
                summary["watch_list"] = build_watch_list(conn, token, progress)
            except Exception as exc:
                summary["watch_list_error"] = f"{type(exc).__name__}: {exc}"[:300]
        else:
            summary["watch_list"] = "waiting: the refunds are still being read"
        if summary["complete"] and os.environ.get("ASANA_TOKEN"):
            try:
                summary["weekly_task"] = weekly_task(conn, os.environ["ASANA_TOKEN"], apply=tasks_live())
            except Exception as exc:
                summary["weekly_task_error"] = f"{type(exc).__name__}: {exc}"[:300]
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
