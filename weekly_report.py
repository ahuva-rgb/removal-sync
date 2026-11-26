"""Safety net: the Monday report.

One Asana task for Cassie every Monday with what each agent did in the last 7 days, what is waiting on a person,
deadlines coming up, and the two "look at these by hand" lists (refunds and reversals).
Every day it also saves a preview, so the numbers can be read at any time. It creates the task only on Mondays
and only when WEEKLY_REPORT_LIVE=1.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from fastapi import APIRouter, Header, HTTPException

import ledger

router = APIRouter(prefix="/agents/report", tags=["agent: weekly report"])
BOARD = os.environ.get("REPORT_PROJECT", "1220818804510392")          # Reimbursement Case Discrepancy
SECTION_NAME = "Weekly report"
OWNER = os.environ.get("REPORT_ASSIGNEE", "1154119074861617")         # Cassie
TZ = ZoneInfo("America/New_York")
_lock = threading.Lock()

REVERSALS_SQL = """
WITH rv AS (SELECT r.reimbursement_id, r.sku, r.fnsku, r.approval_date rdate, -r.amount_total amt,
                   -coalesce(r.quantity_reimbursed_cash, 0) qty, coalesce(r.quantity_reimbursed_inventory, 0) qinv,
                   r.original_reimbursement_id oid, o.reason oreason, o.approval_date odate, o.amazon_order_id oorder
            FROM fba_reimbursements r
            LEFT JOIN LATERAL (SELECT * FROM fba_reimbursements o WHERE o.reimbursement_id = r.original_reimbursement_id
                               AND o.sku = r.sku ORDER BY o.approval_date LIMIT 1) o ON true
            WHERE r.reason = 'Reimbursement_Reversal' AND r.approval_date >= current_date - %(days)s),
ev AS (SELECT rv.*,
         (SELECT count(*) FROM fba_customer_returns c WHERE c.order_id = rv.oorder AND c.sku = rv.sku) returns_found,
         (SELECT coalesce(sum(l.quantity), 0) FROM ledger_detail l WHERE l.fnsku = rv.fnsku AND l.event_type = 'Receipts'
            AND l.quantity > 0 AND l.event_date BETWEEN rv.odate AND rv.rdate + 3) late_receipts,
         (SELECT coalesce(sum(l.quantity), 0) FROM ledger_detail l WHERE l.fnsku = rv.fnsku AND l.event_type = 'Adjustments'
            AND l.reason IN ('F','N') AND l.quantity > 0 AND l.event_date BETWEEN rv.odate - 30 AND rv.rdate + 3) found,
         (SELECT coalesce(sum(a.received - a.shipped), 0) FROM inbound_items_all a
            WHERE a.fnsku = rv.fnsku AND a.received > a.shipped) extra_elsewhere
       FROM rv)
SELECT rdate, reimbursement_id, oid, oreason, odate, sku, qty, amt,
       (oreason IS NOT NULL
        AND NOT (qinv >= qty AND qty > 0)
        AND NOT (oreason IN ('CustomerReturn','CustomerServiceIssue') AND returns_found > 0)
        AND NOT (oreason IN ('Lost_Inbound','Damaged_Inbound') AND late_receipts + found + extra_elsewhere >= qty)
        AND NOT (oreason IN ('Lost_Warehouse','Damaged_Warehouse') AND found >= qty)) AS unexplained
FROM ev"""


def _q(cur, sql, params=None):
    cur.execute(sql, params)
    return cur.fetchall()


def _money(v) -> str:
    return f"${float(v or 0):,.2f}"


def gather(conn) -> dict:
    """All the numbers. Each block is independent: one failing never hides the others."""
    d: dict = {}

    def block(name, fn):
        try:
            with conn.cursor() as cur:
                d[name] = fn(cur)
        except Exception as exc:
            d[name] = {"error": f"{type(exc).__name__}: {exc}"[:200]}

    block("agent3", lambda cur: {
        "new_tasks": _q(cur, """SELECT count(*), coalesce(sum(shortfall), 0) FROM payment_claims
                                 WHERE task_created_at > now() - interval '7 days'""")[0],
        "paid": _q(cur, """SELECT count(*), coalesce(sum(shortfall), 0) FROM payment_claims
                            WHERE monitor_state IN ('paid', 'paid_short') AND task_completed_at > now() - interval '7 days'""")[0],
        "open": _q(cur, """SELECT coalesce(monitor_state, 'to file'), count(*), coalesce(sum(shortfall), 0) FROM payment_claims
                            WHERE asana_task_gid IS NOT NULL AND task_completed_at IS NULL GROUP BY 1 ORDER BY 2 DESC"""),
        "deadlines": _q(cur, """SELECT sku, reimbursement_id, deadline, shortfall FROM payment_claims
                                 WHERE asana_task_gid IS NOT NULL AND task_completed_at IS NULL AND submitted_at IS NULL
                                   AND deadline <= current_date + 7 ORDER BY deadline LIMIT 15"""),
        "no_cost": _q(cur, """SELECT count(*) FROM payment_claims WHERE verdict = 'no_cost_in_notion'
                               AND last_seen > now() - interval '3 days'""")[0][0],
    })
    block("shipments", lambda cur: {
        "new_tasks": _q(cur, """SELECT count(*), coalesce(sum(claim_units), 0) FROM shipment_claims
                                 WHERE task_created_at > now() - interval '7 days'""")[0],
        "closed_no_claim": _q(cur, "SELECT count(*) FROM shipment_claims WHERE closed_at > now() - interval '7 days'")[0][0],
        "by_section": _q(cur, """SELECT section, count(*) FROM shipment_cases WHERE last_seen > now() - interval '2 days'
                                  GROUP BY 1 ORDER BY 1"""),
        "moves": _q(cur, """SELECT state, count(*) FROM shipment_cases WHERE state IS NOT NULL
                             AND last_seen > now() - interval '7 days' GROUP BY 1 ORDER BY 2 DESC"""),
    })
    block("refunds", lambda cur: {
        "waiting": _q(cur, """SELECT order_id, sku, refund_date, -refunded FROM refund_watch
                               WHERE verdict = 'candidate' AND refund_date <= current_date - 75
                               ORDER BY refund_date LIMIT 25"""),
        "on_task": _q(cur, "SELECT count(*) FROM refund_watch WHERE asana_task_gid IS NOT NULL")[0][0],
        "settled": _q(cur, "SELECT count(*) FROM refund_watch WHERE verdict = 'settled' AND checked_at > now() - interval '7 days'")[0][0],
    })
    block("reversals", lambda cur: (lambda rows: {
        "count": len(rows), "amount": sum(r[7] or 0 for r in rows),
        "unexplained": [r for r in rows if r[8]][:15]})(_q(cur, REVERSALS_SQL, {"days": 14})))
    block("health", lambda cur: {
        "payments": _q(cur, "SELECT status, started_at FROM payment_agent_runs ORDER BY id DESC LIMIT 1"),
        "shipments": _q(cur, "SELECT status, started_at FROM shipment_agent_runs ORDER BY id DESC LIMIT 1"),
        "refunds": _q(cur, "SELECT status, started_at FROM refund_agent_runs ORDER BY id DESC LIMIT 1"),
        "ledger": _q(cur, "SELECT status, started_at FROM ledger_sync_runs ORDER BY id DESC LIMIT 1"),
        "claims_data": _q(cur, "SELECT status, started_at FROM claims_sync_runs ORDER BY id DESC LIMIT 1"),
        "failed_runs": _q(cur, """SELECT (SELECT count(*) FROM payment_agent_runs WHERE status NOT IN ('ok','running') AND started_at > now() - interval '7 days')
                                       + (SELECT count(*) FROM shipment_agent_runs WHERE status NOT IN ('ok','running','interrupted') AND started_at > now() - interval '7 days')
                                       + (SELECT count(*) FROM claims_sync_runs WHERE status NOT IN ('ok','running') AND started_at > now() - interval '7 days')""")[0][0],
        "case_emails": _q(cur, "SELECT count(*) FROM case_emails WHERE received_at > now() - interval '7 days'")[0][0],
        "cost_file": _q(cur, "SELECT max(loaded_at) FROM amazon_sourcing_costs")[0][0],
    })
    return d


def render(d: dict, today) -> str:
    """Pure: the report text."""
    L = [f"REIMBURSEMENT AGENTS: WEEK ENDING {today:%b %-d, %Y}", ""]

    def err(name):
        e = d.get(name)
        if e is None or "error" in e:
            L.extend([f"(could not read this section: {(e or {}).get('error', 'no data')})", ""])
            return True
        return False

    L.append("1. REIMBURSEMENTS PAID SHORT (Agent 3, Azel)")
    if not err("agent3"):
        a = d["agent3"]
        L.append(f"- New tasks this week: {a['new_tasks'][0]}, worth {_money(a['new_tasks'][1])}")
        L.append(f"- Paid by Amazon this week after a case: {a['paid'][0]}, worth {_money(a['paid'][1])}")
        names = {"to file": "Waiting to be filed", "submitted": "Case submitted, agent watching", "sas_needed": "SAS case needed",
                 "fix_cost": "Sourcing cost needs fixing", "sas_submitted": "SAS case submitted", "denied": "Denied, needs your decision",
                 "reply_to_read": "Amazon reply to read", "cost_now_approved": "Cost approved, open a new case"}
        for state, n, amt in a["open"]:
            L.append(f"- {names.get(state, state)}: {n} ({_money(amt)})")
        if a["deadlines"]:
            L.append("- NOT FILED and Amazon's deadline is within 7 days:")
            L.extend(f"    {sku} | reimbursement {rid} | deadline {dl:%b %-d} | {_money(amt)}" for sku, rid, dl, amt in a["deadlines"])
        else:
            L.append("- No unfiled task has a deadline in the next 7 days.")
        if a["no_cost"]:
            L.append(f"- {a['no_cost']} reimbursement(s) could not be checked because the SKU has no cost in Notion.")
        L.append("")

    L.append("2. SHORT SHIPMENTS (Azel)")
    if not err("shipments"):
        s = d["shipments"]
        sec = {"1": "To file", "2": "Case submitted", "3": "Open SAS case", "4": "SAS case submitted"}
        L.append(f"- New tasks this week: {s['new_tasks'][0]} ({s['new_tasks'][1]} units)")
        if s["closed_no_claim"]:
            L.append(f"- Closed by the agent as nothing to claim: {s['closed_no_claim']}")
        for k, n in s["by_section"]:
            L.append(f"- {sec.get(k, k)}: {n} open")
        for state, n in s["moves"]:
            L.append(f"- Moved by the agent ({state.replace('_', ' ')}): {n}")
        L.append("")

    L.append("3. REFUNDS NOT RETURNED AND NOT REIMBURSED (Calvin's weekly case)")
    if not err("refunds"):
        r = d["refunds"]
        L.append(f"- Came off the list this week (unit returned or Amazon paid): {r['settled']}")
        L.append(f"- Already sent to Calvin on a weekly task: {r['on_task']} order(s)")
        if r["waiting"]:
            L.append(f"- Full refunds 75+ days old, still open: {len(r['waiting'])}, {_money(sum(x[3] for x in r['waiting']))}")
            L.extend(f"    {o} | {sku} | refunded {dt:%b %-d} | {_money(amt)}" for o, sku, dt, amt in r["waiting"])
        else:
            L.append("- Nothing open.")
        L.append("")

    L.append("4. REIMBURSEMENTS AMAZON TOOK BACK (last 14 days, no tasks are created for these)")
    if not err("reversals"):
        v = d["reversals"]
        L.append(f"- Reversals: {v['count']}, {_money(v['amount'])} taken back")
        if v["unexplained"]:
            L.append(f"- With no sign of the unit coming back: {len(v['unexplained'])}. File by hand if one is worth it:")
            L.extend(f"    reversal {rid} (original {oid}, {reason}, paid {od:%b %-d}) | {sku} | {qty} unit(s) | {_money(amt)}"
                     for _rd, rid, oid, reason, od, sku, qty, amt, _u in v["unexplained"])
        else:
            L.append("- All explained: the unit was returned, found, or received on another shipment.")
        L.append("")

    L.append("5. ARE THE AGENTS RUNNING")
    if not err("health"):
        h = d["health"]
        for label, key in (("Reimbursement check", "payments"), ("Shipments", "shipments"), ("Refunds", "refunds"),
                           ("Inventory ledger data", "ledger"), ("Reimbursement and returns data", "claims_data")):
            row = h[key][0] if h[key] else None
            L.append(f"- {label}: " + (f"last run {row[1].astimezone(TZ):%a %b %-d %-I:%M %p}, {row[0]}" if row else "never ran"))
        L.append(f"- Runs that failed this week: {h['failed_runs']}")
        L.append(f"- Amazon emails read from the cases inbox this week: {h['case_emails']}")
        if h["cost_file"]:
            age = (datetime.now(timezone.utc) - h["cost_file"]).days
            L.append(f"- Sourcing cost export last loaded {age} day(s) ago" + (" (LATE: Azel uploads it Monday and Thursday)" if age > 5 else ""))
    return "\n".join(L)


def run() -> dict:
    conn = ledger._db()
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS weekly_reports (
                               day date PRIMARY KEY, body text, asana_task_gid text, made_at timestamptz DEFAULT now())""")
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
            if cur.fetchone():
                cur.execute("GRANT SELECT ON weekly_reports TO ledger_reader")
        today = datetime.now(timezone.utc).astimezone(TZ).date()
        body = render(gather(conn), today)
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO weekly_reports (day, body) VALUES (%s, %s)
                           ON CONFLICT (day) DO UPDATE SET body = EXCLUDED.body, made_at = now()""", (today, body))
            cur.execute("SELECT asana_task_gid FROM weekly_reports WHERE day = %s", (today,))
            already = cur.fetchone()[0]
        out = {"day": str(today), "monday": today.weekday() == 0, "live": os.environ.get("WEEKLY_REPORT_LIVE") == "1",
               "chars": len(body)}
        token = os.environ.get("ASANA_TOKEN")
        if not (out["monday"] and out["live"] and token and not already):
            return out
        h = {"Authorization": f"Bearer {token}"}
        secs = requests.get(f"https://app.asana.com/api/1.0/projects/{BOARD}/sections", headers=h, timeout=60).json()["data"]
        sec = next((x["gid"] for x in secs if x["name"].strip().lower() == SECTION_NAME.lower()), None)
        if not sec:
            sec = requests.post(f"https://app.asana.com/api/1.0/projects/{BOARD}/sections", headers=h, timeout=60,
                                json={"data": {"name": SECTION_NAME}}).json()["data"]["gid"]
        data = {"name": f"Reimbursement agents: weekly report, {today:%b %-d, %Y}", "notes": body, "due_on": str(today),
                "projects": [BOARD], "memberships": [{"project": BOARD, "section": sec}]}
        if OWNER and OWNER.lower() != "none":
            data["assignee"] = OWNER
        r = requests.post("https://app.asana.com/api/1.0/tasks", headers=h, timeout=60, json={"data": data})
        r.raise_for_status()
        gid = r.json()["data"]["gid"]
        with conn.cursor() as cur:
            cur.execute("UPDATE weekly_reports SET asana_task_gid = %s WHERE day = %s", (gid, today))
        out["task_created"] = gid
        return out
    finally:
        conn.close()


def _wait_for_agents(max_minutes: int = 30):
    """The report is asked for at the same moment as the agents: let them finish first."""
    import time
    time.sleep(60)
    for _ in range(max_minutes * 2):
        conn = ledger._db()
        try:
            with conn.cursor() as cur:
                cur.execute("""SELECT (SELECT count(*) FROM payment_agent_runs WHERE status = 'running' AND started_at > now() - interval '2 hours')
                                    + (SELECT count(*) FROM shipment_agent_runs WHERE status = 'running' AND started_at > now() - interval '2 hours')""")
                if cur.fetchone()[0] == 0:
                    return
        except Exception:
            return
        finally:
            conn.close()
        time.sleep(30)


def _background():
    try:
        _wait_for_agents()
        print("weekly report:", json.dumps(run(), default=str))
    except Exception as exc:
        print(f"weekly report failed: {exc}")
    finally:
        _lock.release()


@router.post("/run")
def trigger(x_sync_token: str = Header("")):
    if not ledger.LEDGER_SYNC_TOKEN or x_sync_token != ledger.LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    if not _lock.acquire(blocking=False):
        return {"started": False}
    threading.Thread(target=_background, daemon=True).start()
    return {"started": True}
