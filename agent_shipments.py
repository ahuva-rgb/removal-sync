"""Agent 2, job 1: short inbound shipments.

Every day: read closed FBA inbound shipments from Amazon (read-only), compare units shipped with units
received, and list the shipments that are still short. This first version only LISTS them (test mode):
it creates no Asana tasks. Task creation with the packing slip is added once the list has been reviewed.

Rule (from Cassie): a shipment is ready to file only when its status is CLOSED.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from fastapi import APIRouter, Header, HTTPException

import ledger

router = APIRouter(prefix="/agents/shipments", tags=["agent: shipments"])
LOOKBACK_DAYS = int(os.environ.get("SHIPMENT_LOOKBACK_DAYS", "120"))
MAX_ITEM_PAGES = int(os.environ.get("SHIPMENT_MAX_ITEM_PAGES", "1500"))
ITEM_WINDOW_DAYS = int(os.environ.get("SHIPMENT_ITEM_WINDOW_DAYS", "45"))
ITEM_LOOKBACK_DAYS = int(os.environ.get("SHIPMENT_ITEM_LOOKBACK_DAYS", "420"))
SPOT_CHECKS = int(os.environ.get("SHIPMENT_SPOT_CHECKS", "8"))
RUN_MINUTES = int(os.environ.get("SHIPMENT_RUN_MINUTES", "300"))   # a run stops here and the next one carries on
MAX_PAGES = int(os.environ.get("SHIPMENT_MAX_PAGES", "150"))
_run_lock = threading.Lock()


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inbound_shipments (
                shipment_id text PRIMARY KEY, name text, status text, destination_fc text,
                shipped int, received int, short_units int, over_units int,
                first_seen_closed timestamptz, synced_at timestamptz NOT NULL DEFAULT now());
            ALTER TABLE inbound_shipments ADD COLUMN IF NOT EXISTS complete_read boolean NOT NULL DEFAULT false;
            ALTER TABLE inbound_shipments ADD COLUMN IF NOT EXISTS read_version int NOT NULL DEFAULT 0;
            CREATE TABLE IF NOT EXISTS inbound_shipment_items (
                shipment_id text NOT NULL, sku text NOT NULL, fnsku text, shipped int, received int,
                PRIMARY KEY (shipment_id, sku));
            CREATE TABLE IF NOT EXISTS shipment_board_refs (
                shipment_id text NOT NULL, project_gid text, board text, task_gid text NOT NULL, section text,
                completed boolean, in_name boolean, PRIMARY KEY (shipment_id, task_gid));
            CREATE TABLE IF NOT EXISTS shipment_claims (
                shipment_id text PRIMARY KEY, short_units int, claim_units int, asana_task_gid text,
                task_created_at timestamptz, slip_attached_at timestamptz,
                first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now());
            CREATE TABLE IF NOT EXISTS shipment_agent_runs (
                id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
                status text NOT NULL, dry_run boolean, summary jsonb, error text);
        """)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON inbound_shipments, inbound_shipment_items, shipment_agent_runs, shipment_board_refs, shipment_claims TO ledger_reader")


BOARDS = {   # where a shipment may already have a task
    "1211824376125946": "New Shipment Reconciliation",
    "1220620863326159": "Reimbursements to File",
    "1220818804510392": "Reimbursement Case Discrepancy",
    "1211663281581349": "Reimbursement Case Discrepancy (old)",
}
_SHIPMENT_ID = re.compile(r"FBA[0-9A-Z]{8,10}")


def scan_boards(conn, token: str, progress=None) -> dict:
    """Record every shipment ID that appears in a task name or description on the boards (open or completed)."""
    out, rows = {}, []
    for gid, label in BOARDS.items():
        params = {"limit": 100, "opt_fields": "name,notes,completed,memberships.section.name,memberships.project.gid"}
        n = 0
        try:
            while True:
                r = requests.get(f"https://app.asana.com/api/1.0/projects/{gid}/tasks", params=params, timeout=60,
                                 headers={"Authorization": f"Bearer {token}"})
                if r.status_code == 429:
                    time.sleep(float(r.headers.get("Retry-After", 5)))
                    continue
                r.raise_for_status()
                j = r.json()
                for t in j["data"]:
                    n += 1
                    ids = set(_SHIPMENT_ID.findall((t.get("name") or "").upper() + " " + (t.get("notes") or "").upper()))
                    sec = next(((m.get("section") or {}).get("name") for m in t.get("memberships", [])
                                if (m.get("project") or {}).get("gid") == gid), None)
                    in_name = set(_SHIPMENT_ID.findall((t.get("name") or "").upper()))
                    rows += [(sid, gid, label, t["gid"], sec, bool(t.get("completed")), sid in in_name) for sid in ids]
                if progress and n % 1000 == 0:
                    progress(f"checking the boards: {label}, {n} tasks read")
                nxt = (j.get("next_page") or {}).get("offset")
                if not nxt:
                    break
                params["offset"] = nxt
            out[label] = n
        except Exception as exc:
            out[label] = f"could not read: {str(exc)[:120]}"
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM shipment_board_refs")
        cur.executemany("""INSERT INTO shipment_board_refs (shipment_id, project_gid, board, task_gid, section, completed, in_name)
                           VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", rows)
    out["shipment_ids_found"] = len({r[0] for r in rows})
    return out


SHIPMENT_BOARD = "1211824376125946"
TO_FILE_SECTION = os.environ.get("SHIPMENT_TO_FILE_SECTION", "1212738040759998")   # "1. To file: shipments that need reconciliation"
ASSIGNEE = os.environ.get("ASANA_ASSIGNEE_GID", "1207717933464435")              # Azel
MAX_TASKS = int(os.environ.get("SHIPMENT_MAX_TASKS", "40"))


def live() -> bool:
    return os.environ.get("SHIPMENT_AGENT_LIVE") == "1"


CANDIDATES_SQL = """
WITH re AS (SELECT fnsku, sum(quantity_reimbursed_total) q FROM fba_reimbursements
            WHERE reason IN ('Lost_Inbound','Damaged_Inbound') AND approval_date >= current_date - 150 GROUP BY 1),
it AS (SELECT i.shipment_id, i.fnsku, (i.shipped - i.received) short,
              sum(i.shipped - i.received) OVER (PARTITION BY i.fnsku ORDER BY i.shipment_id) cum
       FROM inbound_shipment_items i JOIN inbound_shipments s USING (shipment_id)
       WHERE s.read_version >= 2 AND s.status = 'CLOSED' AND i.shipped > i.received),
un AS (SELECT it.shipment_id, sum(greatest(least(it.short, it.cum - coalesce(re.q, 0)), 0)) uncovered, sum(it.short) short
       FROM it LEFT JOIN re USING (fnsku) GROUP BY 1)
SELECT u.shipment_id, s.name, s.destination_fc, s.shipped, s.received, u.short, s.over_units,
       greatest(u.uncovered - s.over_units, 0)::int AS claim_units
FROM un u JOIN inbound_shipments s USING (shipment_id)
WHERE greatest(u.uncovered - s.over_units, 0) > 0
  AND u.shipment_id NOT IN (SELECT shipment_id FROM shipment_board_refs)
  AND u.shipment_id NOT IN (SELECT shipment_id FROM shipment_claims WHERE asana_task_gid IS NOT NULL)
ORDER BY 8 DESC, 1"""


def shipment_items(conn, shipment_id: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""SELECT i.sku, i.fnsku, i.shipped, i.received,
                              (SELECT max(l.title) FROM ledger_detail l
                               WHERE l.fnsku = i.fnsku AND l.event_date > current_date - 400)
                       FROM inbound_shipment_items i WHERE i.shipment_id = %s ORDER BY i.sku""", (shipment_id,))
        return [{"sku": a, "fnsku": b, "shipped": c, "received": d, "title": (e or "").split(" | ")[0][:110]}
                for a, b, c, d, e in cur.fetchall()]


def task_fields(c: dict, items: list[dict]) -> dict:
    """Pure: the Asana task for one short shipment."""
    short = [i for i in items if i["shipped"] > i["received"]]
    lines = [f"- {i['sku']} | FNSKU {i['fnsku']} | shipped {i['shipped']}, received {i['received']}, "
             f"short {i['shipped'] - i['received']}" for i in short]
    notes = ["STEPS",
             "1. Open the shipment in Seller Central and start the reconciliation / missing units case for the items below.",
             "2. Attach the packing slip that is on this task as the proof of what was shipped.",
             "3. Put the Amazon case number in the Case ID field and move the task to \"2. Case submitted\".",
             "",
             f"Shipment: {c['shipment_id']}  ({c['name']})",
             f"Amazon warehouse: {c['destination_fc']}",
             f"Units shipped: {c['shipped']}   Units received: {c['received']}   Units short: {c['short']}",
             f"Units to claim: {c['claim_units']}  (short units that Amazon has not already reimbursed"
             + (f", less {c['over_units']} extra unit(s) Amazon received on this shipment)" if c["over_units"] else ")"),
             "",
             "ITEMS SHORT", *lines,
             "",
             "Note: \"units to claim\" is the agent's estimate. Amazon's reimbursement report does not name the "
             "shipment, so if the shipment page shows a different number of missing units, go by the shipment page."]
    due = (datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")) + timedelta(days=1)).date()
    return {"name": f"Short shipment | {c['shipment_id']} | {c['name']} | {c['claim_units']} unit(s) to claim",
            "notes": "\n".join(notes), "due_on": str(due)}


def _asana(method: str, path: str, token: str, **kw) -> dict:
    r = requests.request(method, f"https://app.asana.com/api/1.0{path}", headers={"Authorization": f"Bearer {token}"},
                         timeout=90, **kw)
    if not r.ok:
        raise RuntimeError(f"Asana {r.status_code}: {r.text[:300]}")
    return r.json()["data"]


def make_tasks(conn, token: str, apply: bool) -> dict:
    """Plan (and, when live, create) a task with its packing slip for every short shipment that has none."""
    import packing_slip
    with conn.cursor() as cur:
        cur.execute(CANDIDATES_SQL)
        cols = ["shipment_id", "name", "destination_fc", "shipped", "received", "short", "over_units", "claim_units"]
        cands = [dict(zip(cols, r)) for r in cur.fetchall()]
    planned, created, errors = [], 0, []
    for c in cands:
        items = shipment_items(conn, c["shipment_id"])
        f = task_fields(c, items)
        planned.append({"task": f["name"], "items_on_slip": len(items), "units_on_slip": sum(i["shipped"] for i in items)})
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO shipment_claims (shipment_id, short_units, claim_units) VALUES (%s,%s,%s)
                           ON CONFLICT (shipment_id) DO UPDATE SET short_units=EXCLUDED.short_units,
                             claim_units=EXCLUDED.claim_units, last_seen=now()""",
                        (c["shipment_id"], c["short"], c["claim_units"]))
        if not apply or created >= MAX_TASKS:
            continue
        try:
            pdf = packing_slip.build(c["shipment_id"], c["name"], c["destination_fc"],
                                     [(i["sku"], i["title"], i["shipped"], i["fnsku"]) for i in items])
            data = {**f, "projects": [SHIPMENT_BOARD], "memberships": [{"project": SHIPMENT_BOARD, "section": TO_FILE_SECTION}]}
            if ASSIGNEE and ASSIGNEE.lower() != "none":
                data["assignee"] = ASSIGNEE
            gid = _asana("POST", "/tasks", token, json={"data": data})["gid"]
            with conn.cursor() as cur:      # remember it first, so a failed upload can never lead to a second task
                cur.execute("UPDATE shipment_claims SET asana_task_gid=%s, task_created_at=now() WHERE shipment_id=%s",
                            (gid, c["shipment_id"]))
            _asana("POST", "/attachments", token, data={"parent": gid},
                   files={"file": (f"Packing Slip {c['shipment_id']}.pdf", pdf, "application/pdf")})
            with conn.cursor() as cur:
                cur.execute("UPDATE shipment_claims SET slip_attached_at=now() WHERE shipment_id=%s", (c["shipment_id"],))
            created += 1
        except Exception as exc:
            errors.append(f"{c['shipment_id']}: {exc}"[:200])
            if len(errors) >= 3:
                break
    return {"live": apply, "candidates": len(cands), "units_to_claim": sum(c["claim_units"] for c in cands),
            "tasks_created": created, "task_errors": errors, "planned": planned}


def _get(token: str, path: str, params: dict) -> dict:
    """One read-only SP-API call. Waits and retries a few times when Amazon says slow down."""
    import main
    for attempt in range(5):
        r = requests.get(f"{main.SPAPI_ENDPOINT}{path}", headers={"x-amz-access-token": token}, params=params, timeout=40)
        if r.status_code == 429:
            time.sleep(2 + attempt * 2)
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"Amazon {r.status_code} on {path}: {r.text[:300]}")
        return r.json().get("payload") or {}
    raise RuntimeError(f"Amazon kept answering 429 on {path}")


def fetch_closed_shipments(token: str, since: datetime, until: datetime, progress=None) -> tuple[list[dict], str]:
    """Closed shipments updated in the window. Returns (shipments, how the paging ended).
    Guards against Amazon handing back the same page forever."""
    import main
    seen, out, tokens, pages = set(), [], set(), 0
    params = {"MarketplaceId": main.MARKETPLACE_ID, "QueryType": "DATE_RANGE", "ShipmentStatusList": "CLOSED",
              "LastUpdatedAfter": since.strftime("%Y-%m-%dT%H:%M:%SZ"),
              "LastUpdatedBefore": until.strftime("%Y-%m-%dT%H:%M:%SZ")}
    while True:
        p = _get(token, "/fba/inbound/v0/shipments", params)
        pages += 1
        new = [x for x in p.get("ShipmentData", []) if x.get("ShipmentId") not in seen]
        for x in new:
            seen.add(x["ShipmentId"])
        out += new
        if progress:
            progress(f"reading the shipment list: page {pages}, {len(out)} closed shipments so far")
        nxt = p.get("NextToken")
        if not nxt:
            return out, "complete"
        if nxt in tokens or not new:
            return out, f"stopped at page {pages}: Amazon repeated a page"
        if pages >= MAX_PAGES:
            return out, f"stopped at the {MAX_PAGES}-page limit"
        tokens.add(nxt)
        params = {"MarketplaceId": main.MARKETPLACE_ID, "QueryType": "NEXT_TOKEN", "NextToken": nxt}
        time.sleep(0.6)


def fetch_items(token: str, shipment_id: str) -> list[dict]:
    """All items of one shipment, one line per SKU. Amazon keeps handing back a NextToken for this call and
    then repeats the same page, so stop as soon as a page brings nothing new."""
    import main
    by, tokens, params = {}, set(), {"MarketplaceId": main.MARKETPLACE_ID}
    for _ in range(60):
        p = _get(token, f"/fba/inbound/v0/shipments/{shipment_id}/items", params)
        new = 0
        for i in p.get("ItemData", []):
            k = i.get("SellerSKU") or ""
            new += k not in by
            by[k] = i
        nxt = p.get("NextToken")
        if not nxt or nxt in tokens or not new:
            break
        tokens.add(nxt)
        params = {"MarketplaceId": main.MARKETPLACE_ID, "NextToken": nxt}
        time.sleep(0.5)
    return list(by.values())


def fetch_all_items(token: str, since: datetime, until: datetime, progress=None) -> tuple[dict, str]:
    """Every shipment item Amazon updated between `since` and `until`, read in large pages, window by window:
    {shipment_id: {sku: item}}.  The window must reach back further than the shipments were CREATED, because
    an item that was never received keeps its creation date - and those are exactly the short ones."""
    import main
    by, pages, n, notes = {}, 0, 0, []
    end = until
    while end > since:
        start = max(since, end - timedelta(days=ITEM_WINDOW_DAYS))
        tokens = set()
        params = {"MarketplaceId": main.MARKETPLACE_ID, "QueryType": "DATE_RANGE",
                  "LastUpdatedAfter": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                  "LastUpdatedBefore": end.strftime("%Y-%m-%dT%H:%M:%SZ")}
        while True:
            p = _get(token, "/fba/inbound/v0/shipmentItems", params)
            pages += 1
            items = p.get("ItemData", [])
            for i in items:
                by.setdefault(i.get("ShipmentId"), {})[i.get("SellerSKU") or ""] = i
            n += len(items)
            if progress and pages % 5 == 0:
                progress(f"reading shipment items: page {pages}, {n} item lines, back to {start:%b %-d, %Y}")
            nxt = p.get("NextToken")
            if not nxt:
                break
            if nxt in tokens or not items:
                notes.append(f"Amazon repeated a page in the window ending {end:%Y-%m-%d}")
                break
            if pages >= MAX_ITEM_PAGES:
                return by, f"stopped at the {MAX_ITEM_PAGES}-page limit, back to {start:%Y-%m-%d}"
            tokens.add(nxt)
            params = {"MarketplaceId": main.MARKETPLACE_ID, "QueryType": "NEXT_TOKEN", "NextToken": nxt}
            time.sleep(0.6)
        end = start
    return by, "; ".join(notes) or f"complete ({pages} pages, {n} item lines)"


def totals(items: list[dict]) -> dict:
    """Pure: shipped / received / short / over for one shipment's items."""
    shipped = sum(int(i.get("QuantityShipped") or 0) for i in items)
    received = sum(int(i.get("QuantityReceived") or 0) for i in items)
    short = sum(max(int(i.get("QuantityShipped") or 0) - int(i.get("QuantityReceived") or 0), 0) for i in items)
    over = sum(max(int(i.get("QuantityReceived") or 0) - int(i.get("QuantityShipped") or 0), 0) for i in items)
    return {"shipped": shipped, "received": received, "short_units": short, "over_units": over}


def run(dry_run: bool = True) -> dict:
    import main
    conn = ledger._db()
    ensure_tables(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE shipment_agent_runs SET status='interrupted', finished_at=now() WHERE status='running'")
        cur.execute("INSERT INTO shipment_agent_runs (status, dry_run) VALUES ('running', true) RETURNING id")
        run_id = cur.fetchone()[0]
    summary = {"test_mode": not live(), "lookback_days": LOOKBACK_DAYS}
    try:
        token = main.lwa_token()
        now = datetime.now(timezone.utc)
        def progress(step: str):
            with conn.cursor() as cur:
                cur.execute("UPDATE shipment_agent_runs SET summary=%s WHERE id=%s",
                            (json.dumps({**summary, "step": step}, default=str), run_id))

        shipments, paging = fetch_closed_shipments(token, now - timedelta(days=LOOKBACK_DAYS), now, progress)
        summary.update(closed_shipments=len(shipments), shipment_list=paging)
        short_list, errors = [], []
        with conn.cursor() as cur:
            # closed and fully received: never read again.  Still short: read again once a day.
            cur.execute("""SELECT shipment_id FROM inbound_shipments
                           WHERE status='CLOSED' AND complete_read AND read_version >= 2
                             AND (short_units = 0 OR synced_at > now() - interval '20 hours')""")
            settled = {r[0] for r in cur.fetchall()}
        deadline = time.time() + RUN_MINUTES * 60
        bulk, t0 = {}, time.time()
        try:
            bulk, how = fetch_all_items(token, now - timedelta(days=ITEM_LOOKBACK_DAYS), now, progress)
            summary.update(item_list=how, shipments_with_items=len(bulk))
        except Exception as exc:
            summary.update(item_list=f"bulk read failed: {str(exc)[:200]}")
        summary["items_seconds"] = round(time.time() - t0)

        # Spot check: read a few shipments the slow, exact way and compare with the bulk numbers.
        checked, mismatches = 0, []
        candidates = [x["ShipmentId"] for x in shipments if x["ShipmentId"] in bulk]
        shorts = [c for c in candidates if totals(list(bulk[c].values()))["short_units"] > 0]
        for sid in (shorts[:SPOT_CHECKS // 2] + [c for c in candidates if c not in shorts][:SPOT_CHECKS // 2]):
            try:
                exact, fast = totals(fetch_items(token, sid)), totals(list(bulk[sid].values()))
                checked += 1
                if exact != fast:
                    mismatches.append({"shipment": sid, "exact": exact, "bulk": fast})
            except Exception as exc:
                mismatches.append({"shipment": sid, "error": str(exc)[:120]})
        summary.update(spot_checked=checked, spot_mismatches=mismatches)
        trust_bulk = checked > 0 and not mismatches

        read = 0
        for n, s in enumerate(shipments):
            sid = s["ShipmentId"]
            if sid in settled:
                continue
            if trust_bulk and sid in bulk:
                items = list(bulk[sid].values())
            else:
                if time.time() > deadline:
                    summary["left_for_next_run"] = summary.get("left_for_next_run", 0) + 1
                    continue
                read += 1
                if read % 5 == 1:
                    progress(f"reading items one shipment at a time: {read} read, at {n + 1} of {len(shipments)}")
                try:
                    items = fetch_items(token, sid)
                    time.sleep(0.5)
                except Exception as exc:
                    errors.append(f"{sid}: {exc}"[:200])
                    if len(errors) >= 5:
                        break
                    continue
            t = totals(items)
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO inbound_shipments (shipment_id, name, status, destination_fc, shipped, received,
                                                              short_units, over_units, first_seen_closed, complete_read, read_version)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now(),true,2)
                               ON CONFLICT (shipment_id) DO UPDATE SET name=EXCLUDED.name, status=EXCLUDED.status,
                                 destination_fc=EXCLUDED.destination_fc, shipped=EXCLUDED.shipped, received=EXCLUDED.received,
                                 short_units=EXCLUDED.short_units, over_units=EXCLUDED.over_units, synced_at=now(),
                                 complete_read=true, read_version=2""",
                            (sid, s.get("ShipmentName"), s.get("ShipmentStatus"), s.get("DestinationFulfillmentCenterId"),
                             t["shipped"], t["received"], t["short_units"], t["over_units"]))
                cur.execute("DELETE FROM inbound_shipment_items WHERE shipment_id=%s", (sid,))
                agg = {}
                for i in items:
                    k = i.get("SellerSKU") or ""
                    a = agg.setdefault(k, [i.get("FulfillmentNetworkSKU"), 0, 0])
                    a[1] += int(i.get("QuantityShipped") or 0)
                    a[2] += int(i.get("QuantityReceived") or 0)
                cur.executemany("INSERT INTO inbound_shipment_items (shipment_id, sku, fnsku, shipped, received) "
                                "VALUES (%s,%s,%s,%s,%s)", [(sid, sku, f, sh, rc) for sku, (f, sh, rc) in agg.items()])
            if t["short_units"] > 0:
                short_list.append({"shipment_id": sid, "name": s.get("ShipmentName"), **t})
        if os.environ.get("ASANA_TOKEN"):
            progress("checking the boards for existing shipment tasks")
            try:
                summary["boards"] = scan_boards(conn, os.environ["ASANA_TOKEN"], progress)
            except Exception as exc:
                summary["boards_error"] = f"{type(exc).__name__}: {exc}"[:300]
        boards_ok = isinstance(summary.get("boards"), dict) and not any(
            str(v).startswith("could not read") for v in summary["boards"].values())
        if boards_ok:      # never plan or create tasks on a day the boards could not be checked
            progress("working out which short shipments need a task")
            summary["tasks"] = make_tasks(conn, os.environ["ASANA_TOKEN"], apply=live())
        else:
            summary["tasks"] = "skipped: the boards could not be checked, so no task was planned or created"
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*), count(*) FILTER (WHERE short_units > 0), coalesce(sum(short_units), 0)
                           FROM inbound_shipments WHERE status='CLOSED' AND read_version >= 2""")
            done, n_short, units = cur.fetchone()
        summary.update(read_one_at_a_time=read, shipments_checked_so_far=done, short_shipments=n_short,
                       short_units=int(units), errors=errors)
        status = "ok" if not errors else "ok_with_errors"
        with conn.cursor() as cur:
            cur.execute("UPDATE shipment_agent_runs SET status=%s, finished_at=now(), summary=%s WHERE id=%s",
                        (status, json.dumps(summary, default=str), run_id))
        return {"run_id": run_id, **summary}
    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute("UPDATE shipment_agent_runs SET status='error', finished_at=now(), summary=%s, error=%s WHERE id=%s",
                        (json.dumps(summary, default=str), f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1200:]}",
                         run_id))
        raise
    finally:
        conn.close()


def _background():
    try:
        run()
    except Exception as exc:
        print(f"shipments agent failed: {exc}")
    finally:
        _run_lock.release()


@router.post("/run")
def trigger(x_sync_token: str = Header("")):
    if not ledger.LEDGER_SYNC_TOKEN or x_sync_token != ledger.LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    if not _run_lock.acquire(blocking=False):
        return {"started": False, "reason": "the shipments agent is already running"}
    threading.Thread(target=_background, daemon=True).start()
    return {"started": True, "test_mode": True}
