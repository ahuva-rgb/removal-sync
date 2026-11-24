"""Agent 2, job 1: short inbound shipments.

Every day: read closed FBA inbound shipments from Amazon (read-only), compare units shipped with units
received, and list the shipments that are still short. This first version only LISTS them (test mode):
it creates no Asana tasks. Task creation with the packing slip is added once the list has been reviewed.

Rule (from Cassie): a shipment is ready to file only when its status is CLOSED.
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
            CREATE TABLE IF NOT EXISTS shipment_agent_runs (
                id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
                status text NOT NULL, dry_run boolean, summary jsonb, error text);
        """)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON inbound_shipments, inbound_shipment_items, shipment_agent_runs TO ledger_reader")


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
    summary = {"test_mode": True, "lookback_days": LOOKBACK_DAYS}
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
