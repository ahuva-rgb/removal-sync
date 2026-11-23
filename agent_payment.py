"""Agent 3 - Payment accuracy.

Runs every day after the ledger + reimbursements syncs. For every warehouse / inbound
reimbursement Amazon issued in the last DISPUTE_DAYS days it:

  1. finds the loss line in the Inventory Ledger and the status the unit was in,
  2. works out whether 100% or 50% of sourcing cost applies,
  3. compares what Amazon paid with the sourcing cost in Notion,
  4. when Amazon paid at least $1 too little, creates ONE Asana task with the two
     amounts for the case form, the case wording and the Notion invoice link.

Payout rule (Cassie, Nov 2026)
  sellable / new unit ..................................... 100% of sourcing cost
  customer damaged or defective, came from a customer return  50%
  customer damaged or defective, but Amazon re-graded it ..... 100%
  any other status (warehouse / carrier / distributor damaged) 100%
  shortfall under $1 per unit, or paid at/above cost ......... no task

Nothing here writes to Amazon. It reads our own database and Notion and writes Asana tasks.

Env vars (all optional except the tokens)
  ASANA_TOKEN                    without it the agent only records what it WOULD create
  ASANA_PROJECT_GID              default: Reimbursement Case Discrepancy board
  ASANA_ASSIGNEE_GID             who gets the tasks (default Azel); "none" for unassigned
  ASANA_SECTION_100 / _50        section names on the board (default "To file: 100%" / "To file: 50%")
  NOTION_TOKEN                   already set on this service
  NOTION_SKU_DB / NOTION_COGS_DB / NOTION_LINE_ITEMS_DB   Notion database ids
  PAYMENT_AGENT_PAUSED=1         dry run only: the list is worked out daily, nothing is created or changed in Asana
  PAYMENT_AGENT_MAX_TASKS        most tasks created in one run (default 100)
  PAYMENT_AGENT_SKIP_SKU         comma list of text; SKUs containing any are ignored (default "ROP")
  PAYMENT_AGENT_ALREADY_FILED    comma list of reimbursement ids filed by hand; never tasked
                                 (default: the two Cassie filed on Nov 17, 2026)
  PAYMENT_AGENT_LIVE_REASONS     reimbursement types that may create tasks (default the two warehouse ones;
                                 "all" for every type). Other types are still checked and listed, with no task.
"""
import json
import os
import re
import threading
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal

import requests
from fastapi import APIRouter, Header, HTTPException, Query

import ledger

router = APIRouter(prefix="/agents/payment-accuracy", tags=["agent: payment accuracy"])

DISPUTE_DAYS = 60            # Amazon accepts an amount dispute for 60 days after it issues the reimbursement
LOSS_LOOKBACK_DAYS = 60      # how far before the approval date the loss line can be
ORIGIN_LOOKBACK_DAYS = 400   # how far back we look for how the unit got its status
REGRADE_NEAR_DAYS = 45       # "Amazon re-graded it at this warehouse shortly before it was lost"
MIN_SHORTFALL = Decimal("1.00")

LOST_CODES = ("M", "5")
DAMAGED_CODES = ("E", "H", "K", "U", "6")
REGRADE_IN_CODES = ("P",) + DAMAGED_CODES      # positive quantity = unit moved INTO this status by Amazon
WAREHOUSE_REASONS = ("Lost_Warehouse", "Damaged_Warehouse")
INBOUND_REASONS = ("Lost_Inbound", "Damaged_Inbound")
OUTBOUND_REASONS = ("Lost_Outbound",)
ORDER_REASONS = ("CustomerReturn", "CustomerServiceIssue", "ReturnAdjustment")     # tied to a customer order
ALL_REASONS = WAREHOUSE_REASONS + INBOUND_REASONS + OUTBOUND_REASONS + ORDER_REASONS
HALF_STATUSES = ("CUSTOMER_DAMAGED", "DEFECTIVE")

ASANA_API = "https://app.asana.com/api/1.0"
ASANA_PROJECT_GID = os.environ.get("ASANA_PROJECT_GID", "1220818804510392")
ASANA_SECTION_100 = os.environ.get("ASANA_SECTION_100", "To file: 100%")
ASANA_SECTION_50 = os.environ.get("ASANA_SECTION_50", "To file: 50%")
ASANA_SECTION_NEEDS_INVOICE = os.environ.get("ASANA_SECTION_NEEDS_INVOICE", "Needs invoice: not found")
ASANA_ASSIGNEE_GID = os.environ.get("ASANA_ASSIGNEE_GID", "1207717933464435")   # Azel Lauzon; set to "none" for unassigned
BUSINESS_TZ = ZoneInfo("America/New_York")
NOTION_SKU_DB = os.environ.get("NOTION_SKU_DB", "2630ab7bdc07806daf93d8a80acbc67e")
NOTION_COGS_DB = os.environ.get("NOTION_COGS_DB", "2630ab7bdc078034b64afa9daafcca20")
NOTION_LINE_ITEMS_DB = os.environ.get("NOTION_LINE_ITEMS_DB", "2370ab7bdc07817f83b7f9fd7d4827a9")
NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

_run_lock = threading.Lock()
_step = ""


def _set_step(s: str):
    global _step
    _step = s
    print("payment agent:", s)


def _skip_patterns():
    return [p.strip().upper() for p in os.environ.get("PAYMENT_AGENT_SKIP_SKU", "ROP").split(",") if p.strip()]


def _reasons():
    """Every reimbursement type is checked. (Reversals and adjustments are netted into the one they belong to.)"""
    return ALL_REASONS


def _live_reasons():
    """Reimbursement types that may create Asana tasks. The others are worked out and saved every day
    (verdict 'claim', no task) so the list can be reviewed before they are switched on."""
    raw = os.environ.get("PAYMENT_AGENT_LIVE_REASONS", ",".join(WAREHOUSE_REASONS))
    if raw.strip().lower() == "all":
        return set(ALL_REASONS)
    return {r.strip() for r in raw.split(",") if r.strip()}


def _already_filed():
    return {p.strip() for p in os.environ.get("PAYMENT_AGENT_ALREADY_FILED", "23548488361,23730409951").split(",") if p.strip()}


# ------------------------------------------------------------------ tables ---

def ensure_tables(conn):
    with ledger._SETUP_LOCK, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sku_costs (
                sku text NOT NULL, valid_from date NOT NULL, cost numeric(14,2) NOT NULL,
                asin text, fnsku text, upc_page_id text, synced_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (sku, valid_from));
            CREATE TABLE IF NOT EXISTS payment_claims (
                reimbursement_id text NOT NULL, sku text NOT NULL,
                approval_date date, deadline date, reason text, fnsku text, asin text,
                quantity bigint, paid_total numeric(14,2),
                status_when_lost text, loss_date date, loss_fc text,
                rule text, pct int, cost numeric(14,2), correct_total numeric(14,2), shortfall numeric(14,2),
                verdict text NOT NULL, check_note text,
                invoice_number text, invoice_url text, invoice_price numeric(14,2),
                asana_task_gid text, task_created_at timestamptz,
                first_seen timestamptz NOT NULL DEFAULT now(), last_seen timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (reimbursement_id, sku));
            CREATE TABLE IF NOT EXISTS payment_agent_runs (
                id bigserial PRIMARY KEY, started_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
                status text NOT NULL, dry_run boolean, summary jsonb, error text);
            CREATE TABLE IF NOT EXISTS sku_cost_uploads (
                sku text PRIMARY KEY, uploaded_on date NOT NULL, rejected boolean NOT NULL DEFAULT false,
                source text, noted_at timestamptz NOT NULL DEFAULT now());
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS steps_version text;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS task_completed_at timestamptz;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS upc text;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS amazon_order_id text;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS invoice_attached_at timestamptz;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS invoice_attach_note text;
            ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS invoice_attach_tries int NOT NULL DEFAULT 0;
        """)
        _seed_cost_uploads(cur)
        cur.execute("""SELECT count(*) FROM information_schema.columns
                       WHERE table_name = 'ledger_detail' AND column_name IN ('fnsku', 'event_date')""")
        if cur.fetchone()[0] == 2:   # speeds up the per-item ledger lookups; harmless if it already exists
            cur.execute("CREATE INDEX IF NOT EXISTS ledger_detail_fnsku_date_idx ON ledger_detail (fnsku, event_date)")
        cur.execute("SELECT to_regclass('fba_reimbursements') IS NOT NULL")
        if cur.fetchone()[0]:        # netting adjustments and reversals looks rows up by the reimbursement they point at
            cur.execute("""CREATE INDEX IF NOT EXISTS fba_reimbursements_original_idx
                           ON fba_reimbursements (original_reimbursement_id)""")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():
            cur.execute("GRANT SELECT ON sku_costs, payment_claims, payment_agent_runs, sku_cost_uploads TO ledger_reader")


def _seed_cost_uploads(cur):
    """sourcing_cost_uploads_seed.csv (sku,uploaded_on) lists costs already uploaded to Amazon by hand."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sourcing_cost_uploads_seed.csv")
    if not os.path.exists(path):
        return
    import csv
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("sku") and row.get("uploaded_on"):
                cur.execute("""INSERT INTO sku_cost_uploads (sku, uploaded_on, rejected, source)
                               VALUES (%s, %s, %s, 'seed file') ON CONFLICT (sku) DO NOTHING""",
                            (row["sku"].strip().upper(), row["uploaded_on"].strip(),
                             (row.get("rejected") or "").strip().lower() == "true"))


def steps_version(c: dict, uploads: dict) -> str:
    """A = upload the sourcing cost first, then open the case.  B = it was already tried: go straight to the case."""
    u = uploads.get(c["sku"].upper())
    if not u:
        return "A"
    if u["rejected"]:
        return "B"
    ref = c.get("loss_date") or c.get("approval_date")
    return "B" if ref and ref >= u["uploaded_on"] else "A"


# ------------------------------------------------------------------ Notion ---

def _notion_headers():
    # PAYMENT_NOTION_TOKEN lets this agent use its own read-only Notion connection; otherwise the service's one.
    token = os.environ.get("PAYMENT_NOTION_TOKEN") or os.environ.get("NOTION_TOKEN", "")
    if not token:
        raise RuntimeError("NOTION_TOKEN is not set")
    return {"Authorization": f"Bearer {token}", "Notion-Version": NOTION_VERSION, "Content-Type": "application/json"}


def _notion(method: str, path: str, body: dict | None = None) -> dict:
    for attempt in range(6):
        r = requests.request(method, f"{NOTION_API}{path}", headers=_notion_headers(),
                             json=body if body is not None else None, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(float(r.headers.get("Retry-After", 2 + attempt * 2)))
            continue
        if r.status_code == 404:
            raise RuntimeError(f"Notion says it cannot find {path.split('/')[2]}. Share that database with the "
                               "integration behind NOTION_TOKEN (••• menu > Connections).")
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}


def _notion_query_all(db_id: str, body: dict | None = None):
    body = dict(body or {})
    body["page_size"] = 100
    while True:
        data = _notion("POST", f"/databases/{db_id}/query", body)
        yield from data.get("results", [])
        if not data.get("has_more"):
            return
        body["start_cursor"] = data["next_cursor"]


def _text(prop: dict | None) -> str:
    if not prop:
        return ""
    parts = prop.get("title") or prop.get("rich_text") or []
    return "".join(p.get("plain_text", "") for p in parts).strip()


def _relation_ids(prop: dict | None) -> list[str]:
    return [r["id"].replace("-", "") for r in (prop or {}).get("relation", [])]


def sync_costs(conn) -> dict:
    """Copy Notion's SKU -> sourcing cost history into sku_costs (bundle accessory cost is NOT included)."""
    _set_step("reading sourcing costs from Notion (COGS)")
    by_upc: dict[str, list[tuple[date, Decimal]]] = {}
    n_cogs = 0
    for page in _notion_query_all(NOTION_COGS_DB):
        p = page["properties"]
        price = (p.get("Unit Price (COGS)") or {}).get("number")
        if price is None:
            continue
        start = ((p.get("Price Valid From") or {}).get("date") or {}).get("start")
        vf = date.fromisoformat(start[:10]) if start else date(1900, 1, 1)
        for upc_id in _relation_ids(p.get("UPC")):
            by_upc.setdefault(upc_id, []).append((vf, Decimal(str(price)).quantize(Decimal("0.01"))))
        n_cogs += 1
    _set_step("reading SKUs from Notion (SKU & ASIN)")
    rows = {}
    for page in _notion_query_all(NOTION_SKU_DB):
        p = page["properties"]
        sku = _text(p.get("SKU"))
        if not sku:
            continue
        for upc_id in _relation_ids(p.get("UPC"))[:1]:
            for vf, cost in by_upc.get(upc_id, []):
                key = (sku, vf)
                # same SKU + same start date twice in Notion: keep the higher one and let the task show it
                if key not in rows or cost > rows[key][0]:
                    rows[key] = (cost, _text(p.get("ASIN")) or None, _text(p.get("fnsku")) or None, upc_id)
    if not rows:
        raise RuntimeError("Notion returned no SKU costs; keeping the previous copy")
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM sku_costs")
        cur.executemany(
            "INSERT INTO sku_costs (sku, valid_from, cost, asin, fnsku, upc_page_id) VALUES (%s,%s,%s,%s,%s,%s)",
            [(sku, vf, c, asin, fnsku, upc) for (sku, vf), (c, asin, fnsku, upc) in rows.items()])
    return {"cogs_rows": n_cogs, "sku_cost_rows": len(rows), "skus": len({k[0] for k in rows})}


def cost_on(history: list[tuple[date, Decimal]], on: date | None = None) -> Decimal | None:
    """The CURRENT cost: the Notion COGS row with the latest "Price Valid From".

    Older prices are never used, whatever the loss date was: the claim has to match what Notion
    (and the invoice) show today. `on` is kept only so existing callers do not break.
    """
    if not history:
        return None
    return sorted(history)[-1][1]


def find_invoice(upc_page_id: str, cost: Decimal) -> dict:
    """Most recent supplier invoice line for this UPC, preferring one whose unit price equals our cost."""
    upc_page = _notion("GET", f"/pages/{upc_page_id}")
    upc = _text(upc_page["properties"].get("UPC"))
    if not upc:
        return {}
    body = {"filter": {"and": [{"property": "UPC", "rich_text": {"equals": upc}},
                               {"property": "Gross Cost Price", "number": {"greater_than": 0}}]},
            "sorts": [{"timestamp": "created_time", "direction": "descending"}], "page_size": 50}
    lines = _notion("POST", f"/databases/{NOTION_LINE_ITEMS_DB}/query", body).get("results", [])
    cands = []
    for li in lines:
        p = li["properties"]
        if (p.get("Line Status") or {}).get("select") and p["Line Status"]["select"].get("name") == "Superseded":
            continue
        inv_ids = _relation_ids(p.get("Invoice"))
        price = (p.get("Gross Cost Price") or {}).get("number")
        if not inv_ids or price is None:
            continue
        price = Decimal(str(price)).quantize(Decimal("0.01"))
        cands.append((price == cost, li.get("created_time", ""), price, inv_ids[0]))
    if not cands:
        return {"upc": upc}          # no invoice line in Notion: the mailbox can still be searched by UPC
    cands.sort(reverse=True)
    _, _, price, inv_id = cands[0]
    inv = _notion("GET", f"/pages/{inv_id}")
    ip = inv["properties"]
    files = (ip.get("Invoice") or {}).get("files") or []
    file_url = next((f.get("file", {}).get("url") or f.get("external", {}).get("url") for f in files), None)
    return {"invoice_number": _text(ip.get("Invoice Number")) or None,
            "invoice_url": inv.get("url"), "invoice_price": price,
            "upc": upc, "_invoice_file_url": file_url, "_invoice_email_id": _text(ip.get("Email ID")) or None}


# ---------------------------------------------------------------- deciding ---

def classify(group: dict, adjustments: list[dict], returns: list[dict],
             cost_history: list[tuple[date, Decimal]], order_returns: list[dict] | None = None) -> dict:
    """Decide one reimbursement. Pure function (no database, no network) so it can be tested.

    group        reimbursement_id, sku, fnsku, reason, approval_date, quantity, paid_total
    adjustments  ledger Adjustments rows for this FNSKU: event_date, reason, disposition, fc, quantity
    returns      ledger CustomerReturns rows into a non-sellable status: event_date, disposition, fc, quantity
    order_returns  for order-based reimbursements: what the Customer Returns report shows for that order + SKU
                   (return_date, disposition)

    Rule: 50% of sourcing cost only when a customer returned the unit damaged or defective; 100% otherwise.
    """
    out = dict(group)
    approval = group["approval_date"]
    out["deadline"] = approval + timedelta(days=DISPUTE_DAYS)
    qty = int(group["quantity"])
    paid = Decimal(group["paid_total"])
    notes = []

    if any(pat in (group["sku"] or "").upper() for pat in _skip_patterns()):
        return {**out, "verdict": "skipped_sku"}

    if group["reason"] in INBOUND_REASONS:
        status, loss_date, loss_fc = "SELLABLE", approval, None
        rule, pct = "New unit lost or damaged on the way in", 100
    elif group["reason"] in OUTBOUND_REASONS:
        status, loss_date, loss_fc = "SELLABLE", approval, None
        rule, pct = "Sellable unit lost on the way to the customer", 100
    elif group["reason"] in ORDER_REASONS:
        loss_date, loss_fc = approval, None
        back = sorted(order_returns or [], key=lambda r: r["return_date"], reverse=True)
        half = [r for r in back if r["disposition"] in HALF_STATUSES]
        if half:
            status = half[0]["disposition"]
            rule, pct = f"Customer returned the unit as {_pretty(status)} on {half[0]['return_date']:%b %-d}", 50
        elif back:
            status = back[0]["disposition"] or "SELLABLE"
            rule, pct = f"Unit came back as {_pretty(status)} (not customer damaged or defective)", 100
        else:
            status = "SELLABLE"
            rule, pct = "The returns report shows no unit coming back for this order", 100
    else:
        codes = LOST_CODES if group["reason"] == "Lost_Warehouse" else DAMAGED_CODES
        lo = approval - timedelta(days=LOSS_LOOKBACK_DAYS)
        cands = [a for a in adjustments if a["quantity"] < 0 and a["reason"] in codes and lo <= a["event_date"] <= approval]
        if not cands:
            return {**out, "verdict": "no_ledger_line"}
        cands.sort(key=lambda a: a["event_date"], reverse=True)
        loss = cands[0]
        status, loss_date, loss_fc = loss["disposition"], loss["event_date"], loss["fc"]
        if len({a["disposition"] for a in cands}) > 1:
            notes.append("This item had losses in more than one status that month. Confirm in the Inventory "
                         f"Ledger that this unit was {_pretty(status)} before filing.")
        if status == "SELLABLE":
            rule, pct = "Sellable unit", 100
        elif status in HALF_STATUSES:
            since = loss_date - timedelta(days=ORIGIN_LOOKBACK_DAYS)
            came_back = [r for r in returns if r["disposition"] == status and since <= r["event_date"] <= loss_date]
            regraded = [a for a in adjustments if a["disposition"] == status and a["quantity"] > 0
                        and a["reason"] in REGRADE_IN_CODES and since <= a["event_date"] <= loss_date]
            near = [a for a in regraded if a["fc"] == loss_fc
                    and a["event_date"] >= loss_date - timedelta(days=REGRADE_NEAR_DAYS)]
            if regraded and not came_back:
                rule, pct = "Amazon re-graded the unit itself (no customer returns)", 100
            elif near:
                when = max(a["event_date"] for a in near)
                rule, pct = f"Amazon re-graded the unit at {loss_fc} on {when:%b %-d}", 100
            elif came_back:
                rule, pct = "Customer return", 50
            else:
                rule, pct = "Customer damaged/defective, origin not in the ledger", 50
                notes.append("The ledger does not show how this unit became unsellable. Claimed at 50%.")
        else:
            rule, pct = f"{_pretty(status).capitalize()} (not caused by a customer)", 100

    out.update(status_when_lost=status, loss_date=loss_date, loss_fc=loss_fc, rule=rule, pct=pct)
    cost = cost_on(cost_history, loss_date)
    if cost is None:
        return {**out, "verdict": "no_cost_in_notion"}
    per_unit = (cost * pct / 100).quantize(Decimal("0.01"))
    correct = per_unit * qty
    short = correct - paid
    out.update(cost=cost, correct_total=correct, shortfall=short, check_note=" ".join(notes) or None)
    if short / qty < MIN_SHORTFALL:
        return {**out, "verdict": "ok_50" if pct == 50 else "ok"}
    return {**out, "verdict": "claim"}


def _pretty(status: str) -> str:
    return (status or "").lower().replace("_", " ")


def _money(v) -> str:
    return f"${Decimal(v):,.2f}"


def case_wording(c: dict) -> str:
    """The text the VA pastes into the Amazon case. Short on purpose."""
    event = {"Lost_Warehouse": "lost in your fulfillment center", "Damaged_Warehouse": "damaged in your fulfillment center",
             "Lost_Inbound": "lost on the way into your fulfillment center",
             "Damaged_Inbound": "damaged on the way into your fulfillment center",
             "Lost_Outbound": "lost on the way to the customer",
             "CustomerReturn": "reimbursed for a customer return", "CustomerServiceIssue": "reimbursed for a customer service issue",
             "ReturnAdjustment": "reimbursed as a return adjustment"}[c["reason"]]
    order = f" on order {c['amazon_order_id']}" if c.get("amazon_order_id") else ""
    kind = {"CustomerReturn": "a customer return", "CustomerServiceIssue": "a customer service issue",
            "ReturnAdjustment": "a return adjustment"}.get(c["reason"])
    where = f" ({c['loss_fc']}, {c['loss_date']:%b %-d, %Y})" if c.get("loss_fc") else ""
    lines = []
    if c["pct"] == 50:
        lines += [f"Reimbursement ID {c['reimbursement_id']} was paid below 50% of our sourcing cost.",
                  f"SKU: {c['sku']} | FNSKU: {c['fnsku']} | ASIN: {c['asin']}",
                  (f"This reimbursement was for {kind}{order}. The customer returned the unit as "
                   f"{_pretty(c['status_when_lost'])}. We understand only 50% of sourcing cost applies." if kind else
                   f"The unit was in {_pretty(c['status_when_lost'])} condition when it was {event}{where}. "
                   "We understand only 50% of sourcing cost applies."),
                  f"Sourcing cost: {_money(c['cost'])} per unit (invoice attached)",
                  f"50% of sourcing cost: {_money(c['cost'] / 2)} per unit"]
    elif c["status_when_lost"] in HALF_STATUSES:
        lines += [f"Reimbursement ID {c['reimbursement_id']} was paid at 50% of our sourcing cost, but the full cost applies.",
                  f"SKU: {c['sku']} | FNSKU: {c['fnsku']} | ASIN: {c['asin']}",
                  f"The unit was {event}{where} in {_pretty(c['status_when_lost'])} condition. The Inventory Ledger shows "
                  "the fulfillment center moved it to that condition itself. It was not a customer return.",
                  f"Sourcing cost: {_money(c['cost'])} per unit (invoice attached)"]
    else:
        cond = "new" if c["reason"] in INBOUND_REASONS else _pretty(c["status_when_lost"])
        if kind and "no unit coming back" in (c.get("rule") or ""):
            what = (f"This reimbursement was for {kind}{order}. The unit was never returned to inventory, "
                    "so our full sourcing cost applies.")
        elif kind:
            what = (f"This reimbursement was for {kind}{order}. The unit came back as {cond}, which was not "
                    "customer damage, so our full sourcing cost applies.")
        else:
            what = f"The unit was in {cond} condition when it was {event}{where}."
        lines += [f"Reimbursement ID {c['reimbursement_id']} was paid below our sourcing cost.",
                  f"SKU: {c['sku']} | FNSKU: {c['fnsku']} | ASIN: {c['asin']}",
                  what,
                  f"Sourcing cost: {_money(c['cost'])} per unit (invoice attached)"]
    if c.get("steps_version") == "B":
        lines.append("We already submitted this sourcing cost on the Sourcing Cost page. The attached invoice is our proof of cost.")
    if int(c["quantity"]) > 1:
        lines.append(f"Units: {c['quantity']}")
    lines += [f"Amount reimbursed: {_money(c['paid_total'])}",
              f"Correct amount: {_money(c['correct_total'])}",
              f"Shortfall: {_money(c['shortfall'])}",
              f"Please reimburse the remaining {_money(c['shortfall'])}."]
    return "\n".join(lines)


def due_tomorrow(now: datetime | None = None) -> date:
    """Rule: every task is due the day after it is created (business time zone)."""
    return ((now or datetime.now(timezone.utc)).astimezone(BUSINESS_TZ) + timedelta(days=1)).date()


def task_fields(c: dict, uploaded_on: date | None = None) -> dict:
    name = (f"[{c['pct']}%] Sourcing cost dispute | {c['sku']} | Reimb {c['reimbursement_id']} | "
            f"short {_money(c['shortfall'])}")
    if c.get("steps_version") == "B":
        when = f" on {uploaded_on:%b %-d, %Y}" if uploaded_on else ""
        steps = ["STEPS",
                 f"1. Do NOT upload the sourcing cost again. It was already submitted{when}.",
                 "2. Open the reimbursement case with the invoice. Use the amounts and wording below.",
                 "3. Put the Amazon case number in the Case ID field."]
    else:
        steps = ["STEPS",
                 f"1. Upload the sourcing cost for this SKU first: {c['sku']} = {_money(c['cost'])} per unit.",
                 "2. Then open the reimbursement case with the invoice. Use the amounts and wording below.",
                 "3. Put the Amazon case number in the Case ID field."]
    notes = steps + ["",
             f"Amazon's deadline: {c['deadline']:%b %-d, %Y} (60 days after Amazon issued the reimbursement)", "",
             "FOR THE CASE FORM",
             f"Amount reimbursed: {_money(c['paid_total'])}",
             f"Correct amount: {_money(c['correct_total'])}", "",
             f"Why {c['pct']}%: {c['rule']}"]
    if c.get("invoice_url"):
        inv = f"Invoice to attach: #{c.get('invoice_number') or '?'}  {c['invoice_url']}"
        if c.get("invoice_price") is not None and Decimal(c["invoice_price"]) != Decimal(c["cost"]):
            inv += (f"\nNOTE: this invoice shows {_money(c['invoice_price'])} per unit but Notion's cost is "
                    f"{_money(c['cost'])}. Amazon pays against the invoice.")
        notes.append(inv)
    else:
        notes.append("Invoice to attach: none found in Notion for this UPC. Find the supplier invoice before filing.")
    if c.get("check_note"):
        notes += ["", "CHECK FIRST: " + c["check_note"]]
    notes += ["", "CASE WORDING (copy and paste)", case_wording(c)]
    return {"name": name, "notes": "\n".join(notes), "due_on": str(due_tomorrow())}


# ------------------------------------------------------------------- Asana ---

def _asana(method: str, path: str, token: str, **kw) -> dict:
    for attempt in range(5):
        r = requests.request(method, f"{ASANA_API}{path}", headers={"Authorization": f"Bearer {token}"},
                             timeout=60, **kw)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(float(r.headers.get("Retry-After", 3 + attempt * 3)))
            continue
        if not r.ok:
            raise RuntimeError(f"Asana {r.status_code}: {r.text[:300]}")
        return r.json()["data"]
    raise RuntimeError("Asana kept failing; stopped")


def _asana_sections(token: str) -> dict:
    """{100: gid, 50: gid} for the two to-file sections on the discrepancy board."""
    by_name = {x["name"].strip().lower(): x["gid"] for x in _asana("GET", f"/projects/{ASANA_PROJECT_GID}/sections", token)}
    out = {}
    for pct, name in ((100, ASANA_SECTION_100), (50, ASANA_SECTION_50)):
        if name.strip().lower() not in by_name:
            raise RuntimeError(f'Asana section "{name}" not found on project {ASANA_PROJECT_GID}')
        out[pct] = by_name[name.strip().lower()]
    return out


def _needs_invoice_section(token: str) -> str | None:
    """Section for tasks that cannot be filed yet because no invoice PDF was found (None if the board has none)."""
    want = ASANA_SECTION_NEEDS_INVOICE.strip().lower()
    for x in _asana("GET", f"/projects/{ASANA_PROJECT_GID}/sections", token):
        if x["name"].strip().lower() == want:
            return x["gid"]
    return None


def create_task(token: str, sections: dict, c: dict, uploaded_on: date | None = None) -> str:
    data = {**task_fields(c, uploaded_on), "projects": [ASANA_PROJECT_GID],
            "memberships": [{"project": ASANA_PROJECT_GID, "section": sections[50 if c["pct"] == 50 else 100]}]}
    if ASANA_ASSIGNEE_GID and ASANA_ASSIGNEE_GID.lower() != "none":
        data["assignee"] = ASANA_ASSIGNEE_GID
    return _asana("POST", "/tasks", token, json={"data": data})["gid"]


def housekeeping(conn, token: str, sections: dict) -> dict:
    """For every task this agent created that is not finished yet:
    - still waiting to be filed and its due date was changed -> put it back (Asana cannot lock a due date)
    - completed -> remember it, and log the sourcing cost as uploaded if the task asked for that (version A)."""
    to_file = set(sections.values())
    reset = completed = 0
    with conn.cursor() as cur:
        cur.execute("""SELECT reimbursement_id, sku, asana_task_gid, steps_version FROM payment_claims
                       WHERE asana_task_gid IS NOT NULL AND task_completed_at IS NULL""")
        rows = cur.fetchall()
    for rid, sku, gid, version in rows:
        try:
            t = _asana("GET", f"/tasks/{gid}", token,
                       params={"opt_fields": "completed,completed_at,due_on,created_at,memberships.section.gid"})
        except Exception:
            continue   # deleted or unreachable: leave it for the safety net
        if t.get("completed"):
            done = datetime.fromisoformat(t["completed_at"].replace("Z", "+00:00"))
            with conn.cursor() as cur:
                cur.execute("UPDATE payment_claims SET task_completed_at=%s WHERE reimbursement_id=%s AND sku=%s",
                            (done, rid, sku))
                if version == "A":
                    cur.execute("""INSERT INTO sku_cost_uploads (sku, uploaded_on, source) VALUES (%s, %s, %s)
                                   ON CONFLICT (sku) DO NOTHING""",
                                (sku.upper(), done.astimezone(BUSINESS_TZ).date(), f"task {gid} completed"))
            completed += 1
            continue
        in_to_file = any((m.get("section") or {}).get("gid") in to_file for m in t.get("memberships", []))
        want = str(due_tomorrow(datetime.fromisoformat(t["created_at"].replace("Z", "+00:00"))))
        if in_to_file and t.get("due_on") != want:
            _asana("PUT", f"/tasks/{gid}", token, json={"data": {"due_on": want}})
            _asana("POST", f"/tasks/{gid}/stories", token, json={"data": {"text":
                   f"Due date was changed. It is locked to the day after the task was created, so it was put back to {want}."}})
            reset += 1
    return {"due_dates_put_back": reset, "tasks_completed": completed}


def attach_invoices(conn, token: str, upc_ids: dict | None = None, limit: int = 120, sections: dict | None = None) -> dict:
    """Put the supplier invoice PDF, with this item's UPC highlighted, on every open task that lacks one."""
    import invoice_pdf
    out = {"attached": 0, "not_highlighted": 0, "not_found": 0, "errors": 0}
    try:
        needs = _needs_invoice_section(token)
    except Exception:
        needs = None
    with conn.cursor() as cur:
        cur.execute("""SELECT reimbursement_id, sku, asana_task_gid, cost, pct FROM payment_claims
                       WHERE asana_task_gid IS NOT NULL AND task_completed_at IS NULL
                         AND invoice_attached_at IS NULL AND invoice_attach_tries < 30
                       ORDER BY invoice_attach_tries, deadline LIMIT %s""", (limit,))
        rows = cur.fetchall()
    if rows and upc_ids is None:
        _, upc_ids = _load_costs(conn, [r[1] for r in rows])
    for rid, sku, gid, cost, pct in rows:
        _set_step(f"claim {rid}: attaching the invoice")
        note, done, inv = None, False, {}
        try:
            upc_id = (upc_ids or {}).get(sku.upper())
            inv = find_invoice(upc_id, Decimal(cost)) if upc_id else {}
            if inv.get("invoice_number"):
                name, data, source = invoice_pdf.fetch(inv["invoice_number"], inv.get("upc") or "",
                                                       inv.get("_invoice_file_url"), inv.get("_invoice_email_id"))
            elif inv.get("upc"):       # Notion has no invoice line for it: look in the invoices mailbox by UPC
                name, data, source = invoice_pdf.fetch_by_upc(inv["upc"])
                inv["invoice_number"] = re.sub(r"\.pdf$", "", name, flags=re.I)[:60] or "from mailbox"
                inv["invoice_price"] = Decimal(cost)
            else:
                raise LookupError("this SKU has no UPC in Notion, so no invoice can be looked up")
            data, hits = invoice_pdf.highlight(data, inv.get("upc") or "", inv.get("invoice_price"))
            fname = f"Invoice {inv['invoice_number']} - UPC {inv.get('upc') or 'unknown'}.pdf"
            _asana("POST", "/attachments", token, data={"parent": gid},
                   files={"file": (fname, data, "application/pdf")})
            if hits:
                note = f"attached from {source}, UPC highlighted"
                text = (f"Invoice {inv['invoice_number']} is attached with UPC {inv['upc']} highlighted. "
                        "Use this file as the proof of cost.")
            else:
                note = f"attached from {source}, NOT highlighted (UPC text not found in the PDF)"
                text = (f"Invoice {inv['invoice_number']} is attached, but the UPC {inv.get('upc')} could not be "
                        "highlighted automatically (the PDF may be a scan). Mark the line by hand before filing.")
                out["not_highlighted"] += 1
            _asana("POST", f"/tasks/{gid}/stories", token, json={"data": {"text": text}})
            done = True
            out["attached"] += 1
            if needs and sections:      # it may have been parked in "Needs invoice": it can be filed now
                try:
                    t = _asana("GET", f"/tasks/{gid}", token, params={"opt_fields": "memberships.section.gid"})
                    if any((m.get("section") or {}).get("gid") == needs for m in t.get("memberships", [])):
                        _asana("POST", f"/sections/{sections[50 if pct == 50 else 100]}/addTask", token,
                               json={"data": {"task": gid}})
                except Exception:
                    pass
        except invoice_pdf.MailboxNotConnected as exc:      # could not look: try again next run, do not park
            note = str(exc)[:300]
            out["not_found"] += 1
        except LookupError as exc:
            note = str(exc)[:300]
            out["not_found"] += 1
            if needs and sections:      # cannot be filed without proof of cost: park it, visibly
                try:
                    t = _asana("GET", f"/tasks/{gid}", token, params={"opt_fields": "memberships.section.gid"})
                    if any((m.get("section") or {}).get("gid") in set(sections.values()) for m in t.get("memberships", [])):
                        _asana("POST", f"/sections/{needs}/addTask", token, json={"data": {"task": gid}})
                        _asana("POST", f"/tasks/{gid}/stories", token, json={"data": {"text":
                               "No invoice PDF could be found for this item (" + note + "). Add the supplier invoice to "
                               "Notion or attach it here, then move this task back to To file."}})
                except Exception:
                    pass
        except Exception as exc:
            note = f"{type(exc).__name__}: {exc}"[:300]
            out["errors"] += 1
        with conn.cursor() as cur:
            cur.execute("""UPDATE payment_claims SET invoice_attach_note=%s, invoice_attach_tries=invoice_attach_tries+1,
                                  invoice_attached_at = CASE WHEN %s THEN now() ELSE NULL END,
                                  upc = coalesce(upc, %s)
                           WHERE reimbursement_id=%s AND sku=%s""",
                        (note, done, inv.get("upc"), rid, sku))
    return out


# --------------------------------------------------------------------- run ---

CLAIM_COLS = ["reimbursement_id", "sku", "approval_date", "deadline", "reason", "fnsku", "asin", "quantity",
              "paid_total", "status_when_lost", "loss_date", "loss_fc", "rule", "pct", "cost", "correct_total",
              "shortfall", "verdict", "check_note", "invoice_number", "invoice_url", "invoice_price", "steps_version", "upc", "amazon_order_id"]


def _load_groups(conn, today: date) -> list[dict]:
    """One row per reimbursement + SKU from the last 60 days. Amazon's later corrections (GeneralAdjustment rows
    pointing at it) are added to what was paid; reimbursements Amazon reversed are left out."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.reimbursement_id, r.sku, max(r.fnsku), max(r.asin), max(r.reason), min(r.approval_date),
                   sum(r.quantity_reimbursed_cash),
                   sum(r.amount_per_unit * r.quantity_reimbursed_cash)
                     + coalesce((SELECT sum(x.amount_total) FROM fba_reimbursements x
                                 WHERE x.original_reimbursement_id = r.reimbursement_id
                                   AND x.reason = 'GeneralAdjustment' AND x.sku = r.sku), 0),
                   max(r.amazon_order_id)
            FROM fba_reimbursements r
            WHERE r.reason = ANY(%s) AND r.quantity_reimbursed_cash > 0 AND r.approval_date >= %s
              AND r.sku IS NOT NULL AND r.sku <> ''
              AND NOT EXISTS (SELECT 1 FROM fba_reimbursements x
                              WHERE x.original_reimbursement_id = r.reimbursement_id
                                AND x.reason = 'Reimbursement_Reversal')
            GROUP BY r.reimbursement_id, r.sku""",
                    (list(_reasons()), today - timedelta(days=DISPUTE_DAYS)))
        keys = ["reimbursement_id", "sku", "fnsku", "asin", "reason", "approval_date", "quantity", "paid_total",
                "amazon_order_id"]
        return [dict(zip(keys, row)) for row in cur.fetchall()]


def _load_order_returns(conn, order_ids: list[str]) -> dict:
    """{(order_id, SKU): [{return_date, disposition}]} from the Customer Returns report."""
    out = {}
    if not order_ids:
        return out
    with conn.cursor() as cur:
        cur.execute("""SELECT order_id, upper(sku), return_date::date, upper(coalesce(detailed_disposition, ''))
                       FROM fba_customer_returns WHERE order_id = ANY(%s)""", (order_ids,))
        for oid, sku, d, disp in cur.fetchall():
            out.setdefault((oid, sku), []).append({"return_date": d, "disposition": disp})
    return out


def _load_ledger(conn, fnskus: list[str], since: date):
    adj, ret = {}, {}
    with conn.cursor() as cur:
        cur.execute("""SELECT fnsku, event_date, reason, disposition, fulfillment_center, quantity FROM ledger_detail
                       WHERE fnsku = ANY(%s) AND event_date >= %s AND event_type = 'Adjustments'""", (fnskus, since))
        for f, d, reason, disp, fc, q in cur.fetchall():
            adj.setdefault(f, []).append({"event_date": d, "reason": reason, "disposition": disp, "fc": fc, "quantity": q})
        cur.execute("""SELECT fnsku, event_date, disposition, fulfillment_center, quantity FROM ledger_detail
                       WHERE fnsku = ANY(%s) AND event_date >= %s AND event_type = 'CustomerReturns'
                         AND disposition <> 'SELLABLE'""", (fnskus, since))
        for f, d, disp, fc, q in cur.fetchall():
            ret.setdefault(f, []).append({"event_date": d, "disposition": disp, "fc": fc, "quantity": q})
    return adj, ret


def _load_costs(conn, skus: list[str]):
    hist, upc = {}, {}
    with conn.cursor() as cur:
        cur.execute("SELECT sku, valid_from, cost, upc_page_id FROM sku_costs WHERE upper(sku) = ANY(%s)",
                    ([s.upper() for s in skus],))
        for sku, vf, cost, upc_id in cur.fetchall():
            hist.setdefault(sku.upper(), []).append((vf, cost))
            upc[sku.upper()] = upc_id
    return hist, upc


def _save(conn, c: dict):
    vals = [c.get(k) for k in CLAIM_COLS]
    sets = ", ".join(f"{k}=EXCLUDED.{k}" for k in CLAIM_COLS[2:])
    with conn.cursor() as cur:
        cur.execute(f"""INSERT INTO payment_claims ({', '.join(CLAIM_COLS)})
                        VALUES ({', '.join(['%s'] * len(CLAIM_COLS))})
                        ON CONFLICT (reimbursement_id, sku) DO UPDATE SET {sets}, last_seen = now()
                        WHERE payment_claims.asana_task_gid IS NULL""", vals)


def run(dry_run: bool = False, refresh_costs: bool = True, today: date | None = None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    token = os.environ.get("ASANA_TOKEN", "")
    if not token or os.environ.get("PAYMENT_AGENT_PAUSED") == "1":
        dry_run = True
    conn = ledger._db()
    ensure_tables(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE payment_agent_runs SET status='interrupted', finished_at=now() WHERE status='running'")
        cur.execute("INSERT INTO payment_agent_runs (status, dry_run) VALUES ('running', %s) RETURNING id", (dry_run,))
        run_id = cur.fetchone()[0]
    summary = {"dry_run": dry_run, "asana_token_set": bool(token)}
    try:
        if refresh_costs:
            try:
                summary["costs"] = sync_costs(conn)
            except Exception as exc:   # keep going on yesterday's copy rather than skip a day
                summary["costs_error"] = f"{type(exc).__name__}: {exc}"
                try:   # say which Notion connection this is, so the right one can be given access
                    summary["notion_connection_name"] = _notion("GET", "/users/me").get("name")
                except Exception:
                    pass
        _set_step("reading reimbursements and the ledger")
        groups = _load_groups(conn, today)
        fnskus = sorted({g["fnsku"] for g in groups if g["fnsku"]})
        adj, ret = _load_ledger(conn, fnskus, today - timedelta(days=DISPUTE_DAYS + LOSS_LOOKBACK_DAYS + ORIGIN_LOOKBACK_DAYS))
        hist, upc_ids = _load_costs(conn, [g["sku"] for g in groups])
        order_ret = _load_order_returns(conn, sorted({g["amazon_order_id"] for g in groups
                                                      if g.get("amazon_order_id") and g["reason"] in ORDER_REASONS}))
        live = _live_reasons()
        with conn.cursor() as cur:
            cur.execute("SELECT reimbursement_id, sku FROM payment_claims WHERE asana_task_gid IS NOT NULL")
            tasked = set(cur.fetchall())
        filed = _already_filed()
        sections = None
        if not dry_run:
            sections = _asana_sections(token)
            _set_step("checking due dates and finished tasks")
            summary["housekeeping"] = housekeeping(conn, token, sections)
        with conn.cursor() as cur:
            cur.execute("SELECT sku, uploaded_on, rejected FROM sku_cost_uploads")
            uploads = {sku: {"uploaded_on": d, "rejected": r} for sku, d, r in cur.fetchall()}

        counts, to_task, held, held_short = {}, [], {}, Decimal("0")
        for g in groups:
            c = classify(g, adj.get(g["fnsku"], []), ret.get(g["fnsku"], []), hist.get(g["sku"].upper(), []),
                         order_ret.get((g.get("amazon_order_id"), g["sku"].upper()), []))
            if c["verdict"] == "claim" and c["reimbursement_id"] in filed:
                c["verdict"] = "filed_by_hand"
            counts[c["verdict"]] = counts.get(c["verdict"], 0) + 1
            if c["verdict"] == "claim" and c["reason"] not in live and (c["reimbursement_id"], c["sku"]) not in tasked:
                held[c["reason"]] = held.get(c["reason"], 0) + 1      # listed for review, no task yet
                held_short += c["shortfall"]
                _save(conn, c)
            elif c["verdict"] == "claim" and (c["reimbursement_id"], c["sku"]) not in tasked:
                to_task.append(c)
            else:
                _save(conn, c)
        summary.update(checked=len(groups), verdicts=counts, already_tasked=len(tasked),
                       held_for_review=held, held_for_review_shortfall=str(held_short))

        to_task.sort(key=lambda c: (c["deadline"], c["reimbursement_id"]))
        max_tasks = int(os.environ.get("PAYMENT_AGENT_MAX_TASKS", "100"))
        created, errors = 0, []
        for c in to_task:
            _set_step(f"claim {c['reimbursement_id']}: looking up the invoice")
            try:
                upc_id = upc_ids.get(c["sku"].upper())
                if upc_id:
                    c.update(find_invoice(upc_id, c["cost"]))
            except Exception as exc:
                c["check_note"] = ((c.get("check_note") or "") + f" Invoice lookup failed ({type(exc).__name__}).").strip()
            c["steps_version"] = steps_version(c, uploads)
            _save(conn, c)
            if dry_run or created >= max_tasks:
                continue
            try:
                gid = create_task(token, sections, c, (uploads.get(c["sku"].upper()) or {}).get("uploaded_on"))
                with conn.cursor() as cur:
                    cur.execute("""UPDATE payment_claims SET asana_task_gid=%s, task_created_at=now()
                                   WHERE reimbursement_id=%s AND sku=%s""", (gid, c["reimbursement_id"], c["sku"]))
                created += 1
            except Exception as exc:
                errors.append(f"{c['reimbursement_id']}: {exc}")
                if len(errors) >= 3:
                    break
        if not dry_run:
            try:
                summary["invoices"] = attach_invoices(conn, token, upc_ids, sections=sections)
            except Exception as exc:
                summary["invoices_error"] = f"{type(exc).__name__}: {exc}"
        summary.update(new_claims=len(to_task), tasks_created=created,
                       shortfall_new_claims=str(sum((c["shortfall"] for c in to_task), Decimal("0"))),
                       task_errors=errors)
        status = "ok" if not errors else "ok_with_errors"
        with conn.cursor() as cur:
            cur.execute("UPDATE payment_agent_runs SET status=%s, finished_at=now(), summary=%s WHERE id=%s",
                        (status, json.dumps(summary, default=str), run_id))
        return {"run_id": run_id, **summary}
    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute("UPDATE payment_agent_runs SET status='error', finished_at=now(), summary=%s, error=%s WHERE id=%s",
                        (json.dumps(summary, default=str),
                         f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}", run_id))
        raise
    finally:
        _set_step("")
        conn.close()


def _wait_for_syncs(max_wait_s: int = 3600):
    """The cron triggers the syncs and this agent together; let today's data land first."""
    import claims_data
    waited = 0
    time.sleep(20)
    while waited < max_wait_s and (ledger._run_lock.locked() or claims_data._run_lock.locked()):
        _set_step("waiting for today's ledger and reimbursement syncs to finish")
        time.sleep(30)
        waited += 30


def _run_in_background(dry_run: bool, wait: bool):
    try:
        if wait:
            _wait_for_syncs()
        run(dry_run=dry_run)
    except Exception as exc:   # already recorded in payment_agent_runs
        print(f"payment agent failed: {exc}")
    finally:
        _run_lock.release()


# ------------------------------------------------------------------ routes ---

@router.post("/run")
def trigger(dry_run: bool = Query(False), wait_for_syncs: bool = Query(True), x_sync_token: str = Header("")):
    if not ledger.LEDGER_SYNC_TOKEN or x_sync_token != ledger.LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    paused = os.environ.get("PAYMENT_AGENT_PAUSED") == "1"
    dry_run = dry_run or paused          # paused = still work out the list every day, but touch nothing in Asana
    if not _run_lock.acquire(blocking=False):
        return {"started": False, "reason": "the payment accuracy agent is already running"}
    threading.Thread(target=_run_in_background, args=(dry_run, wait_for_syncs), daemon=True).start()
    return {"started": True, "paused": paused, "dry_run": dry_run or not os.environ.get("ASANA_TOKEN")}


@router.get("/status")
def status():
    conn = ledger._db()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT verdict, count(*), coalesce(sum(shortfall) FILTER (WHERE verdict='claim'),0) "
                        "FROM payment_claims GROUP BY 1 ORDER BY 2 DESC")
            verdicts = [{"verdict": v, "count": n, "shortfall": str(s)} for v, n, s in cur.fetchall()]
            cur.execute("SELECT count(*) FROM payment_claims WHERE verdict='claim' AND asana_task_gid IS NULL")
            waiting = cur.fetchone()[0]
            cur.execute("SELECT id, started_at, finished_at, status, dry_run, summary, left(coalesce(error,''),300) "
                        "FROM payment_agent_runs ORDER BY id DESC LIMIT 5")
            runs = [dict(zip(["id", "started_at", "finished_at", "status", "dry_run", "summary", "error"],
                             [str(x) if isinstance(x, datetime) else x for x in r])) for r in cur.fetchall()]
        return {"running": _run_lock.locked(), "current_step": _step, "asana_token_set": bool(os.environ.get("ASANA_TOKEN")),
                "claims_waiting_for_a_task": waiting, "verdicts": verdicts, "recent_runs": runs}
    finally:
        conn.close()
