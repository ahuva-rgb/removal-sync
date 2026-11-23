"""Case monitor for the Reimbursement Case Discrepancy board.

What it does every day, for each open task the payment agent created:
  1. Case ID entered           -> move to "Submitted".
  2. Amazon paid the shortfall -> move to "Approved" and complete the task.
  3. Amazon paid part of it    -> complete the task; the payment agent opens a new task for the rest.
  4. Declined, or 5 days with no payment:
        Amazon has NOT approved our sourcing cost -> "Fix sourcing cost"   (no SAS case)
        Amazon HAS approved our sourcing cost     -> "SAS case needed"
  5. SAS Case ID entered       -> "SAS case submitted"; declined again or 7 quiet days -> "Denied".

It never opens a case. It runs in test mode (plans only, touches nothing) unless CASE_MONITOR_LIVE=1.

Where the facts come from:
  payment        Amazon's Reimbursements report (already in the database)
  decline        "Re-Evaluation Reimbursement Request Update" emails in cases@ (matched by FNSKU)
  approved cost  Amazon's "Manage Your Sourcing Cost" export, attached twice a week to a task in the
                 "Sourcing cost export (Mon + Thu)" section
"""
from __future__ import annotations

import base64
import csv
import io
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import requests

ASANA_API = "https://app.asana.com/api/1.0"
PROJECT = os.environ.get("ASANA_PROJECT_GID", "1220818804510392")
ASSIGNEE = os.environ.get("ASANA_ASSIGNEE_GID", "1207717933464435")
CASES_MAILBOX = os.environ.get("CASES_MAILBOX", "cases@gefenoptical.com")
TZ = ZoneInfo("America/New_York")
QUIET_DAYS = int(os.environ.get("CASE_MONITOR_QUIET_DAYS", "5"))        # no payment, no reply -> treat as not paid
SAS_QUIET_DAYS = int(os.environ.get("CASE_MONITOR_SAS_QUIET_DAYS", "7"))
PAID_TOLERANCE = Decimal("1.00")                                         # per unit, same as the payment agent
EXPORT_DAYS = (0, 3)                                                     # Monday, Thursday

SEC = {  # key -> (how to recognise the section by name, name to create if it is missing)
    "to_file_100": ("to file: 100", None),
    "to_file_50": ("to file: 50", None),
    "needs_invoice": ("needs invoice", None),
    "submitted": ("submitted", None),
    "review": ("amazon reply", None),
    "approved": ("approved", None),
    "denied": ("denied", None),
    "fix_cost": ("fix sourcing cost", "Fix sourcing cost (no SAS case)"),
    "sas_needed": ("sas case needed", "SAS case needed"),
    "sas_submitted": ("sas case submitted", "SAS case submitted: agent is watching"),
    "export": ("sourcing cost export", "Sourcing cost export (Mon + Thu)"),
}


def live() -> bool:
    return os.environ.get("CASE_MONITOR_LIVE") == "1"


# ---------------------------------------------------------------- tables ---
def ensure_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS amazon_sourcing_costs (
            fnsku text PRIMARY KEY, asin text, estimated numeric(14,2), approved numeric(14,2),
            source text, updated_at text, file_name text, loaded_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS amazon_cost_files (
            attachment_gid text PRIMARY KEY, task_gid text, name text, rows int,
            loaded_at timestamptz NOT NULL DEFAULT now());
        CREATE TABLE IF NOT EXISTS case_emails (
            message_id text PRIMARY KEY, fnsku text, kind text, received_at timestamptz, subject text,
            used_for_task text, seen_at timestamptz NOT NULL DEFAULT now());
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS case_id text;
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS sas_case_id text;
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS submitted_at timestamptz;
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS sas_submitted_at timestamptz;
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS monitor_state text;
        ALTER TABLE payment_claims ADD COLUMN IF NOT EXISTS monitor_note text;
    """)
    cur.execute("SELECT count(*) FROM amazon_sourcing_costs")
    if cur.fetchone()[0] == 0:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "amazon_sourcing_costs_seed.csv")
        if os.path.exists(path):
            with open(path, newline="") as f:
                _store_costs(cur, list(csv.DictReader(f)), "seed file (export of 2026-11-23)")
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
    if cur.fetchone():
        cur.execute("GRANT SELECT ON amazon_sourcing_costs, amazon_cost_files, case_emails TO ledger_reader")


def _num(v):
    try:
        return Decimal(str(v)).quantize(Decimal("0.01")) if v not in ("", None, "-") else None
    except Exception:
        return None


def _store_costs(cur, rows: list[dict], file_name: str) -> int:
    n = 0
    for r in rows:
        fnsku = (r.get("fnsku") or "").strip()
        if not fnsku:
            continue
        cur.execute("""INSERT INTO amazon_sourcing_costs (fnsku, asin, estimated, approved, source, updated_at, file_name)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (fnsku) DO UPDATE SET asin=EXCLUDED.asin, estimated=EXCLUDED.estimated,
                         approved=EXCLUDED.approved, source=EXCLUDED.source, updated_at=EXCLUDED.updated_at,
                         file_name=EXCLUDED.file_name, loaded_at=now()""",
                    (fnsku, r.get("asin"), _num(r.get("estimated")), _num(r.get("approved")),
                     (r.get("source") or "").strip().upper(), str(r.get("updated_at") or ""), file_name))
        n += 1
    return n


def parse_export(data: bytes) -> list[dict]:
    """Rows of Amazon's sourcing cost export (.xlsx or .csv) as dicts with our column names."""
    want = {"asin": "asin", "fnsku": "fnsku", "amazon estimated cost": "estimated", "latest approved cost": "approved",
            "source of latest approved cost": "source", "date of last updated cost": "updated_at"}
    if data[:2] == b"PK":
        import openpyxl
        ws = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True).worksheets[0]
        it = ws.iter_rows(values_only=True)
    else:
        it = csv.reader(io.StringIO(data.decode("utf-8-sig", "replace")))
    header = [str(h or "").strip().lower() for h in next(it)]
    idx = {want[h]: i for i, h in enumerate(header) if h in want}
    if not {"fnsku", "approved", "source"} <= set(idx):
        raise ValueError("this file is not the Manage Your Sourcing Cost export (columns Fnsku / Latest Approved Cost "
                         "/ Source of Latest Approved Cost not found)")
    return [{k: (row[i] if i < len(row) else None) for k, i in idx.items()} for row in it if row]


def amazon_costs(conn) -> dict:
    """{fnsku: {'approved': Decimal|None, 'source': 'SELLER'|'AMAZON', 'estimated': Decimal|None}}"""
    with conn.cursor() as cur:
        cur.execute("SELECT fnsku, approved, source, estimated FROM amazon_sourcing_costs")
        return {f: {"approved": a, "source": s, "estimated": e} for f, a, s, e in cur.fetchall()}


def seller_approved(costs: dict, fnsku: str) -> Decimal | None:
    """The sourcing cost Amazon has approved from us for this FNSKU (None when it only has its own estimate)."""
    c = costs.get(fnsku or "")
    return c["approved"] if c and c["source"] == "SELLER" and c["approved"] is not None else None


# ----------------------------------------------------------------- Asana ---
def _asana(method: str, path: str, token: str, **kw):
    r = requests.request(method, f"{ASANA_API}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=60, **kw)
    if not r.ok:
        raise RuntimeError(f"Asana {r.status_code}: {r.text[:300]}")
    return r.json()


def _asana_all(path: str, token: str, params: dict) -> list[dict]:
    out, params = [], {**params, "limit": 100}
    while True:
        j = _asana("GET", path, token, params=params)
        out += j["data"]
        nxt = (j.get("next_page") or {}).get("offset")
        if not nxt:
            return out
        params["offset"] = nxt


def sections(token: str, create: bool) -> dict:
    """{key: gid}. Sections the monitor needs are created only when it is live."""
    have = _asana("GET", f"/projects/{PROJECT}/sections", token)["data"]
    out = {}
    for key, (needle, new_name) in SEC.items():
        gid = next((s["gid"] for s in have if s["name"].strip().lower().startswith(needle)), None)
        if gid is None and new_name and create:
            gid = _asana("POST", f"/projects/{PROJECT}/sections", token, json={"data": {"name": new_name}})["data"]["gid"]
        out[key] = gid
    return out


def refresh_costs_from_asana(conn, token: str, secs: dict) -> dict:
    """Load any sourcing cost export attached to a task in the export section that we have not read yet."""
    if not secs.get("export"):
        return {"export_section": "missing"}
    loaded, errors = [], []
    since = (datetime.now(timezone.utc) - timedelta(days=21)).isoformat()
    tasks = _asana_all(f"/sections/{secs['export']}/tasks", token, {"opt_fields": "name,created_at"})
    with conn.cursor() as cur:
        cur.execute("SELECT attachment_gid FROM amazon_cost_files")
        seen = {r[0] for r in cur.fetchall()}
    files = []
    for t in tasks:
        if t.get("created_at", "") < since:
            continue
        for a in _asana_all("/attachments", token, {"parent": t["gid"], "opt_fields": "name,created_at,download_url"}):
            if a["gid"] not in seen and a.get("download_url"):
                files.append((a.get("created_at", ""), a, t["gid"]))
    for _, a, task_gid in sorted(files, key=lambda x: x[0]):      # oldest first so the newest file wins
        try:
            rows = parse_export(requests.get(a["download_url"], timeout=120).content)
            with conn.cursor() as cur:
                n = _store_costs(cur, rows, a["name"])
                cur.execute("INSERT INTO amazon_cost_files (attachment_gid, task_gid, name, rows) VALUES (%s,%s,%s,%s)",
                            (a["gid"], task_gid, a["name"], n))
            loaded.append({"file": a["name"], "rows": n})
        except Exception as exc:
            errors.append(f"{a.get('name')}: {exc}")
            with conn.cursor() as cur:   # do not retry a file that cannot be read; say so on the task once
                cur.execute("INSERT INTO amazon_cost_files (attachment_gid, task_gid, name, rows) VALUES (%s,%s,%s,0)",
                            (a["gid"], task_gid, a["name"]))
            try:
                _asana("POST", f"/tasks/{task_gid}/stories", token, json={"data": {"text":
                       f"The agent could not read \"{a.get('name')}\": {exc}. Please attach the Excel export from "
                       "Manage Your Sourcing Cost without editing it."}})
            except Exception:
                pass
    with conn.cursor() as cur:
        cur.execute("SELECT max(loaded_at), count(*) FROM amazon_sourcing_costs")
        last, n = cur.fetchone()
    return {"files_loaded": loaded, "errors": errors, "costs_on_file": n, "last_loaded": str(last)}


def ensure_export_task(token: str, secs: dict, now: datetime | None = None) -> str | None:
    """On Monday and Thursday, make sure Azel has a task to upload the export."""
    now = (now or datetime.now(timezone.utc)).astimezone(TZ)
    if now.weekday() not in EXPORT_DAYS or not secs.get("export"):
        return None
    name = f"Upload Amazon sourcing cost export ({now:%a %b %-d})"
    week_ago = (now - timedelta(days=3)).astimezone(timezone.utc).isoformat()
    for t in _asana_all(f"/sections/{secs['export']}/tasks", token, {"opt_fields": "name,created_at,due_on"}):
        if t["name"] == name or t.get("due_on") == str(now.date()) or \
                (t.get("created_at", "") > week_ago and now.strftime("%b %-d") in t["name"]):
            return None
    data = {"name": name, "projects": [PROJECT], "due_on": str(now.date()),
            "memberships": [{"project": PROJECT, "section": secs["export"]}],
            "notes": "Twice a week, Monday and Thursday.\n\n"
                     "1. In Seller Central, open Manage Your Sourcing Cost.\n"
                     "2. Download the full export (the Excel file with the columns Asin, Fnsku, Amazon Estimated Cost, "
                     "Latest Approved Cost, Source of Latest Approved Cost).\n"
                     "3. Attach the file to this task. Do not rename or edit it.\n"
                     "4. Mark the task complete.\n\n"
                     "The reimbursement agent reads this file to see which sourcing costs Amazon has approved. That "
                     "decides whether an unpaid case needs a sourcing cost update or an SAS case."}
    if ASSIGNEE and ASSIGNEE.lower() != "none":
        data["assignee"] = ASSIGNEE
    return _asana("POST", "/tasks", token, json={"data": data})["data"]["gid"]


# ---------------------------------------------------------------- emails ---
_FNSKU = re.compile(r"FNSKU\s+([A-Z0-9]{10})")


def read_case_emails(conn) -> dict:
    """Record Amazon's re-evaluation replies that reached the cases mailbox. Returns counts (or why it could not)."""
    try:
        import google_mail
        if not google_mail.is_connected(CASES_MAILBOX):
            return {"mailbox": "not connected"}
        ids = google_mail.search(CASES_MAILBOX, 'subject:"Reimbursement Request Update" newer_than:45d', 300)
    except Exception as exc:
        return {"mailbox_error": f"{type(exc).__name__}: {exc}"}
    with conn.cursor() as cur:
        cur.execute("SELECT message_id FROM case_emails")
        seen = {r[0] for r in cur.fetchall()}
    new = 0
    for mid in ids:
        if mid in seen:
            continue
        try:
            m = google_mail._gmail(CASES_MAILBOX, f"/messages/{mid}", {"format": "full"})
        except Exception:
            continue
        text = m.get("snippet", "") + " " + _body_text(m.get("payload", {}))
        hit = _FNSKU.search(text)
        low = text.lower()
        kind = "declined" if "unable to make any additional adjustments" in low else \
               "approved" if ("has been approved" in low or "we have issued" in low or "additional reimbursement" in low) \
               else "other"
        subject = next((h["value"] for h in m.get("payload", {}).get("headers", []) if h["name"].lower() == "subject"), "")
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO case_emails (message_id, fnsku, kind, received_at, subject)
                           VALUES (%s,%s,%s,to_timestamp(%s),%s) ON CONFLICT DO NOTHING""",
                        (mid, hit.group(1) if hit else None, kind, int(m.get("internalDate", 0)) / 1000, subject[:200]))
        new += 1
    return {"emails_found": len(ids), "new": new}


def _body_text(part: dict) -> str:
    out = []
    data = (part.get("body") or {}).get("data")
    if data and part.get("mimeType", "").startswith("text/"):
        try:
            out.append(re.sub(r"<[^>]+>", " ", base64.urlsafe_b64decode(data.encode()).decode("utf-8", "replace")))
        except Exception:
            pass
    for p in part.get("parts", []) or []:
        out.append(_body_text(p))
    return " ".join(out)


# -------------------------------------------------------------- decision ---
def decide(t: dict, now: datetime) -> dict | None:
    """What to do with one open task. Pure function so it can be tested.

    t: section (key from SEC or None), case_id, sas_case_id, pct, quantity,
       claim_cost (cost per unit the task claims), correct_total (what the task says Amazon owes in total),
       paid_at_task (paid when the task was made), paid_now (None when the reimbursement is no longer in the data),
       submitted_at, sas_submitted_at, declined_at (latest decline email for this task), other_reply_at,
       approved_cost (Amazon-approved seller cost for the FNSKU, or None)
    Returns {'action', 'to' (section key), 'why', ...} or None when there is nothing to do.
    """
    qty = max(int(t.get("quantity") or 1), 1)
    paid_now = t.get("paid_now")
    if paid_now is not None and t.get("correct_total") is not None:
        remaining = Decimal(t["correct_total"]) - Decimal(paid_now)
        extra = Decimal(paid_now) - Decimal(t.get("paid_at_task") or 0)
        if remaining / qty < PAID_TOLERANCE:
            return {"action": "paid", "to": "approved", "complete": True,
                    "why": f"Amazon has now paid ${Decimal(paid_now):,.2f} in total for this reimbursement, which covers "
                           f"the ${Decimal(t['correct_total']):,.2f} owed. Nothing more to do."}
        if extra >= Decimal("0.01") and (t.get("case_id") or t.get("sas_case_id")):
            return {"action": "paid_short", "to": "approved", "complete": True, "retask": True,
                    "why": f"Amazon paid ${extra:,.2f} more, but it is still short ${remaining:,.2f}. This task is closed "
                           "and a new task for the remaining amount will be created."}
    sec, case_id, sas_id = t.get("section"), t.get("case_id"), t.get("sas_case_id")
    if sec in ("approved", "denied", "review", "export"):
        return None
    cost_ok = t.get("approved_cost") is not None and t.get("claim_cost") is not None and \
        Decimal(t["approved_cost"]) >= Decimal(t["claim_cost"]) - Decimal("0.02")

    if sas_id:                                                    # ---- SAS stage
        if sec != "sas_submitted":
            return {"action": "sas_submitted", "to": "sas_submitted", "stamp": "sas_submitted_at",
                    "why": f"SAS case {sas_id} entered. The agent is watching for the payment."}
        since = t.get("sas_submitted_at")
        declined = t.get("declined_at") and since and t["declined_at"] > since
        quiet = since and now - since >= timedelta(days=SAS_QUIET_DAYS)
        if declined or quiet:
            return {"action": "denied", "to": "denied",
                    "why": ("Amazon declined again after the SAS case." if declined else
                            f"No payment {SAS_QUIET_DAYS} days after the SAS case.") + " Needs a decision from Cassie."}
        return None

    if sec == "fix_cost":                                         # ---- waiting for the cost to be approved
        if cost_ok:
            return {"action": "cost_now_approved", "to": "to_file_50" if t.get("pct") == 50 else "to_file_100",
                    "clear_case_id": True, "due": 1,
                    "why": f"Amazon has now approved the sourcing cost (${Decimal(t['approved_cost']):,.2f}). Open a NEW "
                           f"case for this reimbursement and enter the new Case ID."
                           + (f" The earlier case was {case_id}." if case_id else "")}
        return None

    if sec == "sas_needed":
        return None                                               # waiting for Azel to enter the SAS Case ID
    if not case_id and sec != "submitted":
        return None                                               # still waiting to be filed
    if sec != "submitted":
        return {"action": "submitted", "to": "submitted", "stamp": "submitted_at", "due": QUIET_DAYS,
                "why": f"Case {case_id} entered. The agent is watching for the payment; nobody needs to check on it."}
    since = t.get("submitted_at")
    declined = t.get("declined_at") and (not since or t["declined_at"] >= since - timedelta(hours=12))
    quiet = since and now - since >= timedelta(days=QUIET_DAYS)
    if not (declined or quiet):
        if t.get("other_reply_at") and (not since or t["other_reply_at"] >= since - timedelta(hours=12)):
            return {"action": "reply_to_read", "to": "review",
                    "why": "Amazon sent a reply for this FNSKU that is not a standard decline. Please read it."}
        return None
    reason = "Amazon declined the case" if declined else f"No payment {QUIET_DAYS} days after the case was opened"
    if cost_ok:
        return {"action": "sas_needed", "to": "sas_needed", "due": 1, "assign": True,
                "why": f"{reason}, and Amazon has already approved our sourcing cost "
                       f"(${Decimal(t['approved_cost']):,.2f}). Open an SAS case and enter the SAS Case ID."}
    have = (f"Amazon's approved cost is ${Decimal(t['approved_cost']):,.2f}, below the "
            f"${Decimal(t['claim_cost']):,.2f} this task claims" if t.get("approved_cost") is not None
            else "Amazon has no approved sourcing cost from us for this FNSKU")
    return {"action": "fix_cost", "to": "fix_cost", "due": 1, "assign": True,
            "why": f"{reason}. {have}. Do NOT open an SAS case. Upload the sourcing cost with the attached invoice on "
                   "Manage Your Sourcing Cost. The agent moves this task back when Amazon approves the cost."}


# ------------------------------------------------------------------- run ---
def _field(task: dict, starts: str) -> tuple[str | None, str | None]:
    for f in task.get("custom_fields", []):
        if (f.get("name") or "").strip().lower().startswith(starts) and f.get("enabled", True) is not False:
            v = f.get("text_value") or f.get("display_value")
            return f["gid"], (str(v).strip() or None) if v else None
    return None, None


def prepare(conn, token: str, dry_run: bool, now: datetime | None = None) -> dict:
    """Run first: read any new sourcing cost export, and on Monday/Thursday ask for the next one."""
    out = {}
    secs = sections(token, create=False)
    try:
        out["sourcing_costs"] = refresh_costs_from_asana(conn, token, secs)
    except Exception as exc:
        out["sourcing_costs_error"] = f"{type(exc).__name__}: {exc}"
    with conn.cursor() as cur:   # a file already loaded today (business time) means nobody needs to be asked today
        cur.execute("SELECT (max(loaded_at) AT TIME ZONE 'America/New_York')::date = "
                    "(now() AT TIME ZONE 'America/New_York')::date FROM amazon_sourcing_costs")
        fresh_today = bool(cur.fetchone()[0])
    if not dry_run and not fresh_today:
        try:
            made = ensure_export_task(token, secs, now)
            if made:
                out["export_task_created"] = made
        except Exception as exc:
            out["export_task_error"] = f"{type(exc).__name__}: {exc}"
    return out


def monitor(conn, token: str, fresh: dict, dry_run: bool, now: datetime | None = None) -> dict:
    """fresh: {(reimbursement_id, sku): today's classify() result} for everything still in the 60-day window."""
    now = now or datetime.now(timezone.utc)
    apply = live() and not dry_run
    secs = sections(token, create=apply)
    out = {"live": apply}
    out["emails"] = read_case_emails(conn)
    costs = amazon_costs(conn)
    by_gid = {gid: key for key, gid in secs.items() if gid}

    with conn.cursor() as cur:
        cur.execute("""SELECT reimbursement_id, sku, fnsku, pct, quantity, cost, correct_total, paid_total, asana_task_gid,
                              submitted_at, sas_submitted_at
                       FROM payment_claims WHERE asana_task_gid IS NOT NULL AND task_completed_at IS NULL""")
        cols = ["reimbursement_id", "sku", "fnsku", "pct", "quantity", "claim_cost", "correct_total", "paid_at_task",
                "gid", "submitted_at", "sas_submitted_at"]
        claims = {r[8]: dict(zip(cols, r)) for r in cur.fetchall()}
        cur.execute("""SELECT message_id, fnsku, kind, received_at, used_for_task FROM case_emails
                       WHERE fnsku IS NOT NULL ORDER BY received_at""")
        emails = [dict(zip(["id", "fnsku", "kind", "at", "used"], r)) for r in cur.fetchall()]

    tasks = _asana_all(f"/projects/{PROJECT}/tasks", token, {
        "completed_since": "now",
        "opt_fields": "name,memberships.section.gid,memberships.project.gid,custom_fields.name,"
                      "custom_fields.text_value,custom_fields.display_value,custom_fields.enabled"})
    actions, counts = [], {}
    for task in sorted(tasks, key=lambda x: x["gid"]):
        c = claims.get(task["gid"])
        if not c:
            continue
        sec_gid = next((m["section"]["gid"] for m in task.get("memberships", [])
                        if (m.get("project") or {}).get("gid") == PROJECT and m.get("section")), None)
        case_gid, case_id = _field(task, "case id")
        sas_gid, sas_id = _field(task, "sas case")
        f = fresh.get((c["reimbursement_id"], c["sku"]))
        mine = [e for e in emails if e["fnsku"] == c["fnsku"] and e["used"] in (None, task["gid"])]
        declined = next((e for e in reversed(mine) if e["kind"] == "declined"), None)
        other = next((e for e in reversed(mine) if e["kind"] != "declined"), None)
        t = {**c, "section": by_gid.get(sec_gid), "case_id": case_id, "sas_case_id": sas_id,
             "paid_now": f.get("paid_total") if f else None,
             "declined_at": declined["at"] if declined else None, "other_reply_at": other["at"] if other else None,
             "approved_cost": seller_approved(costs, c["fnsku"])}
        if t["section"] == "submitted" and not c["submitted_at"]:
            t["submitted_at"] = now      # first time the agent sees it in Submitted: the clock starts today
            with conn.cursor() as cur:
                cur.execute("""UPDATE payment_claims SET submitted_at=%s, case_id=coalesce(%s, case_id)
                               WHERE reimbursement_id=%s AND sku=%s""", (now, case_id, c["reimbursement_id"], c["sku"]))
        d = decide(t, now)
        if not d:
            continue
        counts[d["action"]] = counts.get(d["action"], 0) + 1
        actions.append({"task": task["name"], "gid": task["gid"], "from": by_gid.get(sec_gid) or "other",
                        "to": d["to"], "action": d["action"], "why": d["why"]})
        if d["action"] in ("sas_needed", "fix_cost", "denied") and declined:
            declined["used"] = task["gid"]          # one decline email answers one task (same FNSKU, several tasks)
        if not apply:
            continue
        try:
            _apply(conn, token, secs, task["gid"], c, d, case_id, sas_id, case_gid, declined, now)
        except Exception as exc:
            actions[-1]["error"] = f"{type(exc).__name__}: {exc}"
    out.update(actions=actions, counts=counts, tasks_watched=len(claims))
    return out


def _apply(conn, token, secs, gid, c, d, case_id, sas_id, case_field_gid, declined, now):
    if not secs.get(d["to"]):
        raise RuntimeError(f"section for '{d['to']}' not found on the board")
    _asana("POST", f"/sections/{secs[d['to']]}/addTask", token, json={"data": {"task": gid}})
    data = {}
    if d.get("due"):
        data["due_on"] = str((now.astimezone(TZ) + timedelta(days=d["due"])).date())
    if d.get("assign") and ASSIGNEE and ASSIGNEE.lower() != "none":
        data["assignee"] = ASSIGNEE
    if d.get("clear_case_id") and case_field_gid:
        data["custom_fields"] = {case_field_gid: ""}
    if d.get("complete"):
        data["completed"] = True
    if data:
        _asana("PUT", f"/tasks/{gid}", token, json={"data": data})
    _asana("POST", f"/tasks/{gid}/stories", token, json={"data": {"text": d["why"]}})
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE payment_claims SET monitor_state=%s, monitor_note=%s, case_id=coalesce(%s, case_id),
                          sas_case_id=coalesce(%s, sas_case_id)
                          {', ' + d['stamp'] + '=now()' if d.get('stamp') in ('submitted_at', 'sas_submitted_at') else ''}
                          {', task_completed_at=now()' if d.get('complete') else ''}
                        WHERE reimbursement_id=%s AND sku=%s""",
                    (d["action"], d["why"], case_id, sas_id, c["reimbursement_id"], c["sku"]))
        if d.get("retask"):     # let the payment agent open a fresh task for what is still owed
            cur.execute("""UPDATE payment_claims SET asana_task_gid=NULL, task_created_at=NULL, task_completed_at=NULL,
                             invoice_attached_at=NULL, invoice_attach_tries=0, submitted_at=NULL, sas_submitted_at=NULL
                           WHERE reimbursement_id=%s AND sku=%s""", (c["reimbursement_id"], c["sku"]))
        if declined and d["action"] in ("sas_needed", "fix_cost", "denied"):
            cur.execute("UPDATE case_emails SET used_for_task=%s WHERE message_id=%s", (gid, declined["id"]))
