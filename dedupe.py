"""
Removal box de-duplication.

Background
----------
For a period, two writers populated the Amazon Removal Warehouse Box Items
database at the same time: the old Relay flow and the new materialiser worker.
Where both ran over the same shipment line, the box ended up holding two item
records for every unit Amazon actually shipped.

This module reconciles Notion against the Amazon truth already held in the
"Amazon Removal Report Lines" staging database, and archives the surplus.

Safety properties
-----------------
* Amazon is the only authority for how many units a box should hold. A
  (tracking, SKU) pair with no staging line is never touched.
* Archiving only ever removes rows *down to* the Amazon quantity, never below.
* The keep-rule preserves human work first: frame status set, a processed-by
  person, or a link to an RA prep / stock / don't-sell / warranty record.
  Among equals the materialiser row wins, then the oldest.
* A scan produces an immutable plan. Applying archives exactly the page IDs in
  that plan - it never recomputes, so what you reviewed is what gets removed.
* Rows carrying human work are refused unless force=true is passed explicitly.
* Archiving sends pages to Notion's trash, where they are restorable for 30
  days. Nothing is hard-deleted.
* Every mutating call requires DEDUPE_KEY. Without it set, apply is disabled.
"""

from __future__ import annotations

import hmac
import os
import re
import threading
import time
import uuid
from collections import defaultdict

import requests
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/dedupe", tags=["dedupe"])

NOTION_API = "https://api.notion.com/v1"
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
DEDUPE_KEY = os.environ.get("DEDUPE_KEY", "")

# Databases in the Amazon Removals Flow workspace.
DB_BOXES = os.environ.get("DEDUPE_DB_BOXES", "2990ab7b-dc07-8020-83ad-f29731302f38")
DB_ITEMS = os.environ.get("DEDUPE_DB_ITEMS", "2990ab7b-dc07-8022-9aaa-da3bbf4840c5")
DB_STAGING = os.environ.get("DEDUPE_DB_STAGING", "3ce0ab7b-dc07-810b-8089-c524afa230df")

# Refuse to archive more than this in one pass unless overridden, so a bad
# scan can never quietly empty the database.
DEFAULT_MAX_ARCHIVE = 2500

WORK_RELATIONS = ["RA Prep Boxes", "Stock Boxes", "Don’t Sell Boxes", "Case & Warranty Forms"]
PLACEHOLDER_FRAME = {None, "", "Please Select Frame Status", "Option to Select Frame Status"}

ITEM_TITLE_RE = re.compile(r"^(.*?)\s+-\s+Item\s+\d+/\d+$")

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


# ------------------------------------------------------------------ helpers

def _auth(key: str | None) -> None:
    if not DEDUPE_KEY:
        raise HTTPException(
            503,
            "DEDUPE_KEY is not set on this service, so de-duplication is disabled. "
            "Add it in the Render dashboard (Environment -> Add Environment Variable) "
            "and redeploy.",
        )
    if not key or not hmac.compare_digest(key, DEDUPE_KEY):
        raise HTTPException(401, "Bad or missing key.")


def _headers(version: str) -> dict:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": version,
        "Content-Type": "application/json",
    }


def _post(path: str, body: dict, version: str) -> dict:
    last = None
    for attempt in range(6):
        r = requests.post(f"{NOTION_API}{path}", headers=_headers(version), json=body, timeout=60)
        if r.status_code in (429, 502, 503, 504):
            time.sleep(min(2 ** attempt, 16))
            last = r
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"{r.status_code} on {path}: {r.text[:300]}")
        return r.json()
    raise RuntimeError(f"gave up on {path}: {last.status_code if last else '?'}")


def _scan_db(db_id: str, reduce_fn, progress=None) -> None:
    """Page through a database, handing each page to reduce_fn.

    Pages are reduced to small tuples as they arrive rather than accumulated,
    so a 30k-row database does not have to fit in memory on a small instance.
    Works against either the data_sources or the databases query endpoint.
    """
    endpoints = [
        (f"/data_sources/{db_id}/query", "2025-09-03"),
        (f"/databases/{db_id}/query", "2022-06-28"),
    ]
    chosen = None
    cursor = None
    seen = 0
    while True:
        body: dict = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        if chosen is None:
            errors = []
            for path, version in endpoints:
                try:
                    page = _post(path, body, version)
                    chosen = (path, version)
                    break
                except Exception as exc:  # try the other dialect
                    errors.append(str(exc))
            if chosen is None:
                raise RuntimeError(
                    f"Could not query {db_id}. Check the integration is shared into "
                    f"that database. Errors: {' | '.join(errors)}"
                )
        else:
            page = _post(chosen[0], body, chosen[1])
        for row in page.get("results", []):
            reduce_fn(row)
            seen += 1
        if progress:
            progress(seen)
        if not page.get("has_more"):
            return
        cursor = page.get("next_cursor")
        time.sleep(0.34)


def _text(page: dict, prop: str) -> str:
    v = page.get("properties", {}).get(prop) or {}
    parts = v.get("title") or v.get("rich_text") or []
    return "".join(t.get("plain_text", "") for t in parts).strip()


def _number(page: dict, prop: str) -> float:
    v = page.get("properties", {}).get(prop) or {}
    return v.get("number") or 0


def _select(page: dict, prop: str):
    v = page.get("properties", {}).get(prop) or {}
    s = v.get("select") or v.get("status")
    return (s or {}).get("name")


def _relation_ids(page: dict, prop: str) -> list:
    v = page.get("properties", {}).get(prop) or {}
    return [r.get("id") for r in v.get("relation", [])]


def _checkbox(page: dict, prop: str) -> bool:
    v = page.get("properties", {}).get(prop) or {}
    return bool(v.get("checkbox"))


def _work_score(page: dict) -> int:
    """How much human processing an item carries. Higher survives."""
    score = 0
    if _select(page, "Frame Status") not in PLACEHOLDER_FRAME:
        score += 1
    if (page.get("properties", {}).get("Processed By") or {}).get("people"):
        score += 1
    for rel in WORK_RELATIONS:
        if _relation_ids(page, rel):
            score += 1
    return score


# --------------------------------------------------------------------- scan

def _run_scan(job_id: str) -> None:
    job = _jobs[job_id]

    def note(stage: str, **kw):
        with _jobs_lock:
            job["stage"] = stage
            job.update(kw)

    try:
        # 1. Amazon truth: units shipped per (tracking, SKU).
        expected: dict[tuple, int] = defaultdict(int)
        staging_seen = [0]

        def take_line(row):
            trk = _text(row, "Tracking Number")
            sku = _text(row, "SKU")
            qty = _number(row, "Shipped Quantity")
            if trk and sku:
                expected[(trk, sku)] += int(qty)

        note("reading Amazon report lines")
        _scan_db(DB_STAGING, take_line, lambda n: note("reading Amazon report lines", staging_rows=n))
        staging_seen[0] = sum(expected.values())

        # 2. Box page id -> tracking number.
        boxes: dict[str, str] = {}

        def take_box(row):
            trk = _text(row, "Box Tracking Number")
            if trk:
                boxes[row["id"]] = trk

        note("reading boxes", amazon_pairs=len(expected))
        _scan_db(DB_BOXES, take_box, lambda n: note("reading boxes", box_rows=n))

        # 3. Items, reduced to what the keep-rule needs.
        grouped: dict[tuple, list] = defaultdict(list)
        unparsed = [0]

        def take_item(row):
            title = _text(row, "Box Item")
            m = ITEM_TITLE_RE.match(title)
            if not m:
                unparsed[0] += 1
                return
            sku = m.group(1).strip()
            rec = (
                -_work_score(row),                        # work first
                not _checkbox(row, "Created By Materialiser"),
                row.get("created_time", ""),              # then oldest
                row["id"],
                row.get("url", ""),
                title,
            )
            for bid in _relation_ids(row, "Box Tracking Numbers"):
                trk = boxes.get(bid)
                if trk:
                    grouped[(trk, sku)].append(rec)

        note("reading box items", boxes=len(boxes))
        _scan_db(DB_ITEMS, take_item, lambda n: note("reading box items", item_rows=n))

        # 4. Apply the keep-rule.
        note("reconciling", groups=len(grouped))
        plan, kept, no_amazon, over_boxes = [], 0, 0, set()
        for (trk, sku), group in grouped.items():
            keep_n = expected.get((trk, sku))
            if keep_n is None:
                no_amazon += len(group)      # outside the Amazon window - leave alone
                continue
            if len(group) <= keep_n:
                kept += len(group)
                continue
            over_boxes.add(trk)
            group.sort()                     # the tuple already encodes the keep-rule
            kept += keep_n
            for neg_work, not_mat, created, pid, url, title in group[keep_n:]:
                plan.append({
                    "page_id": pid,
                    "url": url,
                    "title": title,
                    "tracking": trk,
                    "sku": sku,
                    "amazon_units": keep_n,
                    "in_notion": len(group),
                    "created": created,
                    "by_materialiser": not not_mat,
                    "work_score": -neg_work,
                })

        carries_work = sum(1 for d in plan if d["work_score"] > 0)
        with _jobs_lock:
            job.update({
                "stage": "done",
                "finished_at": time.time(),
                "plan": plan,
                "summary": {
                    "amazon_pairs": len(expected),
                    "amazon_units": staging_seen[0],
                    "boxes": len(boxes),
                    "item_groups": len(grouped),
                    "items_unparsed_title": unparsed[0],
                    "boxes_over_filled": len(over_boxes),
                    "pairs_over_filled": len({(d["tracking"], d["sku"]) for d in plan}),
                    "keep": kept,
                    "archive": len(plan),
                    "archive_carrying_work": carries_work,
                    "left_alone_no_amazon_line": no_amazon,
                },
            })
    except Exception as exc:
        with _jobs_lock:
            job.update({"stage": "error", "error": f"{type(exc).__name__}: {exc}"})


@router.post("/scan")
def start_scan(key: str = Query(...)):
    """Reconcile Notion against Amazon. Read-only - changes nothing."""
    _auth(key)
    if not NOTION_TOKEN:
        raise HTTPException(500, "NOTION_TOKEN is not set on this service.")
    job_id = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _jobs[job_id] = {"id": job_id, "stage": "starting", "started_at": time.time(), "plan": None}
    threading.Thread(target=_run_scan, args=(job_id,), daemon=True).start()
    return {"job_id": job_id, "poll": f"/dedupe/status?job_id={job_id}&key=..."}


@router.get("/status")
def status(job_id: str = Query(...), key: str = Query(...), sample: int = Query(5, ge=0, le=50)):
    _auth(key)
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "No such job. Scans are held in memory and are lost on redeploy.")
    out = {k: v for k, v in job.items() if k != "plan"}
    if job.get("plan") is not None:
        out["plan_size"] = len(job["plan"])
        out["sample"] = job["plan"][:sample]
    return out


@router.get("/plan")
def plan(job_id: str = Query(...), key: str = Query(...),
         offset: int = Query(0, ge=0), limit: int = Query(200, ge=1, le=1000)):
    """The full archive list, for auditing before you apply it."""
    _auth(key)
    job = _jobs.get(job_id)
    if not job or job.get("plan") is None:
        raise HTTPException(404, "No finished scan with that id.")
    rows = job["plan"]
    return {"total": len(rows), "offset": offset, "rows": rows[offset:offset + limit]}


# -------------------------------------------------------------------- apply

def _run_apply(job_id: str, apply_id: str) -> None:
    job = _jobs[job_id]
    run = job["applies"][apply_id]
    rows = run["rows"]
    for i, d in enumerate(rows, 1):
        try:
            r = requests.patch(
                f"{NOTION_API}/pages/{d['page_id']}",
                headers=_headers("2022-06-28"),
                json={"archived": True},
                timeout=30,
            )
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2)
                r = requests.patch(
                    f"{NOTION_API}/pages/{d['page_id']}",
                    headers=_headers("2022-06-28"),
                    json={"archived": True},
                    timeout=30,
                )
            if r.status_code >= 400:
                run["failed"].append({"page_id": d["page_id"], "error": r.text[:200]})
            else:
                run["archived"] += 1
        except Exception as exc:
            run["failed"].append({"page_id": d["page_id"], "error": str(exc)[:200]})
        run["done"] = i
        time.sleep(0.34)                      # stay under Notion's ~3 req/s
    run["stage"] = "done"
    run["finished_at"] = time.time()


@router.post("/apply")
def apply(job_id: str = Query(...),
          key: str = Query(...),
          confirm: str = Query(..., description="must be the literal string ARCHIVE"),
          force: bool = Query(False, description="also archive rows carrying human work"),
          max_archive: int = Query(DEFAULT_MAX_ARCHIVE, ge=1, le=20000)):
    """Archive exactly the pages in a finished scan's plan. Reversible for 30 days."""
    _auth(key)
    if confirm != "ARCHIVE":
        raise HTTPException(400, "Pass confirm=ARCHIVE to proceed.")
    job = _jobs.get(job_id)
    if not job or job.get("plan") is None:
        raise HTTPException(404, "No finished scan with that id. Run /dedupe/scan first.")
    if job.get("applies"):
        running = [a for a in job["applies"].values() if a["stage"] != "done"]
        if running:
            raise HTTPException(409, "An apply is already running for this scan.")

    rows = job["plan"]
    carries_work = [d for d in rows if d["work_score"] > 0]
    if carries_work and not force:
        raise HTTPException(
            409,
            f"{len(carries_work)} rows due for archiving carry human work "
            f"(frame status, processed-by, or a linked prep/stock/warranty record). "
            f"Review them at /dedupe/plan, then pass force=true if you still want them gone.",
        )
    if len(rows) > max_archive:
        raise HTTPException(
            409,
            f"Plan is {len(rows)} rows, above the {max_archive} safety cap. "
            f"Re-check the scan, then raise max_archive deliberately.",
        )

    apply_id = uuid.uuid4().hex[:8]
    job.setdefault("applies", {})[apply_id] = {
        "id": apply_id, "stage": "running", "started_at": time.time(),
        "total": len(rows), "done": 0, "archived": 0, "failed": [], "rows": rows,
    }
    threading.Thread(target=_run_apply, args=(job_id, apply_id), daemon=True).start()
    return {"apply_id": apply_id, "total": len(rows),
            "poll": f"/dedupe/apply/status?job_id={job_id}&apply_id={apply_id}&key=..."}


@router.get("/apply/status")
def apply_status(job_id: str = Query(...), apply_id: str = Query(...), key: str = Query(...)):
    _auth(key)
    job = _jobs.get(job_id) or {}
    run = (job.get("applies") or {}).get(apply_id)
    if not run:
        raise HTTPException(404, "No such apply run.")
    return {k: v for k, v in run.items() if k != "rows"} | {"failed_count": len(run["failed"])}
