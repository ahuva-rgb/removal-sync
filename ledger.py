"""
Inventory Ledger sync - Amazon SP-API -> Render Postgres

Pulls two reports and keeps them in Postgres so they can be queried any time:
  * GET_LEDGER_DETAIL_VIEW_DATA  -> table ledger_detail  (one row per event)
  * GET_LEDGER_SUMMARY_VIEW_DATA -> table ledger_summary (monthly, per FNSKU /
                                    disposition, country level)

Idempotent by design: each pull covers a date window, and the rows for that
window are deleted and re-inserted in one transaction. Re-running a window
never creates duplicates, and Amazon's late revisions get picked up.

Columns are created from the report headers (normalised to snake_case), so if
Amazon adds a column it simply appears as a new column.

Env vars (on top of the SP-API ones main.py already uses):
  DATABASE_URL        Render Postgres internal URL
  LEDGER_SYNC_TOKEN   shared secret the cron job sends to trigger a run

Routes:
  POST /ledger/sync?mode=daily|backfill   (header X-Sync-Token)
  GET  /ledger/status                     last runs + row counts
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import threading
import time
import traceback
from datetime import date, datetime, timedelta, timezone

import psycopg
import requests
from fastapi import APIRouter, Header, HTTPException, Query

router = APIRouter(prefix="/ledger", tags=["ledger"])

DATABASE_URL = os.environ.get("DATABASE_URL", "")
LEDGER_SYNC_TOKEN = os.environ.get("LEDGER_SYNC_TOKEN", "")

DETAIL_REPORT = "GET_LEDGER_DETAIL_VIEW_DATA"
SUMMARY_REPORT = "GET_LEDGER_SUMMARY_VIEW_DATA"

BACKFILL_DAYS = 540          # Amazon keeps ~18 months of ledger history
DAILY_LOOKBACK_DAYS = 10     # re-pull recent days to catch Amazon's revisions
CHUNK_DAYS = 7               # one report request per 7-day window (keeps memory low)

_run_lock = threading.Lock()

# Report columns that hold quantities -> stored as integers.
_INT_HINTS = ("quantity", "balance", "receipts", "shipments", "returns",
              "transfer", "found", "lost", "damaged", "disposed", "events",
              "in_transit", "adjustments")


# ---------------------------------------------------------------- helpers ---

def _col(name: str) -> str:
    """'Unreconciled Quantity' -> 'unreconciled_quantity'."""
    n = re.sub(r"[^a-z0-9]+", "_", (name or "").strip().strip('"').lower()).strip("_")
    return n or "col"


def _col_type(table: str, col: str) -> str:
    if col in ("event_date", "period_month"):
        return "date"
    if col == "event_datetime":
        return "timestamptz"
    if any(h in col for h in _INT_HINTS):
        return "bigint"
    return "text"


def _parse_date(v: str) -> date | None:
    v = (v or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v[:10], fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_dt(v: str):
    v = (v or "").strip()
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _to_int(v: str):
    v = (v or "").strip().replace(",", "")
    if v == "":
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def _db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set on this service")
    return psycopg.connect(DATABASE_URL, autocommit=True)


def _ensure_base_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ledger_detail (
                id bigserial PRIMARY KEY,
                event_date date,
                loaded_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS ledger_detail_date_idx ON ledger_detail (event_date);
            CREATE TABLE IF NOT EXISTS ledger_summary (
                id bigserial PRIMARY KEY,
                period_month date,
                loaded_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS ledger_summary_month_idx ON ledger_summary (period_month);
            CREATE TABLE IF NOT EXISTS ledger_loaded_windows (
                report text NOT NULL,
                start_date date NOT NULL,
                end_date date NOT NULL,
                rows bigint,
                loaded_at timestamptz NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS ledger_sync_runs (
                id bigserial PRIMARY KEY,
                mode text,
                started_at timestamptz NOT NULL DEFAULT now(),
                finished_at timestamptz,
                status text,
                detail_rows bigint DEFAULT 0,
                summary_rows bigint DEFAULT 0,
                windows jsonb DEFAULT '[]'::jsonb,
                error text
            );
        """)
    conn.commit()


def _existing_columns(conn, table: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table,))
        return {r[0] for r in cur.fetchall()}


def _ensure_columns(conn, table: str, cols: list[str]):
    have = _existing_columns(conn, table)
    with conn.cursor() as cur:
        for c in cols:
            if c not in have:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{c}" {_col_type(table, c)}')
                if c in ("fnsku", "msku", "asin", "event_type", "disposition", "reference_id"):
                    cur.execute(f'CREATE INDEX IF NOT EXISTS {table}_{c}_idx ON {table} ("{c}")')


def _convert(table: str, col: str, v: str):
    t = _col_type(table, col)
    if t == "bigint":
        return _to_int(v)
    if t == "date":
        return _parse_date(v)
    if t == "timestamptz":
        return _parse_dt(v)
    return (v or "").strip() or None


# ---------------------------------------------------------------- SP-API ----

def _with_retry(fn, *args, tries: int = 6):
    """createReport is throttled hard (burst 15, then ~1/min). Back off on 429."""
    for i in range(tries):
        try:
            return fn(*args)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code in (429, 500, 503) and i < tries - 1:
                time.sleep(min(60 * (i + 1), 300))
                continue
            raise


def _create_report(token: str, report_type: str, start: date, end: date, options: dict | None):
    import main
    body = {
        "reportType": report_type,
        "marketplaceIds": [main.MARKETPLACE_ID],
        "dataStartTime": f"{start.isoformat()}T00:00:00Z",
        "dataEndTime": f"{end.isoformat()}T23:59:59Z",
    }
    if options:
        body["reportOptions"] = options
    r = requests.post(
        f"{main.SPAPI_ENDPOINT}/reports/2021-06-30/reports",
        headers={"x-amz-access-token": token, "Content-Type": "application/json"},
        json=body, timeout=30,
    )
    if r.status_code >= 400:
        err = requests.HTTPError(f"{r.status_code} creating {report_type}: {r.text[:400]}", response=r)
        raise err
    return r.json()["reportId"]


def _iter_report_rows(report_type: str, start: date, end: date, options: dict | None = None):
    """Yield one dict per report line, streamed straight from Amazon's file.

    Nothing holds the whole report in memory: the download is decompressed and
    parsed line by line, so a big window can't blow the instance's RAM.
    """
    import main
    token = main.lwa_token()
    report_id = _with_retry(_create_report, token, report_type, start, end, options)
    doc_id = main.poll_report(token, report_id, timeout_s=1800)
    if not doc_id:
        return  # CANCELLED = no data in window
    token = main.lwa_token()  # tokens last 1h; refresh before download
    meta = requests.get(f"{main.SPAPI_ENDPOINT}/reports/2021-06-30/documents/{doc_id}",
                        headers={"x-amz-access-token": token}, timeout=30)
    meta.raise_for_status()
    meta = meta.json()
    with requests.get(meta["url"], stream=True, timeout=300) as resp:
        resp.raise_for_status()
        raw = resp.raw
        if meta.get("compressionAlgorithm") == "GZIP":
            raw = gzip.GzipFile(fileobj=raw)
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
        for row in csv.DictReader(text, delimiter="\t"):
            yield {(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}


BATCH = 1000


def _insert_stream(conn, table: str, rows, fixed: dict) -> int:
    """Insert mapped rows in batches. `fixed` = extra constant columns."""
    n, cols, batch, cur = 0, None, [], conn.cursor()
    sql = None
    for r in rows:
        if cols is None:
            cols = list(fixed) + sorted(c for c in r if c not in fixed)
            _ensure_columns(conn, table, cols)
            sql = (f"INSERT INTO {table} (" + ", ".join(f'"{c}"' for c in cols) +
                   ") VALUES (" + ", ".join(["%s"] * len(cols)) + ")")
        batch.append([fixed[c] if c in fixed else _convert(table, c, r.get(c, "")) for c in cols])
        if len(batch) >= BATCH:
            cur.executemany(sql, batch)
            n += len(batch)
            batch = []
    if batch:
        cur.executemany(sql, batch)
        n += len(batch)
    return n


def _detail_rows(start: date, end: date):
    for raw in _iter_report_rows(DETAIL_REPORT, start, end):
        r = {}
        for k, v in raw.items():
            c = _col(k)
            if c == "date":
                c = "event_date"
            elif c in ("date_and_time", "datetime", "date_time"):
                c = "event_datetime"
            r[c] = v
        d = _parse_date(r.get("event_date", ""))
        if d is None or d < start or d > end:
            continue  # adjacent window owns it
        yield r


def _load_detail_window(conn, start: date, end: date) -> int:
    with conn.transaction():
        conn.execute("DELETE FROM ledger_detail WHERE event_date BETWEEN %s AND %s", (start, end))
        n = _insert_stream(conn, "ledger_detail", _detail_rows(start, end), {})
        _mark_loaded(conn, "detail", start, end, n)
    return n


def _summary_rows(month_start: date, month_end: date):
    opts = {"aggregateByLocation": "COUNTRY", "aggregatedByTimePeriod": "MONTHLY"}
    for raw in _iter_report_rows(SUMMARY_REPORT, month_start, month_end, opts):
        yield {("report_date_label" if _col(k) == "date" else _col(k)): v for k, v in raw.items()}


def _load_summary_month(conn, month_start: date, month_end: date) -> int:
    with conn.transaction():
        conn.execute("DELETE FROM ledger_summary WHERE period_month = %s", (month_start,))
        n = _insert_stream(conn, "ledger_summary", _summary_rows(month_start, month_end),
                           {"period_month": month_start})
        _mark_loaded(conn, "summary", month_start, month_end, n)
    return n


def _mark_loaded(conn, report: str, start: date, end: date, n: int):
    conn.execute("""INSERT INTO ledger_loaded_windows (report, start_date, end_date, rows, loaded_at)
                    VALUES (%s, %s, %s, %s, now())""", (report, start, end, n))


def _already_loaded(conn, report: str, start: date, end: date) -> bool:
    """True if a past run loaded a window covering [start, end] (used to resume backfills)."""
    with conn.cursor() as cur:
        cur.execute("""SELECT 1 FROM ledger_loaded_windows
                       WHERE report = %s AND start_date <= %s AND end_date >= %s LIMIT 1""",
                    (report, start, end))
        return cur.fetchone() is not None


def _windows(start: date, end: date, size: int):
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=size - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


def _months(start: date, end: date):
    m = date(start.year, start.month, 1)
    while m <= end:
        nxt = date(m.year + (m.month == 12), (m.month % 12) + 1, 1)
        yield m, min(nxt - timedelta(days=1), end)
        m = nxt


def run_sync(mode: str = "daily") -> dict:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    back = BACKFILL_DAYS if mode == "backfill" else DAILY_LOOKBACK_DAYS
    detail_start = yesterday - timedelta(days=back - 1)
    summary_start = (yesterday - timedelta(days=back - 1)) if mode == "backfill" \
        else date(yesterday.year, yesterday.month, 1) - timedelta(days=1)  # also prior month

    conn = _db()
    _ensure_base_tables(conn)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO ledger_sync_runs (mode, status) VALUES (%s, 'running') RETURNING id", (mode,))
        run_id = cur.fetchone()[0]
    conn.commit()

    done, detail_total, summary_total = [], 0, 0
    try:
        # newest first, so recent data is usable while a backfill is still running
        for s, e in reversed(list(_windows(detail_start, yesterday, CHUNK_DAYS))):
            if mode == "backfill" and _already_loaded(conn, "detail", s, e):
                continue  # resume: an earlier (interrupted) run already loaded this
            n = _load_detail_window(conn, s, e)
            detail_total += n
            done.append({"report": "detail", "start": str(s), "end": str(e), "rows": n})
            _progress(conn, run_id, done, detail_total, summary_total)
        for s, e in reversed(list(_months(summary_start, yesterday))):
            if mode == "backfill" and e < yesterday and _already_loaded(conn, "summary", s, e):
                continue
            n = _load_summary_month(conn, s, e)
            summary_total += n
            done.append({"report": "summary", "start": str(s), "end": str(e), "rows": n})
            _progress(conn, run_id, done, detail_total, summary_total)
        _finish(conn, run_id, "ok", None)
        return {"run_id": run_id, "detail_rows": detail_total, "summary_rows": summary_total}
    except Exception as exc:
        conn.rollback()
        _finish(conn, run_id, "error", f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}")
        raise
    finally:
        conn.close()


def _progress(conn, run_id, done, d, s):
    with conn.cursor() as cur:
        cur.execute("UPDATE ledger_sync_runs SET windows=%s, detail_rows=%s, summary_rows=%s WHERE id=%s",
                    (json.dumps(done), d, s, run_id))
    conn.commit()


def _finish(conn, run_id, status, error):
    with conn.cursor() as cur:
        cur.execute("UPDATE ledger_sync_runs SET status=%s, error=%s, finished_at=now() WHERE id=%s",
                    (status, error, run_id))
    conn.commit()


def _run_in_background(mode: str):
    try:
        run_sync(mode)
    except Exception as exc:  # already logged to ledger_sync_runs
        print(f"ledger sync ({mode}) failed: {exc}")
    finally:
        _run_lock.release()


# ----------------------------------------------------------------- routes ---

@router.post("/sync")
def trigger(mode: str = Query("daily", pattern="^(daily|backfill)$"),
            x_sync_token: str = Header("")):
    if not LEDGER_SYNC_TOKEN or x_sync_token != LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    if not _run_lock.acquire(blocking=False):
        return {"started": False, "reason": "a ledger sync is already running"}
    threading.Thread(target=_run_in_background, args=(mode,), daemon=True).start()
    return {"started": True, "mode": mode}


@router.get("/status")
def status():
    conn = _db()
    try:
        _ensure_base_tables(conn)
        with conn.cursor() as cur:
            cur.execute("""SELECT id, mode, status, started_at, finished_at, detail_rows, summary_rows,
                                  left(coalesce(error,''), 300)
                           FROM ledger_sync_runs ORDER BY id DESC LIMIT 5""")
            runs = [dict(zip(["id", "mode", "status", "started_at", "finished_at",
                              "detail_rows", "summary_rows", "error"], [str(x) if x is not None else None for x in r]))
                    for r in cur.fetchall()]
            cur.execute("SELECT count(*), min(event_date), max(event_date) FROM ledger_detail")
            n, lo, hi = cur.fetchone()
        return {"running": _run_lock.locked(), "ledger_detail": {"rows": n, "from": str(lo), "to": str(hi)},
                "recent_runs": runs}
    finally:
        conn.close()


# --------------------------------------------------------- auto-backfill ---

def maybe_start_backfill():
    """On boot: if LEDGER_BACKFILL_ON_START=1 and no backfill has finished OK yet,
    start one in the background. Safe to repeat - loads are idempotent."""
    if os.environ.get("LEDGER_BACKFILL_ON_START") != "1" or not DATABASE_URL:
        return
    try:
        conn = _db()
        _ensure_base_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM ledger_sync_runs WHERE mode='backfill' AND status='ok'")
            done = cur.fetchone()[0]
        conn.close()
        print(f"ledger: database reachable; completed backfills = {done}")
        with _db() as c2:
            c2.execute("UPDATE ledger_sync_runs SET status='interrupted', finished_at=now() "
                       "WHERE status='running'")
            # One-time: record windows loaded by runs from before window tracking existed.
            c2.execute("""INSERT INTO ledger_loaded_windows (report, start_date, end_date, rows)
                SELECT DISTINCT w->>'report', (w->>'start')::date, (w->>'end')::date, (w->>'rows')::bigint
                FROM ledger_sync_runs r, jsonb_array_elements(r.windows) w
                WHERE NOT EXISTS (SELECT 1 FROM ledger_loaded_windows)""")
        if done == 0 and _run_lock.acquire(blocking=False):
            threading.Thread(target=_run_in_background, args=("backfill",), daemon=True).start()
            print("ledger: backfill started")
    except Exception as exc:
        print(f"ledger: auto-backfill check failed: {type(exc).__name__}: {exc}")


@router.on_event("startup")
def _on_startup():
    threading.Thread(target=maybe_start_backfill, daemon=True).start()
