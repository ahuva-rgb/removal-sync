"""
Reimbursements + FBA Customer Returns -> Postgres (same database as the ledger)

Adds the two reports the reimbursement checks need next to the inventory ledger:

  GET_FBA_REIMBURSEMENTS_DATA                 -> fba_reimbursements     (one row per report line)
  GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA   -> fba_customer_returns   (one row per returned unit line)

Both reports need only the Amazon Fulfillment role.

How a load works
  - One report request per window. On Amazon's 429 the run stops; it never retries
    (same rule as the ledger - Amazon's report limits are shared with Seller Central).
  - Rows are replaced by exact timestamp range: everything already stored between the
    oldest and newest timestamp in the downloaded file is deleted and re-inserted in one
    transaction. Re-running a window never duplicates rows, and a window boundary never
    drops a neighbour's rows.
  - The customer-comments column of the returns report is NOT stored (free text typed by
    buyers; not needed for any check).

Modes
  daily    - re-pull the last 30 days of each report (2 report requests)
  backfill - fill ~18 months, newest first, in 60-day windows; windows already loaded are skipped
  auto     - backfill until one has finished, then daily

Routes
  POST /claims/sync?mode=auto|daily|backfill   header X-Sync-Token: <LEDGER_SYNC_TOKEN>
  GET  /claims/status

Set CLAIMS_PAUSED=1 on the service to switch this off without a code change.
"""
import json
import os
import threading
import time
import traceback
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Header, HTTPException, Query

import ledger

router = APIRouter(prefix="/claims", tags=["claims"])

REIMB_REPORT = "GET_FBA_REIMBURSEMENTS_DATA"
RETURNS_REPORT = "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"

BACKFILL_DAYS = 540
DAILY_LOOKBACK_DAYS = 30
CHUNK_DAYS = 60
# Pause between backfill report requests: Amazon allows a burst of report requests, then ~1 a minute.
BACKFILL_PAUSE_S = int(os.environ.get("CLAIMS_BACKFILL_PAUSE_S", "45"))

# table, report type, the report's date column (normalised), stored timestamp column, stored date column
SPECS = {
    "reimbursements": {
        "table": "fba_reimbursements", "report": REIMB_REPORT,
        "src_date": "approval_date", "ts_col": "approval_ts", "date_col": "approval_date",
        "index": ("reimbursement_id", "fnsku", "sku", "asin", "amazon_order_id", "reason", "case_id"),
        "drop": (),
    },
    "returns": {
        "table": "fba_customer_returns", "report": RETURNS_REPORT,
        "src_date": "return_date", "ts_col": "return_ts", "date_col": "return_date",
        "index": ("order_id", "fnsku", "sku", "asin", "detailed_disposition", "license_plate_number"),
        "drop": ("customer_comments",),
    },
}
TABLES = tuple(s["table"] for s in SPECS.values()) + ("claims_sync_runs",)

_INT_COLS = ("quantity", "quantity_reimbursed_cash", "quantity_reimbursed_inventory",
             "quantity_reimbursed_total")
_NUM_COLS = ("amount_per_unit", "amount_total")

_run_lock = threading.Lock()
_step = {"what": "idle", "since": None}


def _set_step(what: str):
    _step["what"] = what
    _step["since"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"claims: {what}", flush=True)


# ---------------------------------------------------------------- schema ----

def _col_type(col: str) -> str:
    if col in _INT_COLS:
        return "bigint"
    if col in _NUM_COLS:
        return "numeric(14,2)"
    return "text"


def _to_num(v: str):
    v = (v or "").strip().replace(",", "")
    if v == "":
        return None
    try:
        return round(float(v), 2)
    except ValueError:
        return None


def _convert(col: str, v: str):
    t = _col_type(col)
    if t == "bigint":
        return ledger._to_int(v)
    if t.startswith("numeric"):
        return _to_num(v)
    return (v or "").strip() or None


def ensure_tables(conn):
    """Create the tables if missing and let the read-only connector login read them."""
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM unnest(%s::text[]) n WHERE to_regclass(n) IS NULL",
                    (list(TABLES),))
        missing = cur.fetchone()[0]
    if missing:
        with ledger._SETUP_LOCK, conn.cursor() as cur:
            cur.execute("SET lock_timeout = '10s'")
            for spec in SPECS.values():
                t, ts, d = spec["table"], spec["ts_col"], spec["date_col"]
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {t} (
                        id bigserial PRIMARY KEY,
                        {d} date,
                        {ts} timestamptz,
                        loaded_at timestamptz NOT NULL DEFAULT now()
                    );
                    CREATE INDEX IF NOT EXISTS {t}_{d}_idx ON {t} ({d});
                    CREATE INDEX IF NOT EXISTS {t}_{ts}_idx ON {t} ({ts});
                """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS claims_sync_runs (
                    id bigserial PRIMARY KEY,
                    mode text,
                    started_at timestamptz NOT NULL DEFAULT now(),
                    finished_at timestamptz,
                    status text,
                    reimbursement_rows bigint DEFAULT 0,
                    return_rows bigint DEFAULT 0,
                    windows jsonb DEFAULT '[]'::jsonb,
                    error text
                );
            """)
        conn.commit()
    _grant_reader(conn)


def _grant_reader(conn):
    """Give the connector's read-only login SELECT on these tables (only if that login exists)."""
    try:
        import mcp_ledger
        role = mcp_ledger.READER_ROLE
    except Exception:
        role = "ledger_reader"
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if not cur.fetchone():
            return
        for t in TABLES:
            cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')", (role, t))
            if not cur.fetchone()[0]:
                cur.execute(f"GRANT SELECT ON {t} TO {role}")
    conn.commit()


def _ensure_columns(conn, spec: dict, cols: list[str]):
    table = spec["table"]
    have = ledger._existing_columns(conn, table)
    with conn.cursor() as cur:
        for c in cols:
            if c not in have:
                cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{c}" {_col_type(c)}')
                if c in spec["index"]:
                    cur.execute(f'CREATE INDEX IF NOT EXISTS {table}_{c}_idx ON {table} ("{c}")')


# ----------------------------------------------------------------- loading ---

def _parse_ts(v: str):
    """Report timestamps look like 2026-10-28T16:30:50+00:00; fall back to a plain date."""
    ts = ledger._parse_dt(v)
    if ts is None:
        d = ledger._parse_date(v)
        if d is None:
            return None
        ts = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _mapped_rows(path: str, spec: dict):
    """Yield (timestamp, {column: raw value}) for every usable line of the report file."""
    if not path:
        return
    for raw in ledger._iter_file_rows(path):
        r = {ledger._col(k): v for k, v in raw.items() if k}
        ts = _parse_ts(r.pop(spec["src_date"], ""))
        if ts is None:
            continue  # blank/garbled line
        for c in spec["drop"]:
            r.pop(c, None)
        r.pop("id", None)
        r.pop("loaded_at", None)
        yield ts, r


def _load_window(conn, kind: str, start: date, end: date) -> int:
    spec = SPECS[kind]
    table, ts_col, date_col = spec["table"], spec["ts_col"], spec["date_col"]
    path = ledger._download_report(spec["report"], start, end)
    try:
        if not path:  # CANCELLED = no data in the window
            with conn.transaction():
                ledger._mark_loaded(conn, kind, start, end, 0)
            return 0
        # Pass 1: the exact timestamp range this file covers.
        lo = hi = None
        for ts, _ in _mapped_rows(path, spec):
            lo = ts if lo is None or ts < lo else lo
            hi = ts if hi is None or ts > hi else hi
        n = 0
        with conn.transaction():
            if lo is not None:
                conn.execute(f"DELETE FROM {table} WHERE {ts_col} BETWEEN %s AND %s", (lo, hi))
                cols, sql, batch, cur = None, None, [], conn.cursor()
                for ts, r in _mapped_rows(path, spec):
                    if cols is None:
                        cols = sorted(r)
                        _ensure_columns(conn, spec, cols)
                        names = [date_col, ts_col] + cols
                        sql = (f"INSERT INTO {table} (" + ", ".join(f'"{c}"' for c in names) +
                               ") VALUES (" + ", ".join(["%s"] * len(names)) + ")")
                    batch.append([ts.astimezone(timezone.utc).date(), ts] +
                                 [_convert(c, r.get(c, "")) for c in cols])
                    if len(batch) >= ledger.BATCH:
                        cur.executemany(sql, batch)
                        n += len(batch)
                        batch = []
                if batch:
                    cur.executemany(sql, batch)
                    n += len(batch)
            ledger._mark_loaded(conn, kind, start, end, n)
        return n
    finally:
        if path:
            os.remove(path)


def _has_completed_backfill(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM claims_sync_runs WHERE mode = 'backfill' AND status = 'ok' LIMIT 1")
        return cur.fetchone() is not None


def run_sync(mode: str = "auto") -> dict:
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    conn = ledger._db()
    ledger._ensure_base_tables(conn)   # ledger_loaded_windows lives there
    ensure_tables(conn)
    if mode == "auto":
        mode = "daily" if _has_completed_backfill(conn) else "backfill"
    with conn.cursor() as cur:
        # Only one run can be active (see _run_lock), so any row still 'running' is from a restart.
        cur.execute("UPDATE claims_sync_runs SET status='interrupted', finished_at=now() WHERE status='running'")
        cur.execute("INSERT INTO claims_sync_runs (mode, status) VALUES (%s, 'running') RETURNING id", (mode,))
        run_id = cur.fetchone()[0]

    done, totals = [], {"reimbursements": 0, "returns": 0}

    def progress():
        with conn.cursor() as c:
            c.execute("UPDATE claims_sync_runs SET windows=%s, reimbursement_rows=%s, return_rows=%s WHERE id=%s",
                      (json.dumps(done), totals["reimbursements"], totals["returns"], run_id))
        conn.commit()

    def finish(status, error):
        with conn.cursor() as c:
            c.execute("UPDATE claims_sync_runs SET status=%s, error=%s, finished_at=now() WHERE id=%s",
                      (status, error, run_id))
        conn.commit()

    try:
        if mode == "backfill":
            earliest = yesterday - timedelta(days=BACKFILL_DAYS - 1)
            windows = list(reversed(list(ledger._windows(earliest, yesterday, CHUNK_DAYS))))
        else:
            windows = [(yesterday - timedelta(days=DAILY_LOOKBACK_DAYS - 1), yesterday)]
        for kind in SPECS:
            for s, e in windows:
                if mode == "backfill" and ledger._already_loaded(conn, kind, s, e):
                    continue
                if mode == "backfill" and done and BACKFILL_PAUSE_S:
                    _set_step(f"pausing {BACKFILL_PAUSE_S}s before the next report request")
                    time.sleep(BACKFILL_PAUSE_S)
                n = _load_window(conn, kind, s, e)
                totals[kind] += n
                done.append({"report": kind, "start": str(s), "end": str(e), "rows": n})
                progress()
        finish("ok", None)
        return {"run_id": run_id, "mode": mode, **totals}
    except ledger.AmazonLimitReached as exc:
        finish("stopped_amazon_limit", "Amazon's limit for this report was reached; no retries. " + str(exc))
        raise
    except Exception as exc:
        finish("error", f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}")
        raise
    finally:
        conn.close()


def _run_in_background(mode: str):
    try:
        run_sync(mode)
    except Exception as exc:  # already logged to claims_sync_runs
        print(f"claims sync ({mode}) failed: {exc}")
    finally:
        _run_lock.release()


# ----------------------------------------------------------------- routes ---

@router.post("/sync")
def trigger(mode: str = Query("auto", pattern="^(auto|daily|backfill)$"),
            x_sync_token: str = Header("")):
    if not ledger.LEDGER_SYNC_TOKEN or x_sync_token != ledger.LEDGER_SYNC_TOKEN:
        raise HTTPException(401, "bad or missing X-Sync-Token")
    if os.environ.get("CLAIMS_PAUSED") == "1":
        return {"started": False, "reason": "claims data sync is paused (CLAIMS_PAUSED=1)"}
    if not _run_lock.acquire(blocking=False):
        return {"started": False, "reason": "a claims data sync is already running"}
    threading.Thread(target=_run_in_background, args=(mode,), daemon=True).start()
    return {"started": True, "mode": mode}


@router.get("/status")
def status():
    conn = ledger._db()
    try:
        ledger._ensure_base_tables(conn)
        ensure_tables(conn)
        out = {"running": _run_lock.locked(), "current_step": _step}
        with conn.cursor() as cur:
            for kind, spec in SPECS.items():
                cur.execute(f"SELECT count(*), min({spec['date_col']}), max({spec['date_col']}) FROM {spec['table']}")
                n, lo, hi = cur.fetchone()
                out[spec["table"]] = {"rows": n, "from": str(lo), "to": str(hi)}
            cur.execute("""SELECT id, mode, status, started_at, finished_at, reimbursement_rows, return_rows,
                                  left(coalesce(error,''), 300)
                           FROM claims_sync_runs ORDER BY id DESC LIMIT 5""")
            out["recent_runs"] = [
                dict(zip(["id", "mode", "status", "started_at", "finished_at",
                          "reimbursement_rows", "return_rows", "error"],
                         [str(x) if x is not None else None for x in r]))
                for r in cur.fetchall()]
        return out
    finally:
        conn.close()
