"""
Read-only "Designer iWear Ledger" connector for Claude (MCP over Streamable HTTP).

Lets anyone in the Claude organization ask questions about the Amazon Inventory
Ledger in plain English. Claude writes SQL; this server runs it as a dedicated
read-only Postgres login that can only SELECT from the ledger tables, inside a
READ ONLY transaction with a timeout. It cannot change or delete anything, and
it cannot see anything except the ledger.

Endpoint: POST /mcp/{MCP_PATH_TOKEN}
Env vars: DATABASE_URL (owner URL, used only to create/refresh the reader login),
          LEDGER_READER_PASSWORD, MCP_PATH_TOKEN
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hmac
import json
import os

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

router = APIRouter()

DATABASE_URL = os.environ.get("DATABASE_URL", "")
READER_PASSWORD = os.environ.get("LEDGER_READER_PASSWORD", "")
MCP_PATH_TOKEN = os.environ.get("MCP_PATH_TOKEN", "")

READER_ROLE = "ledger_reader"
TABLES = ("ledger_detail", "ledger_summary", "ledger_sync_runs", "ledger_loaded_windows")
MAX_ROWS = 2000
SUPPORTED_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")

INSTRUCTIONS = """Amazon FBA Inventory Ledger for Designer iWear (US marketplace), synced daily from SP-API.
Call describe_ledger first to see the exact columns and date coverage, then query_ledger with PostgreSQL SELECTs.
- ledger_detail: one row per ledger event (event_date, fnsku, asin, msku, title, event_type, reference_id,
  quantity, fulfillment_center, disposition, reason, country, reconciled_quantity, unreconciled_quantity, ...).
  event_type values look like Shipments, CustomerReturns, Adjustments, Receipts, WhseTransfers, VendorReturns.
- ledger_summary: monthly balances per FNSKU/disposition (period_month = first day of the month).
- ledger_sync_runs: when the data was last refreshed.
Aggregate in SQL rather than pulling raw rows; results are capped at 2000 rows."""

TOOLS = [
    {
        "name": "describe_ledger",
        "description": "List the ledger tables with their columns, data types, row counts, date coverage, "
                       "the distinct event types and dispositions, and when the data was last synced. "
                       "Call this before writing a query.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "query_ledger",
        "description": "Run one read-only PostgreSQL SELECT against the ledger tables "
                       "(ledger_detail, ledger_summary, ledger_sync_runs). Returns up to 2000 rows as JSON. "
                       "Writes are impossible (read-only login, read-only transaction, 60s timeout).",
        "inputSchema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "A single SELECT (CTEs allowed)."}},
            "required": ["sql"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True},
    },
]


# ------------------------------------------------------------ db access ----

def ensure_reader():
    """Create/refresh the read-only login. Idempotent; uses the owner URL."""
    if not (DATABASE_URL and READER_PASSWORD):
        raise RuntimeError("DATABASE_URL / LEDGER_READER_PASSWORD not set on the service")
    import ledger
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        ledger._ensure_base_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '10s'")
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (READER_ROLE,))
            verb = "ALTER" if cur.fetchone() else "CREATE"
            pw = READER_PASSWORD.replace("'", "''")
            cur.execute(f"{verb} ROLE {READER_ROLE} LOGIN PASSWORD '{pw}'")
            db = conn.info.dbname
            cur.execute(f'GRANT CONNECT ON DATABASE "{db}" TO {READER_ROLE}')
            cur.execute(f"GRANT USAGE ON SCHEMA public TO {READER_ROLE}")
            cur.execute(f"REVOKE CREATE ON SCHEMA public FROM {READER_ROLE}")
            for t in TABLES:  # only grant what's missing: GRANT waits on table locks
                cur.execute("SELECT has_table_privilege(%s, %s, 'SELECT')", (READER_ROLE, t))
                if not cur.fetchone()[0]:
                    cur.execute(f"GRANT SELECT ON {t} TO {READER_ROLE}")
            cur.execute(f"ALTER ROLE {READER_ROLE} SET default_transaction_read_only = on")
            cur.execute(f"ALTER ROLE {READER_ROLE} SET statement_timeout = '60s'")


_reader_ready = False


def _reader_conn():
    """Connect as the read-only login. Setup normally ran at startup; if the login
    doesn't work yet (first boot), set it up once and retry."""
    global _reader_ready
    params = conninfo_to_dict(DATABASE_URL)
    params.update(user=READER_ROLE, password=READER_PASSWORD, connect_timeout=10)
    info = make_conninfo(**params)
    try:
        return psycopg.connect(info, autocommit=False)
    except psycopg.OperationalError:
        if _reader_ready:
            raise
        ensure_reader()
        _reader_ready = True
        return psycopg.connect(info, autocommit=False)


def _jsonable(v):
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    return v


def run_select(sql: str) -> dict:
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise ValueError("empty query")
    with _reader_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("SET LOCAL statement_timeout = '60s'")
            cur.execute(sql)
            if cur.description is None:
                raise ValueError("query returned no result set; only SELECTs are supported")
            cols = [d.name for d in cur.description]
            rows = cur.fetchmany(MAX_ROWS + 1)
        conn.rollback()
    truncated = len(rows) > MAX_ROWS
    rows = rows[:MAX_ROWS]
    return {"columns": cols, "rows": [[_jsonable(v) for v in r] for r in rows],
            "row_count": len(rows), "truncated": truncated}


def describe() -> dict:
    out = {}
    with _reader_conn() as conn:
        with conn.cursor() as cur:
            for t in TABLES:
                cur.execute("""SELECT column_name, data_type FROM information_schema.columns
                               WHERE table_name = %s ORDER BY ordinal_position""", (t,))
                cols = {c: ty for c, ty in cur.fetchall()}
                cur.execute(f"SELECT count(*) FROM {t}")
                info = {"columns": cols, "rows": cur.fetchone()[0]}
                if t == "ledger_detail" and info["rows"]:
                    cur.execute("SELECT min(event_date), max(event_date) FROM ledger_detail")
                    lo, hi = cur.fetchone()
                    info["date_coverage"] = [str(lo), str(hi)]
                    for c in ("event_type", "disposition"):
                        if c in cols:
                            cur.execute(f'SELECT "{c}", count(*) FROM ledger_detail GROUP BY 1 ORDER BY 2 DESC')
                            info[f"{c}_values"] = {str(k): n for k, n in cur.fetchall()}
                if t == "ledger_summary" and info["rows"]:
                    cur.execute("SELECT min(period_month), max(period_month) FROM ledger_summary")
                    lo, hi = cur.fetchone()
                    info["month_coverage"] = [str(lo), str(hi)]
                if t == "ledger_sync_runs":
                    cur.execute("""SELECT mode, status, started_at, finished_at FROM ledger_sync_runs
                                   ORDER BY id DESC LIMIT 3""")
                    info["recent_runs"] = [[_jsonable(v) for v in r] for r in cur.fetchall()]
                out[t] = info
        conn.rollback()
    return out


# ------------------------------------------------------------ JSON-RPC -----

def _result(mid, result):
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def _error(mid, code, msg):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": msg}}


def _text(payload, is_error=False):
    text = payload if isinstance(payload, str) else json.dumps(payload, default=str)
    return {"content": [{"type": "text", "text": text}], "isError": is_error}


def handle(msg: dict):
    mid = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params") or {}
    if mid is None:  # notification (e.g. notifications/initialized)
        return None
    if method == "initialize":
        req = params.get("protocolVersion")
        ver = req if req in SUPPORTED_VERSIONS else SUPPORTED_VERSIONS[0]
        return _result(mid, {
            "protocolVersion": ver,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "designer-iwear-ledger", "version": "1.0.0"},
            "instructions": INSTRUCTIONS,
        })
    if method == "ping":
        return _result(mid, {})
    if method == "tools/list":
        return _result(mid, {"tools": TOOLS})
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        try:
            if name == "describe_ledger":
                return _result(mid, _text(describe()))
            if name == "query_ledger":
                return _result(mid, _text(run_select(str(args.get("sql", "")))))
            return _error(mid, -32602, f"unknown tool: {name}")
        except Exception as exc:  # surfaced to Claude so it can fix its SQL
            return _result(mid, _text(f"{type(exc).__name__}: {exc}", is_error=True))
    return _error(mid, -32601, f"method not found: {method}")


def _authorized(token: str) -> bool:
    return bool(MCP_PATH_TOKEN) and hmac.compare_digest(token, MCP_PATH_TOKEN)


@router.post("/mcp/{token}")
async def mcp_post(token: str, request: Request):
    if not _authorized(token):
        return Response(status_code=404)
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(_error(None, -32700, "parse error"), status_code=400)
    if isinstance(body, list):
        replies = [r for r in (handle(m) for m in body) if r is not None]
        return JSONResponse(replies) if replies else Response(status_code=202)
    reply = handle(body)
    return JSONResponse(reply) if reply is not None else Response(status_code=202)


@router.get("/mcp/{token}")
async def mcp_get(token: str):
    # No server-initiated stream; clients fall back to plain POST responses.
    return Response(status_code=405 if _authorized(token) else 404)


@router.delete("/mcp/{token}")
async def mcp_delete(token: str):
    return Response(status_code=200 if _authorized(token) else 404)
