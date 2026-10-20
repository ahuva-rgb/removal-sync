"""
Manual Inventory Ledger import - upload Seller Central "Detailed View" exports.

Seller Central downloads don't use the SP-API report quota, so this is the fast way to
load older history while the API is throttled. Files load into the same ledger_detail
table: for each file, the date range it covers is replaced (so re-uploading the same
file never duplicates), and that range is recorded so the API backfill skips it.

Page:   GET  /ledger/import/{MCP_PATH_TOKEN}
Upload: PUT  /ledger/import/{MCP_PATH_TOKEN}?name=<file name>   (raw file body)
"""

from __future__ import annotations

import csv
import hmac
import io
import os
import tempfile
import threading
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

import ledger

router = APIRouter()
_import_lock = threading.Lock()
_jobs: list[dict] = []  # newest first, kept in memory for the status table


def _authorized(token: str) -> bool:
    t = os.environ.get("MCP_PATH_TOKEN", "")
    return bool(t) and hmac.compare_digest(token, t)


def _sniff_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
        head = fh.readline()
    return "\t" if head.count("\t") > head.count(",") else ","


def _file_rows(path: str):
    delim = _sniff_delimiter(path)
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        for raw in csv.DictReader(fh, delimiter=delim):
            r = {}
            for k, v in raw.items():
                c = ledger._col(k)
                if c == "date":
                    c = "event_date"
                elif c in ("date_and_time", "datetime", "date_time"):
                    c = "event_datetime"
                r[c] = (v or "").strip()
            yield r


def _date_range(path: str):
    lo = hi = None
    n = 0
    for r in _file_rows(path):
        d = ledger._parse_date(r.get("event_date", ""))
        if d is None:
            continue
        n += 1
        lo = d if lo is None or d < lo else lo
        hi = d if hi is None or d > hi else hi
    return lo, hi, n


def _process(job: dict, path: str):
    try:
        with _import_lock:
            job["status"] = "checking file"
            lo, hi, n = _date_range(path)
            if not n:
                raise ValueError("no rows with a readable Date column - is this the Detailed View export?")
            job.update(start=str(lo), end=str(hi), status=f"loading {n:,} rows")
            conn = ledger._db()
            try:
                ledger._ensure_base_tables(conn)
                with conn.transaction():
                    conn.execute("DELETE FROM ledger_detail WHERE event_date BETWEEN %s AND %s", (lo, hi))
                    rows = (r for r in _file_rows(path) if ledger._parse_date(r.get("event_date", "")))
                    loaded = ledger._insert_stream(conn, "ledger_detail", rows, {})
                    conn.execute("""INSERT INTO ledger_loaded_windows (report, start_date, end_date, rows)
                                    VALUES ('detail', %s, %s, %s)""", (lo, hi, loaded))
                    conn.execute("""INSERT INTO ledger_sync_runs (mode, status, finished_at, detail_rows, windows)
                                    VALUES ('manual_import', 'ok', now(), %s, %s::jsonb)""",
                                 (loaded, f'[{{"report":"detail","start":"{lo}","end":"{hi}","rows":{loaded},'
                                          f'"file":"{job["name"][:80]}"}}]'))
            finally:
                conn.close()
            job.update(status="done", rows=loaded)
            print(f"ledger import: {job['name']} {lo}..{hi} {loaded} rows", flush=True)
    except Exception as exc:
        job.update(status="error", error=f"{type(exc).__name__}: {exc}"[:400])
        print(f"ledger import failed: {job['name']}: {exc}", flush=True)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


@router.put("/ledger/import/{token}")
async def upload(token: str, request: Request, name: str = "upload.csv"):
    if not _authorized(token):
        return Response(status_code=404)
    fd, path = tempfile.mkstemp(prefix="ledger_import_", suffix=".csv")
    size = 0
    with os.fdopen(fd, "wb") as fh:  # streamed to disk, never held in memory
        async for chunk in request.stream():
            fh.write(chunk)
            size += len(chunk)
    job = {"name": name, "size_mb": round(size / 1e6, 1), "status": "queued",
           "uploaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    _jobs.insert(0, job)
    del _jobs[20:]
    threading.Thread(target=_process, args=(job, path), daemon=True).start()
    return JSONResponse(job)


@router.get("/ledger/import/{token}/status")
def import_status(token: str):
    if not _authorized(token):
        return Response(status_code=404)
    return JSONResponse(_jobs)


@router.get("/ledger/import/{token}", response_class=HTMLResponse)
def page(token: str):
    if not _authorized(token):
        return Response(status_code=404)
    return """<!doctype html><html><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1"><title>Ledger Import</title>
<style>
 body{font-family:system-ui,sans-serif;max-width:640px;margin:32px auto;padding:0 16px;color:#1d1d1f;font-size:18px}
 h1{font-size:26px} .drop{border:2px dashed #999;border-radius:12px;padding:36px;text-align:center;cursor:pointer}
 .drop.on{border-color:#0a7;background:#f0fbf6} table{width:100%;border-collapse:collapse;margin-top:20px;font-size:16px}
 td,th{padding:8px;border-bottom:1px solid #ddd;text-align:left} .err{color:#b00}
</style></head><body>
<h1>Upload Inventory Ledger files</h1>
<p>Seller Central &rarr; Inventory Ledger &rarr; <b>Detailed View</b> &rarr; download CSV. Drop one or more files below.</p>
<p>Re-uploading the same dates is safe &mdash; they get replaced, not duplicated.</p>
<label class=drop id=drop>Click or drop CSV files here<input id=f type=file multiple accept=".csv,.txt,.tsv" hidden></label>
<table><thead><tr><th>File</th><th>Dates</th><th>Rows</th><th>Status</th></tr></thead><tbody id=t></tbody></table>
<script>
const base = location.pathname.replace(/\\/$/, '');
const drop = document.getElementById('drop'), input = document.getElementById('f');
async function send(files){
  for (const file of files){
    drop.textContent = 'Uploading ' + file.name + '…';
    await fetch(base + '?name=' + encodeURIComponent(file.name), {method:'PUT', body:file});
  }
  drop.textContent = 'Click or drop CSV files here'; refresh();
}
input.onchange = () => send(input.files);
drop.ondragover = e => {e.preventDefault(); drop.classList.add('on')};
drop.ondragleave = () => drop.classList.remove('on');
drop.ondrop = e => {e.preventDefault(); drop.classList.remove('on'); send(e.dataTransfer.files)};
async function refresh(){
  try{
    const jobs = await (await fetch(base + '/status')).json();
    document.getElementById('t').innerHTML = jobs.map(j =>
      `<tr><td>${j.name}</td><td>${j.start ? j.start + ' → ' + j.end : ''}</td>` +
      `<td>${j.rows ? j.rows.toLocaleString() : ''}</td>` +
      `<td class="${j.status==='error'?'err':''}">${j.status}${j.error ? ': ' + j.error : ''}</td></tr>`).join('');
  }catch(e){}
}
refresh(); setInterval(refresh, 4000);
</script></body></html>"""
