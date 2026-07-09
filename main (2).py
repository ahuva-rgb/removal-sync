"""
Removal Sync - Amazon SP-API -> Notion (dedupe mode)
Pulls GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA and inserts one Notion
row per shipment line. Rows already in Notion (matched on
"order-id | sku | tracking-number") are skipped, never re-uploaded.

Env vars:
  LWA_CLIENT_ID, LWA_CLIENT_SECRET, SPAPI_REFRESH_TOKEN
  NOTION_TOKEN
  NOTION_DATABASE_ID    (or blank + NOTION_PARENT_PAGE_ID to auto-create)
Optional: MARKETPLACE_ID (default US), SPAPI_ENDPOINT
"""

import csv
import gzip
import io
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

app = FastAPI(title="Removal Sync")

LWA_CLIENT_ID = os.environ.get("LWA_CLIENT_ID", "")
LWA_CLIENT_SECRET = os.environ.get("LWA_CLIENT_SECRET", "")
SPAPI_REFRESH_TOKEN = os.environ.get("SPAPI_REFRESH_TOKEN", "")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
NOTION_PARENT_PAGE_ID = os.environ.get("NOTION_PARENT_PAGE_ID", "")
MARKETPLACE_ID = os.environ.get("MARKETPLACE_ID", "ATVPDKIKX0DER")
SPAPI_ENDPOINT = os.environ.get("SPAPI_ENDPOINT", "https://sellingpartnerapi-na.amazon.com")

SHIPMENT_REPORT = "GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA"
NOTION_VERSION = "2022-06-28"
NOTION_API = "https://api.notion.com/v1"


# ---------------------------------------------------------------- SP-API ----

def lwa_token() -> str:
    r = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": SPAPI_REFRESH_TOKEN,
            "client_id": LWA_CLIENT_ID,
            "client_secret": LWA_CLIENT_SECRET,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def request_report(token: str, start_iso: str, end_iso: str) -> str:
    r = requests.post(
        f"{SPAPI_ENDPOINT}/reports/2021-06-30/reports",
        headers={"x-amz-access-token": token, "Content-Type": "application/json"},
        json={
            "reportType": SHIPMENT_REPORT,
            "marketplaceIds": [MARKETPLACE_ID],
            "dataStartTime": start_iso,
            "dataEndTime": end_iso,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["reportId"]


def poll_report(token: str, report_id: str, timeout_s: int = 600) -> str | None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = requests.get(
            f"{SPAPI_ENDPOINT}/reports/2021-06-30/reports/{report_id}",
            headers={"x-amz-access-token": token},
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        status = body.get("processingStatus")
        if status == "DONE":
            return body["reportDocumentId"]
        if status == "CANCELLED":
            return None  # no data in range
        if status == "FATAL":
            detail = f"Report {report_id} FATAL"
            doc_id = body.get("reportDocumentId")
            if doc_id:
                try:  # Amazon attaches an error document explaining the failure
                    detail += " — Amazon says: " + download_report(token, doc_id)[:500]
                except Exception as e:
                    detail += f" — error doc fetch failed: {e}"
            else:
                detail += " — no error document provided (v2)"
            raise HTTPException(502, detail)
        time.sleep(15)
    raise HTTPException(504, f"Timed out waiting for report {report_id}")


def download_report(token: str, document_id: str) -> str:
    r = requests.get(
        f"{SPAPI_ENDPOINT}/reports/2021-06-30/documents/{document_id}",
        headers={"x-amz-access-token": token},
        timeout=30,
    )
    r.raise_for_status()
    doc = r.json()
    raw = requests.get(doc["url"], timeout=120).content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8", errors="replace")


def parse_tsv(text: str) -> list[dict]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [{(k or "").strip().lower(): (v or "").strip() for k, v in row.items()} for row in reader]


# ---------------------------------------------------------------- Notion ----

def notion_headers() -> dict:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


DB_SCHEMA = {
    "Key": {"title": {}},
    "Request Date": {"date": {}},
    "Order ID": {"rich_text": {}},
    "Shipment Date": {"date": {}},
    "SKU": {"rich_text": {}},
    "FNSKU": {"rich_text": {}},
    "Disposition": {"select": {}},
    "Shipped Qty": {"number": {}},
    "Carrier": {"rich_text": {}},
    "Tracking Number": {"rich_text": {}},
    "Removal Order Type": {"select": {}},
    "Order Source": {"rich_text": {}},
    "Shipment ID": {"rich_text": {}},
}


def ensure_database() -> str:
    global NOTION_DATABASE_ID
    if NOTION_DATABASE_ID:
        return NOTION_DATABASE_ID
    if not NOTION_PARENT_PAGE_ID:
        raise HTTPException(500, "Set NOTION_DATABASE_ID or NOTION_PARENT_PAGE_ID")
    r = requests.post(
        f"{NOTION_API}/databases",
        headers=notion_headers(),
        json={
            "parent": {"type": "page_id", "page_id": NOTION_PARENT_PAGE_ID},
            "title": [{"type": "text", "text": {"content": "FBA Removal Shipments"}}],
            "properties": DB_SCHEMA,
        },
        timeout=30,
    )
    r.raise_for_status()
    NOTION_DATABASE_ID = r.json()["id"]
    return NOTION_DATABASE_ID


def existing_rows(db_id: str) -> dict[str, tuple[str, float | None]]:
    """Fetch every Key already in the database -> (page_id, shipped_qty)."""
    rows: dict[str, tuple[str, float | None]] = {}
    cursor = None
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(f"{NOTION_API}/databases/{db_id}/query",
                          headers=notion_headers(), json=payload, timeout=60)
        r.raise_for_status()
        body = r.json()
        for page in body.get("results", []):
            props = page.get("properties", {})
            title = props.get("Key", {}).get("title", [])
            if title:
                key = "".join(t.get("plain_text", "") for t in title)
                qty = props.get("Shipped Qty", {}).get("number")
                rows[key] = (page["id"], qty)
        if not body.get("has_more"):
            return rows
        cursor = body.get("next_cursor")
        time.sleep(0.34)


def aggregate_rows(rows: list[dict]) -> list[dict]:
    """Collapse report lines sharing the same key; sum shipped-quantity."""
    agg: dict[str, dict] = {}
    for row in rows:
        key = row_key(row)
        if key not in agg:
            agg[key] = dict(row)
        else:
            q1 = _num(agg[key].get("shipped-quantity", "")) or 0
            q2 = _num(row.get("shipped-quantity", "")) or 0
            agg[key]["shipped-quantity"] = str(int(q1 + q2) if (q1 + q2).is_integer() else q1 + q2)
            # keep earliest shipment date on the row
            d1, d2 = agg[key].get("shipment-date", ""), row.get("shipment-date", "")
            if d2 and (not d1 or d2 < d1):
                agg[key]["shipment-date"] = d2
    return list(agg.values())


def row_key(row: dict) -> str:
    return " | ".join([row.get("order-id", ""), row.get("sku", ""),
                       row.get("tracking-number", "")])


def _rt(v: str):
    return {"rich_text": [{"text": {"content": v[:2000]}}] if v else []}


def _sel(v: str):
    return {"select": {"name": v[:100]}} if v else {"select": None}


def _num(v: str):
    try:
        return float(v) if v not in ("", None) else None
    except ValueError:
        return None


def _date(v: str):
    if not v:
        return None
    return {"date": {"start": v.strip().replace(" ", "T", 1)}}


def build_properties(row: dict) -> dict:
    props = {
        "Key": {"title": [{"text": {"content": row_key(row)[:2000]}}]},
        "Order ID": _rt(row.get("order-id", "")),
        "SKU": _rt(row.get("sku", "")),
        "FNSKU": _rt(row.get("fnsku", "")),
        "Disposition": _sel(row.get("disposition", "")),
        "Shipped Qty": {"number": _num(row.get("shipped-quantity", ""))},
        "Carrier": _rt(row.get("carrier", "")),
        "Tracking Number": _rt(row.get("tracking-number", "")),  # text: no truncation / sci notation
        "Removal Order Type": _sel(row.get("removal-order-type", "")),
        "Order Source": _rt(row.get("order-source", "")),
        "Shipment ID": _rt(row.get("shipment-id", "")),
    }
    rd = _date(row.get("request-date", ""))
    if rd:
        props["Request Date"] = rd
    sd = _date(row.get("shipment-date", ""))
    if sd:
        props["Shipment Date"] = sd
    return props


def create_page(db_id: str, row: dict):
    r = requests.post(f"{NOTION_API}/pages", headers=notion_headers(),
                      json={"parent": {"database_id": db_id},
                            "properties": build_properties(row)}, timeout=30)
    r.raise_for_status()


# ------------------------------------------------------------------ routes --

@app.head("/")
@app.get("/", response_class=HTMLResponse)
def home():
    return """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1">
    <body style="font-family:sans-serif;max-width:480px;margin:40px auto;padding:0 16px">
    <h2>FBA Removal Shipments &rarr; Notion</h2>
    <p style="color:#666">Already-uploaded tracking rows are skipped automatically.</p>
    <label>Days back: <input id=d type=number value=30 style="width:70px"></label>
    <button onclick="run()" style="margin-left:12px;padding:8px 20px">Run Sync</button>
    <button onclick="location.href='/preview?days='+d.value" style="margin-left:8px;padding:8px 20px">Preview CSV (no Notion)</button>
    <pre id=out style="background:#f4f4f4;padding:12px;white-space:pre-wrap"></pre>
    <script>
    async function run(){
      out.textContent='Running... (Amazon report generation can take a few minutes)';
      try{
        const r=await fetch('/sync?days='+d.value,{method:'POST'});
        out.textContent=JSON.stringify(await r.json(),null,2);
      }catch(e){out.textContent='Error: '+e}
    }
    </script>"""


CSV_COLUMNS = ["request-date", "order-id", "shipment-date", "sku", "fnsku",
               "disposition", "shipped-quantity", "carrier", "tracking-number",
               "removal-order-type", "order-source", "shipment-id"]


def pull_report_rows(days: int) -> tuple[list[dict], str]:
    for name, val in [("LWA_CLIENT_ID", LWA_CLIENT_ID), ("LWA_CLIENT_SECRET", LWA_CLIENT_SECRET),
                      ("SPAPI_REFRESH_TOKEN", SPAPI_REFRESH_TOKEN)]:
        if not val:
            raise HTTPException(500, f"Missing env var: {name}")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_iso = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    token = lwa_token()
    report_id = request_report(token, start_iso, end_iso)
    doc_id = poll_report(token, report_id)
    rows = parse_tsv(download_report(token, doc_id)) if doc_id else []
    return rows, f"{start_iso} -> {end_iso}"


@app.post("/preview")
@app.get("/preview")
def preview(days: int = Query(30, ge=1, le=540)):
    """Pull from Amazon and return the CSV. NO Notion writes."""
    rows, window = pull_report_rows(days)
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({c: row.get(c, "") for c in CSV_COLUMNS})
    return PlainTextResponse(
        out.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition":
                 f'attachment; filename="removal_shipments_preview_{days}d.csv"',
                 "X-Window": window, "X-Rows": str(len(rows))},
    )


@app.post("/sync")
def sync(days: int = Query(30, ge=1, le=540)):
    if not NOTION_TOKEN:
        raise HTTPException(500, "Missing env var: NOTION_TOKEN")
    rows, window = pull_report_rows(days)

    db_id = ensure_database()
    existing = existing_rows(db_id)
    rows = aggregate_rows(rows)  # same key within one report -> one row, qty summed

    created = skipped = qty_updated = errors = 0
    error_samples = []
    for row in rows:
        key = row_key(row)
        new_qty = _num(row.get("shipped-quantity", ""))
        if key in existing:
            page_id, old_qty = existing[key]
            if new_qty is not None and old_qty is not None and new_qty > old_qty:
                try:  # late-arriving line under same tracking: bump the qty
                    r = requests.patch(f"{NOTION_API}/pages/{page_id}",
                                       headers=notion_headers(),
                                       json={"properties": {"Shipped Qty": {"number": new_qty}}},
                                       timeout=30)
                    r.raise_for_status()
                    qty_updated += 1
                except Exception as e:
                    errors += 1
                    if len(error_samples) < 10:
                        error_samples.append(f"{key}: {e}")
                time.sleep(0.34)
            else:
                skipped += 1
            continue
        try:
            create_page(db_id, row)
            existing[key] = ("", new_qty)
            created += 1
        except Exception as e:
            errors += 1
            if len(error_samples) < 10:
                error_samples.append(f"{key}: {e}")
        time.sleep(0.34)  # Notion ~3 req/s

    return {
        "window": window,
        "report_rows": len(rows),
        "already_in_notion_skipped": skipped,
        "created": created,
        "qty_updated": qty_updated,
        "errors": errors,
        "error_samples": error_samples,
        "notion_database_id": db_id,
    }
