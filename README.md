# Removal Sync — SP-API → Notion (dedupe mode)

Pulls the FBA Removal Shipment Detail report and inserts one Notion row per
shipment line — same columns as the Relay CSV flow, but with two fixes:

1. **No duplicates.** Before writing, the app reads every existing Key in the
   Notion database and skips any row already there. Key = `order-id | sku | tracking-number`.
   Run it daily, weekly, whenever — the same tracking never uploads twice.
2. **No scientific notation.** Tracking numbers go straight from Amazon's raw
   report into a Notion text property. Nothing passes through Excel/Relay, so
   `9200190213423955881110` stays exact.

## Notion columns
Key (title), Request Date, Order ID, Shipment Date, SKU, FNSKU, Disposition,
Shipped Qty, Carrier, Tracking Number, Removal Order Type, Order Source, Shipment ID

## Deploy on Render
1. Push `main.py`, `requirements.txt`, `render.yaml` to a new GitHub repo.
2. Render → New → Web Service → connect repo.
3. Env vars:
   - `LWA_CLIENT_ID`, `LWA_CLIENT_SECRET`, `SPAPI_REFRESH_TOKEN` — SP-API creds (Reports role required)
   - `NOTION_TOKEN` — integration token; share the target DB/page with the integration
   - `NOTION_DATABASE_ID` — existing DB, **or** leave blank + set `NOTION_PARENT_PAGE_ID`
     to auto-create "FBA Removal Shipments" on first run (response returns the new
     DB id — save it as `NOTION_DATABASE_ID` after)

If pointing at your **existing** Relay-fed database instead of a new one, the
property names above must match exactly, and a "Key" title property must exist
(or tell me your current title property and I'll adapt the code).

## Use — test first, then sync
1. **Preview (no Notion writes):** open the service URL → "Preview CSV" button,
   or visit `https://.../preview?days=30` in a browser. Downloads a CSV in the
   exact Relay column format pulled live from SP-API. Only the 3 Amazon env
   vars are needed for this — Notion token can wait.
2. Compare it to a known-good Relay CSV. If it matches, set the Notion env
   vars and use **Run Sync** / `POST /sync?days=N`.
   Response shows: report_rows, already_in_notion_skipped, created, qty_updated, errors.

## Notes
- Amazon takes 1–5 min to generate the report; app polls every 15s.
- CANCELLED report = no data in range = treated as empty, not an error.
- Notion writes throttled ~3/s. Dedupe scan is fast (100 rows/page).
- Overlap your windows generously (e.g. 60 days) — dedupe makes re-pulls free.
