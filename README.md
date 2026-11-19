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

---

# Inventory Ledger → Postgres (`ledger.py`)

Pulls `GET_LEDGER_DETAIL_VIEW_DATA` (every event) and `GET_LEDGER_SUMMARY_VIEW_DATA`
(monthly, country level) into the `amazon-ledger-db` Render Postgres so the ledger can
be queried from chat.

- Tables: `ledger_detail` (one row per event, `event_date`), `ledger_summary`
  (`period_month`), `ledger_sync_runs` (run log).
- Each window is deleted and re-inserted in one transaction, so re-runs never duplicate.
- Daily mode re-pulls the last 10 days (catches Amazon revisions) plus this and last month's summary.
- Backfill mode pulls ~18 months, newest first.
- Env vars on this service: `DATABASE_URL` (internal URL of amazon-ledger-db), `LEDGER_SYNC_TOKEN`.
- Triggered daily by the `ledger-sync-daily` cron job (`trigger_ledger.py`).
- `GET /ledger/status` shows recent runs and row counts.

## Ledger connector for Claude (`mcp_ledger.py`)
Read-only MCP endpoint at `/mcp/<MCP_PATH_TOKEN>` with two tools: `describe_ledger` and
`query_ledger`. Queries run as the `ledger_reader` Postgres login (SELECT on the three ledger
tables only, read-only transaction, 60s timeout). Env vars: `LEDGER_READER_PASSWORD`, `MCP_PATH_TOKEN`.
Added once as a custom connector in the Claude organization so everyone there can use it.

---

# Reimbursements + Customer Returns → Postgres (`claims_data.py`)

Pulls `GET_FBA_REIMBURSEMENTS_DATA` and `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` into the same
`amazon-ledger-db` database, so the reimbursement checks can see what Amazon already paid and which
refunded units actually came back. Both reports need only the Amazon Fulfillment role.

- Tables: `fba_reimbursements` (`approval_date`, `approval_ts`), `fba_customer_returns`
  (`return_date`, `return_ts`), `claims_sync_runs` (run log).
- Rows are replaced by the exact timestamp range of each downloaded file, in one transaction, so
  re-runs never duplicate and never drop a neighbouring window's rows.
- The returns report's `customer-comments` column (buyer free text) is not stored.
- Daily mode re-pulls the last 30 days of each report (2 report requests). Backfill fills ~18 months
  in 60-day windows, newest first, pausing between requests; it resumes where it stopped.
- A 429 from Amazon stops the run without retrying, same as the ledger.
- Triggered by the existing `ledger-sync-daily` cron job: `trigger_ledger.py` now calls
  `/ledger/sync` and then `/claims/sync`. No new Render service is needed.
- Switches: `CLAIMS_PAUSED=1` on the service turns it off; `CLAIMS_SYNC=0` on the cron job skips the call.
- `GET /claims/status` shows row counts, date coverage and recent runs.
- The Claude ledger connector (`mcp_ledger.py`) lists and can query the two new tables once they exist.


## Agent 3: Payment accuracy (`agent_payment.py`)

Runs every morning after the ledger and reimbursement syncs (the same cron job calls
`POST /agents/payment-accuracy/run`; the agent waits for the syncs to finish).

For each warehouse lost/damaged reimbursement Amazon issued in the last 60 days it finds the loss line
in the Inventory Ledger, decides whether 100% or 50% of sourcing cost applies, compares with the
cost in Notion on the loss date, and creates one Asana task per underpaid reimbursement
(board "Reimbursement Case Discrepancy", section "To Be Opened"). The task holds the two amounts for
the case form, the case wording, the filing deadline and the Notion invoice link.

| Status when lost | How it got that status | Pays |
|---|---|---|
| Sellable | n/a | 100% |
| Customer damaged / defective | customer return | 50% |
| Customer damaged / defective | Amazon re-graded it (no returns, or re-graded at that warehouse within 45 days) | 100% |
| Warehouse / carrier / distributor damaged | n/a | 100% |

No task when the shortfall is under $1 a unit, the reimbursement was reversed, the SKU contains `ROP`,
or a task already exists for that reimbursement. Each reimbursement is tasked once, ever.

- Without `ASANA_TOKEN` the agent only records what it would create (table `payment_claims`).
- `GET /agents/payment-accuracy/status` shows the last runs and counts.
- `PAYMENT_AGENT_PAUSED=1` stops it. `PAYMENT_AGENT=0` on the cron job skips the call.
- Inbound reimbursements are off until checked by hand (`PAYMENT_AGENT_INCLUDE_INBOUND=1`).
- The Notion integration behind `NOTION_TOKEN` needs access to the SKU & ASIN, UPC, COGS, Invoices and
  Line Items databases.

### Agent 3 update (Nov 17)

- Tasks go to the board "Reimbursement Case Discrepancy", section "To file: 100%" or "To file: 50%", assigned to Azel
  (`ASANA_ASSIGNEE_GID`), due the day after they are created. Asana cannot lock a due date, so each run puts the date
  back on any of its tasks still waiting to be filed and leaves a comment.
- Every task starts with STEPS. Version A: upload the sourcing cost first, then open the case. Version B: the cost
  was already submitted for that SKU before the loss (table `sku_cost_uploads`), so go straight to the case.
  A version A task that gets completed logs the SKU as uploaded. Costs uploaded by hand can be listed in
  `sourcing_cost_uploads_seed.csv` (`sku,uploaded_on`).
- `PAYMENT_AGENT_PAUSED=1` now means dry run: the daily list is still written to `payment_claims`, nothing is
  created or changed in Asana.

### Invoice PDFs on agent 3 tasks, and the mailbox connection

- `google_mail.py`: read-only Gmail access. One-time sign-in per mailbox at `/google/connect`
  (only `cases@` and `invoices@` can be connected; scope is `gmail.readonly`). `/google/status` shows what is connected.
  Needs `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET`.
- `invoice_pdf.py`: finds the supplier invoice PDF (Notion file, then the email Notion recorded, then a mailbox
  search by invoice number) and highlights the item's UPC and unit price.
- Agent 3 attaches that PDF to every open task that does not have one yet, including tasks created earlier,
  and leaves a comment. If the UPC cannot be highlighted (a scan) it says so on the task.
