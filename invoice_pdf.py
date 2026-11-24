"""
Find a supplier invoice PDF and highlight one UPC line on it.

Sources, in order:
  1. the PDF already fetched onto the Notion invoice page ("Invoice" files property)
  2. the email Notion recorded for that invoice ("Email ID"), read from the invoices mailbox
  3. a search of the invoices mailbox for the invoice number
"""

import os
import re

import requests

import google_mail

INVOICE_MAILBOX = os.environ.get("INVOICE_MAILBOX", "invoices@gefenoptical.com")
MAX_PDF_BYTES = 12 * 1024 * 1024


class MailboxNotConnected(LookupError):
    """The invoice could not be looked for at all (temporary): not the same as "there is no invoice"."""


def pdf_text(data: bytes) -> str:
    import pymupdf
    with pymupdf.open(stream=data, filetype="pdf") as doc:
        return "\n".join(page.get_text() for page in doc)


def highlight(data: bytes, upc: str, price=None) -> tuple[bytes, int]:
    """Yellow-highlight every occurrence of the UPC, plus the unit price printed on the same line item.
    Returns (pdf bytes, number of UPC hits). 0 hits = the PDF has no searchable text for it (e.g. a scan)."""
    import pymupdf
    hits = 0
    with pymupdf.open(stream=data, filetype="pdf") as doc:
        for page in doc:
            for r in page.search_for(upc):
                page.add_highlight_annot(r)
                hits += 1
                if price is not None:
                    for x in page.search_for(f"${float(price):,.2f}"):
                        if abs(x.y0 - r.y0) < 40:      # same row of the invoice table (wrapped rows included)
                            page.add_highlight_annot(x)
        return (doc.tobytes() if hits else data), hits


def _is_pdf(data: bytes) -> bool:
    return data[:5] == b"%PDF-"


def _pick(pdfs: list[tuple[str, bytes]], invoice_number: str, upc: str):
    """Prefer the PDF that shows the UPC, then one that shows the invoice number, then one named after it."""
    scored = []
    for name, data in pdfs:
        try:
            text = pdf_text(data)
        except Exception:
            text = ""
        score = (2 if upc and upc in text else 0) + (1 if invoice_number and invoice_number in text else 0) \
                + (0.5 if invoice_number and invoice_number in name else 0)
        scored.append((score, name, data))
    scored.sort(key=lambda s: -s[0])
    return (scored[0][1], scored[0][2]) if scored and scored[0][0] > 0 else None


def _from_message(message_id: str, invoice_number: str, upc: str):
    msg = google_mail.message(INVOICE_MAILBOX, message_id)
    pdfs = []
    for a in msg["attachments"]:
        if not (a["filename"].lower().endswith(".pdf") or "pdf" in a["mime"]) or a["size"] > MAX_PDF_BYTES * 1.4:
            continue
        data = google_mail.attachment(INVOICE_MAILBOX, message_id, a["attachment_id"])
        if _is_pdf(data) and len(data) <= MAX_PDF_BYTES:
            pdfs.append((a["filename"], data))
    return _pick(pdfs, invoice_number, upc)


def fetch(invoice_number: str, upc: str, notion_file_url: str | None = None, email_id: str | None = None):
    """Returns (filename, pdf bytes, where it came from) or raises LookupError with a plain reason."""
    reasons = []
    if notion_file_url:
        try:
            r = requests.get(notion_file_url, timeout=60)
            if r.ok and _is_pdf(r.content) and len(r.content) <= MAX_PDF_BYTES:
                return f"{invoice_number}.pdf", r.content, "Notion"
            reasons.append("the file on the Notion invoice page is not a readable PDF")
        except Exception as exc:
            reasons.append(f"Notion file download failed ({type(exc).__name__})")
    if not google_mail.is_connected(INVOICE_MAILBOX):
        raise MailboxNotConnected("; ".join(reasons + [f"{INVOICE_MAILBOX} is not connected yet"]))
    if email_id and re.fullmatch(r"[0-9a-f]{12,20}", email_id.strip()):
        try:
            hit = _from_message(email_id.strip(), invoice_number, upc)
            if hit:
                return hit[0], hit[1], "the email Notion recorded"
            reasons.append("the email Notion recorded has no PDF showing this invoice")
        except Exception as exc:
            reasons.append(f"could not open the email Notion recorded ({type(exc).__name__})")
    if invoice_number:
        for mid in google_mail.search(INVOICE_MAILBOX, f'"{invoice_number}" has:attachment', 8):
            try:
                hit = _from_message(mid, invoice_number, upc)
            except Exception:
                continue
            if hit:
                return hit[0], hit[1], f"a search of {INVOICE_MAILBOX}"
        reasons.append(f"no email in {INVOICE_MAILBOX} has a PDF showing invoice {invoice_number}")
    raise LookupError("; ".join(reasons) or "no invoice number to look for")


def _shows(text: str, number: str) -> bool:
    """True when the PDF text contains the number, also when the PDF breaks it up with spaces."""
    return number in text or number in re.sub(r"\s+", "", text)


def fetch_by_upc(upc: str):
    """No invoice number known: search the invoices mailbox for a PDF that shows this UPC.
    Returns (filename, pdf bytes, where it came from) or raises LookupError."""
    if not upc:
        raise LookupError("no UPC to look for")
    if not google_mail.is_connected(INVOICE_MAILBOX):
        raise MailboxNotConnected(f"no supplier invoice in Notion for this UPC, and {INVOICE_MAILBOX} is not connected")
    seen = 0
    for mid in google_mail.search(INVOICE_MAILBOX, f'"{upc}" has:attachment', 10):
        seen += 1
        try:
            msg = google_mail.message(INVOICE_MAILBOX, mid)
        except Exception:
            continue
        for a in msg["attachments"]:
            if not (a["filename"].lower().endswith(".pdf") or "pdf" in a["mime"]) or a["size"] > MAX_PDF_BYTES * 1.4:
                continue
            try:
                data = google_mail.attachment(INVOICE_MAILBOX, mid, a["attachment_id"])
                if _is_pdf(data) and len(data) <= MAX_PDF_BYTES and _shows(pdf_text(data), upc):
                    return a["filename"], data, f"a search of {INVOICE_MAILBOX} for the UPC"
            except Exception:
                continue
    raise LookupError(f"no supplier invoice in Notion for this UPC, and no PDF in {INVOICE_MAILBOX} shows UPC {upc}"
                      + (f" ({seen} emails checked)" if seen else ""))
