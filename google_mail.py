"""
Read-only Gmail access for the reimbursement agents.

One Google sign-in app (GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET, user type Internal) and a
one-time sign-in per mailbox:

  1. Open  https://<service>/google/connect?mailbox=invoices@gefenoptical.com
  2. Sign in as that mailbox and approve "read your email".
  3. /google/status shows which mailboxes are connected.

Only the mailboxes in GOOGLE_ALLOWED_MAILBOXES can be connected; a sign-in as anyone else is
thrown away. The only permission requested is gmail.readonly: nothing here can send, delete
or change mail. Refresh tokens are stored encrypted (key derived from GOOGLE_CLIENT_SECRET),
in a table the read-only ledger login cannot see.

Env vars:
  GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
  GOOGLE_REDIRECT_URI        default https://removal-sync.onrender.com/google/callback
  GOOGLE_ALLOWED_MAILBOXES   default cases@gefenoptical.com,invoices@gefenoptical.com
"""

import base64
import hashlib
import hmac
import os
import time
from urllib.parse import urlencode

import requests
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse

import ledger

router = APIRouter(prefix="/google", tags=["google mail"])

SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL = "https://gmail.googleapis.com/gmail/v1/users/me"
STATE_TTL_S = 900

_access_cache: dict[str, tuple[str, float]] = {}


def _cfg():
    cid, secret = os.environ.get("GOOGLE_CLIENT_ID", ""), os.environ.get("GOOGLE_CLIENT_SECRET", "")
    if not cid or not secret:
        raise RuntimeError("GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET are not set")
    redirect = os.environ.get("GOOGLE_REDIRECT_URI", "https://removal-sync.onrender.com/google/callback")
    return cid, secret, redirect


def allowed_mailboxes() -> list[str]:
    raw = os.environ.get("GOOGLE_ALLOWED_MAILBOXES", "cases@gefenoptical.com,invoices@gefenoptical.com")
    return [m.strip().lower() for m in raw.split(",") if m.strip()]


def _fernet():
    from cryptography.fernet import Fernet
    key = base64.urlsafe_b64encode(hashlib.sha256(("mailbox-tokens:" + _cfg()[1]).encode()).digest())
    return Fernet(key)


def _sign(payload: str) -> str:
    return hmac.new(_cfg()[1].encode(), payload.encode(), hashlib.sha256).hexdigest()[:32]


def make_state(mailbox: str, now: float | None = None) -> str:
    payload = f"{mailbox}|{int(now or time.time())}"
    return base64.urlsafe_b64encode(f"{payload}|{_sign(payload)}".encode()).decode()


def read_state(state: str, now: float | None = None) -> str:
    try:
        mailbox, ts, sig = base64.urlsafe_b64decode(state.encode()).decode().split("|")
    except Exception:
        raise ValueError("bad state")
    if not hmac.compare_digest(sig, _sign(f"{mailbox}|{ts}")):
        raise ValueError("bad state signature")
    if (now or time.time()) - int(ts) > STATE_TTL_S:
        raise ValueError("this sign-in link expired; start again from /google/connect")
    return mailbox


def ensure_tables(conn):
    with ledger._SETUP_LOCK, conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS google_mailboxes (
                           email text PRIMARY KEY, refresh_token_enc text NOT NULL,
                           connected_at timestamptz NOT NULL DEFAULT now(), last_used_at timestamptz)""")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'ledger_reader'")
        if cur.fetchone():   # belt and braces: the read-only login must never see this table
            cur.execute("REVOKE ALL ON google_mailboxes FROM ledger_reader")


def connected() -> list[dict]:
    conn = ledger._db()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT email, connected_at, last_used_at FROM google_mailboxes ORDER BY email")
            return [{"mailbox": e, "connected_at": str(c), "last_used_at": str(u) if u else None}
                    for e, c, u in cur.fetchall()]
    finally:
        conn.close()


def is_connected(mailbox: str) -> bool:
    try:
        return any(m["mailbox"] == mailbox.lower() for m in connected())
    except Exception:
        return False


# ------------------------------------------------------------------ tokens ---

def access_token(mailbox: str) -> str:
    mailbox = mailbox.lower()
    hit = _access_cache.get(mailbox)
    if hit and hit[1] > time.time() + 60:
        return hit[0]
    cid, secret, _ = _cfg()
    conn = ledger._db()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_token_enc FROM google_mailboxes WHERE email = %s", (mailbox,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"{mailbox} is not connected yet (open /google/connect?mailbox={mailbox})")
            refresh = _fernet().decrypt(row[0].encode()).decode()
            r = requests.post(TOKEN_URL, timeout=30, data={"client_id": cid, "client_secret": secret,
                                                           "refresh_token": refresh, "grant_type": "refresh_token"})
            if not r.ok:
                raise RuntimeError(f"Google would not refresh access for {mailbox} ({r.status_code}). "
                                   f"Reconnect it at /google/connect?mailbox={mailbox}")
            cur.execute("UPDATE google_mailboxes SET last_used_at = now() WHERE email = %s", (mailbox,))
        data = r.json()
        _access_cache[mailbox] = (data["access_token"], time.time() + int(data.get("expires_in", 3000)))
        return data["access_token"]
    finally:
        conn.close()


def _gmail(mailbox: str, path: str, params: dict | None = None) -> dict:
    for attempt in range(4):
        r = requests.get(f"{GMAIL}{path}", headers={"Authorization": f"Bearer {access_token(mailbox)}"},
                         params=params, timeout=60)
        if r.status_code in (429, 500, 502, 503):
            time.sleep(2 + attempt * 3)
            continue
        if not r.ok:
            raise RuntimeError(f"Gmail {r.status_code}: {r.text[:200]}")
        return r.json()
    raise RuntimeError("Gmail kept failing; stopped")


# ----------------------------------------------------------------- reading ---

def search(mailbox: str, query: str, max_results: int = 10) -> list[str]:
    """Message ids matching a Gmail search, newest first."""
    data = _gmail(mailbox, "/messages", {"q": query, "maxResults": max_results})
    return [m["id"] for m in data.get("messages", [])]


def _walk(part: dict):
    yield part
    for p in part.get("parts", []) or []:
        yield from _walk(p)


def message(mailbox: str, message_id: str) -> dict:
    """{'subject', 'from', 'date_ms', 'snippet', 'attachments': [{'filename','mime','attachment_id','size'}]}"""
    m = _gmail(mailbox, f"/messages/{message_id}", {"format": "full"})
    headers = {h["name"].lower(): h["value"] for h in m.get("payload", {}).get("headers", [])}
    atts = []
    for p in _walk(m.get("payload", {})):
        body = p.get("body", {})
        if p.get("filename") and body.get("attachmentId"):
            atts.append({"filename": p["filename"], "mime": p.get("mimeType", ""),
                         "attachment_id": body["attachmentId"], "size": body.get("size", 0)})
    return {"id": m["id"], "thread_id": m.get("threadId"), "subject": headers.get("subject", ""),
            "from": headers.get("from", ""), "date_ms": int(m.get("internalDate", 0)),
            "snippet": m.get("snippet", ""), "attachments": atts}


def attachment(mailbox: str, message_id: str, attachment_id: str) -> bytes:
    data = _gmail(mailbox, f"/messages/{message_id}/attachments/{attachment_id}")
    return base64.urlsafe_b64decode(data["data"].encode())


# ------------------------------------------------------------------ routes ---

def _page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(f"""<!doctype html><meta charset="utf-8"><title>{title}</title>
<body style="font-family:system-ui;max-width:640px;margin:60px auto;line-height:1.5">
<h2>{title}</h2>{body}</body>""")


@router.get("/connect")
def connect(mailbox: str = Query("")):
    mailbox = mailbox.strip().lower()
    if mailbox not in allowed_mailboxes():
        links = "".join(f'<li><a href="/google/connect?mailbox={m}">Connect {m}</a></li>' for m in allowed_mailboxes())
        return _page("Connect a mailbox (read-only)", f"<p>Choose the mailbox to connect:</p><ul>{links}</ul>"
                     "<p>You will be asked to sign in to Google <b>as that mailbox</b>.</p>")
    try:
        cid, _, redirect = _cfg()
    except RuntimeError as exc:
        raise HTTPException(500, str(exc))
    q = {"client_id": cid, "redirect_uri": redirect, "response_type": "code", "scope": SCOPE,
         "access_type": "offline", "prompt": "consent", "login_hint": mailbox,
         "include_granted_scopes": "false", "state": make_state(mailbox)}
    return RedirectResponse(f"{AUTH_URL}?{urlencode(q)}")


@router.get("/callback")
def callback(code: str = Query(""), state: str = Query(""), error: str = Query("")):
    if error or not code:
        return _page("Not connected", f"<p>Google did not grant access ({error or 'no code'}). Nothing was saved.</p>"
                     '<p><a href="/google/connect">Try again</a></p>')
    try:
        wanted = read_state(state)
        cid, secret, redirect = _cfg()
    except Exception as exc:
        return _page("Not connected", f"<p>{exc}</p><p><a href=\"/google/connect\">Start again</a></p>")
    r = requests.post(TOKEN_URL, timeout=30, data={"code": code, "client_id": cid, "client_secret": secret,
                                                   "redirect_uri": redirect, "grant_type": "authorization_code"})
    if not r.ok:
        return _page("Not connected", f"<p>Google refused the sign-in ({r.status_code}). Nothing was saved.</p>")
    tok = r.json()
    prof = requests.get(f"{GMAIL}/profile", headers={"Authorization": f"Bearer {tok.get('access_token', '')}"}, timeout=30)
    email = (prof.json().get("emailAddress", "") if prof.ok else "").lower()
    if email != wanted or email not in allowed_mailboxes():
        return _page("Not connected", f"<p>You signed in as <b>{email or 'an unknown account'}</b>, but this link was for "
                     f"<b>{wanted}</b>. Nothing was saved.</p><p>Sign out of Google (or use a private window) and "
                     f'<a href="/google/connect?mailbox={wanted}">try again</a>.</p>')
    if not tok.get("refresh_token"):
        return _page("Not connected", "<p>Google did not return a long-lived token. Nothing was saved.</p>"
                     f'<p><a href="/google/connect?mailbox={wanted}">Try again</a></p>')
    conn = ledger._db()
    try:
        ensure_tables(conn)
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO google_mailboxes (email, refresh_token_enc) VALUES (%s, %s)
                           ON CONFLICT (email) DO UPDATE SET refresh_token_enc = EXCLUDED.refresh_token_enc,
                                                             connected_at = now()""",
                        (email, _fernet().encrypt(tok["refresh_token"].encode()).decode()))
    finally:
        conn.close()
    _access_cache.pop(email, None)
    rest = [m for m in allowed_mailboxes() if not is_connected(m)]
    nxt = "".join(f'<li><a href="/google/connect?mailbox={m}">Connect {m}</a> (sign in as that mailbox)</li>' for m in rest)
    return _page("Connected", f"<p><b>{email}</b> is connected, read-only.</p>"
                 + (f"<p>Still to connect:</p><ul>{nxt}</ul>" if rest else "<p>All mailboxes are connected. You can close this page.</p>"))


@router.get("/status")
def status():
    try:
        return {"client_configured": bool(os.environ.get("GOOGLE_CLIENT_ID") and os.environ.get("GOOGLE_CLIENT_SECRET")),
                "allowed": allowed_mailboxes(), "connected": connected()}
    except Exception as exc:
        raise HTTPException(500, f"{type(exc).__name__}: {exc}")
