# api_gateway.py
import os
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

import stripe
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

# ---------- ENV ----------
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
DB_FILE = os.environ.get("SEARCHES_DB", "searches.db")
ADMIN_USER = os.environ.get("ADMIN_BASIC_AUTH_USER")
ADMIN_PASS = os.environ.get("ADMIN_BASIC_AUTH_PASS")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

stripe.api_key = STRIPE_SECRET_KEY
stripe.max_network_retries = 2

# ---------- APP ----------
app = FastAPI(title="FastDrivingTestFinder API", version="1.4.3")

# CORS: allow any subdomain of fastdrivingtestfinder.co.uk + localhost dev
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^https?://([a-z0-9-]+\.)?fastdrivingtestfinder\.co\.uk$|^http://localhost(:\d+)?$",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- DB ----------
def get_conn():
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def migrate():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'pending_payment',
            last_event TEXT,
            paid INTEGER DEFAULT 0,
            payment_intent_id TEXT,
            amount INTEGER,
            booking_type TEXT,
            licence_number TEXT,
            booking_reference TEXT,
            theory_pass TEXT,
            date_window_from TEXT,
            date_window_to TEXT,
            time_window_from TEXT,
            time_window_to TEXT,
            phone TEXT,
            whatsapp TEXT,
            email TEXT,
            centres_json TEXT,
            options_json TEXT,
            notes TEXT,
            expires_at TEXT
        )
    """)
    expected_cols = {
        "created_at","updated_at","status","last_event","paid",
        "payment_intent_id","amount",
        "booking_type","licence_number","booking_reference","theory_pass",
        "date_window_from","date_window_to","time_window_from","time_window_to",
        "phone","whatsapp","email","centres_json","options_json","notes","expires_at"
    }
    cur.execute("PRAGMA table_info(searches)")
    have = {row["name"] for row in cur.fetchall()}
    missing = expected_cols - have
    for col in missing:
        coltype = "INTEGER" if col in {"paid","amount"} else "TEXT"
        cur.execute(f"ALTER TABLE searches ADD COLUMN {col} {coltype}")

    # Idempotency table for Stripe events
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_events (
            id TEXT PRIMARY KEY,
            type TEXT,
            created_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_status ON searches(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_paid ON searches(paid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_expires ON searches(expires_at)")
    conn.commit()
    conn.close()

migrate()

# ---------- MODELS ----------
class SearchIn(BaseModel):
    # NEW: allow FE to update an existing draft/paid record
    search_id: Optional[int] = None

    booking_type: Literal["new", "swap"] = "new"
    licence_number: Optional[str] = None
    booking_reference: Optional[str] = None
    theory_pass: Optional[str] = None

    date_window_from: Optional[str] = Field(None, description="YYYY-MM-DD")
    date_window_to: Optional[str]   = Field(None, description="YYYY-MM-DD")
    time_window_from: Optional[str] = Field(None, description="HH:MM (24h)")
    time_window_to: Optional[str]   = Field(None, description="HH:MM (24h)")

    phone: Optional[str] = None
    whatsapp: Optional[str] = None
    email: Optional[str] = None
    surname: Optional[str] = None  # for front-end prepay create

    centres: Any = Field(default_factory=list)   # list[str] or CSV
    options: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None

class StartSearchIn(BaseModel):
    """Minimal payload used by /api/search/start to pre-create a draft record."""
    email: Optional[str] = None
    booking_type: Optional[Literal["new","swap"]] = None

# ---------- CHANGED: search_id optional, optional email for metadata ----------
class PayCreateIntentIn(BaseModel):
    search_id: Optional[int] = None
    amount_cents: Optional[int] = None  # prefer cents
    amount:      Optional[int] = None   # legacy
    currency: str = "gbp"
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    email: Optional[str] = None

class WorkerClaimRequest(BaseModel):
    limit: int = 8

class WorkerEvent(BaseModel):
    event: str

class WorkerStatus(BaseModel):
    status: str
    event: str

# ---------- HELPERS ----------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return utcnow().replace(microsecond=0).isoformat()

def json_dumps(v) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "null"

def _parse_centres(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        return [s.strip() for s in value.split(",") if s.strip()]
    return []

def require_admin(request: Request):
    if not ADMIN_USER or not ADMIN_PASS:
        return
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate": "Basic"})
    import base64
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=401, detail="Bad auth", headers={"WWW-Authenticate": "Basic"})
    user, _, pwd = decoded.partition(":")
    if user != ADMIN_USER or pwd != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Invalid credentials", headers={"WWW-Authenticate": "Basic"})

def _verify_worker(authorization: str | None = Header(default=None)):
    if not WORKER_TOKEN:
        return True
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != WORKER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    return True

def _expire_unpaid(conn: sqlite3.Connection):
    """Mark unpaid searches past expires_at as expired."""
    cur = conn.cursor()  # <<< FIXED: was `conn.cursor.` causing a runtime error
    ts = now_iso()
    cur.execute("""
        UPDATE searches
           SET status='expired', last_event='auto_expired', updated_at=?
         WHERE paid=0 AND expires_at IS NOT NULL AND expires_at < ?
           AND status IN ('pending_payment','new')
    """, (ts, ts))
    conn.commit()

# ---- New band helpers (replace strict _price_is_plausible) ----
def _band_ok(amount_cents: int, base_cents: int) -> bool:
    """Accept whole-pound amounts at/above base in £30 (3000¢) steps."""
    if amount_cents <= 0 or amount_cents % 100 != 0:
        return False
    if amount_cents < base_cents:
        return False
    return (amount_cents - base_cents) % 3000 == 0

def _infer_booking_type(amount_cents: int) -> Optional[str]:
    """Infer booking type purely from amount band."""
    if amount_cents >= 15000:
        return "new"
    if amount_cents >= 7000:
        return "swap"
    return None

# ============================  SEARCH-ID CREATION (SERVER)  ============================
def _create_search_record(
    conn: sqlite3.Connection,
    *,
    booking_type: Optional[str] = None,
    licence_number: Optional[str] = None,
    booking_reference: Optional[str] = None,
    theory_pass: Optional[str] = None,
    date_window_from: Optional[str] = None,
    date_window_to: Optional[str] = None,
    time_window_from: Optional[str] = None,
    time_window_to: Optional[str] = None,
    phone: Optional[str] = None,
    whatsapp: Optional[str] = None,
    email: Optional[str] = None,
    centres_json: str = "[]",
    options_json: str = "{}",
    notes: Optional[str] = None,
    status: str = "pending_payment",
    last_event: str = "created"
) -> Dict[str, Any]:
    ts = now_iso()
    expires = (utcnow() + timedelta(minutes=30)).replace(microsecond=0).isoformat()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO searches (
            created_at, updated_at, status, last_event, paid,
            booking_type, licence_number, booking_reference, theory_pass,
            date_window_from, date_window_to, time_window_from, time_window_to,
            phone, whatsapp, email, centres_json, options_json, notes, expires_at
        ) VALUES (?,?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?, ?,?,?)
    """, (
        ts, ts, status, last_event, 0,
        booking_type, licence_number, booking_reference, theory_pass,
        date_window_from, date_window_to, time_window_from, time_window_to,
        phone, whatsapp, email, centres_json, options_json, notes, expires
    ))
    search_id = cur.lastrowid
    conn.commit()
    return {"search_id": search_id, "status": status, "expires_at": expires}
# ==========================  END SEARCH-ID CREATION BLOCK  ===========================

# ---------- HEALTH ----------
@app.get("/api/health")
def health():
    return {"ok": True, "ts": now_iso(), "version": "1.4.3"}

# ---------- ROUTES ----------

# Tiny lookup so the FE can verify cached IDs
@app.get("/api/search/{sid}")
def get_search(sid: int):
    conn = get_conn()
    _expire_unpaid(conn)
    cur = conn.cursor()
    cur.execute("SELECT id, status, paid, email, booking_type, created_at, updated_at FROM searches WHERE id = ?", (sid,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    return {
        "ok": True,
        "id": row["id"],
        "status": row["status"],
        "paid": bool(row["paid"]),
        "booking_type": row["booking_type"],
        "email": row["email"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }

# Minimal draft creator for the Payment section
@app.post("/api/search/start")
def start_search(payload: StartSearchIn):
    conn = get_conn()
    _expire_unpaid(conn)
    rec = _create_search_record(
        conn,
        booking_type=payload.booking_type,
        email=payload.email,
        centres_json="[]",
        options_json="{}",
        notes=None,
        status="pending_payment",
        last_event="created"
    )
    conn.close()
    return {"ok": True, **rec}

@app.post("/api/search")
async def create_search(payload: SearchIn):
    """
    If `payload.search_id` is provided, update that existing row (typically after payment).
    Otherwise, create a new pending draft (original behavior).
    """
    centres_list = _parse_centres(payload.centres)
    conn = get_conn()
    _expire_unpaid(conn)
    cur = conn.cursor()

    if payload.search_id:
        cur.execute("SELECT id FROM searches WHERE id=?", (payload.search_id,))
        row = cur.fetchone()
        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="search_id not found")

        cur.execute("""
            UPDATE searches SET
                booking_type=?,
                licence_number=?,
                booking_reference=?,
                theory_pass=?,
                date_window_from=?,
                date_window_to=?,
                time_window_from=?,
                time_window_to=?,
                phone=?,
                whatsapp=?,
                email=?,
                centres_json=?,
                options_json=?,
                notes=?,
                updated_at=?
            WHERE id=?
        """, (
            payload.booking_type,
            payload.licence_number,
            payload.booking_reference,
            payload.theory_pass,
            payload.date_window_from,
            payload.date_window_to,
            payload.time_window_from,
            payload.time_window_to,
            payload.phone,
            payload.whatsapp,
            payload.email,
            json_dumps(centres_list),
            json_dumps(payload.options),
            payload.notes,
            now_iso(),
            payload.search_id
        ))
        conn.commit()
        conn.close()
        return {"ok": True, "search_id": payload.search_id}

    # Insert new (unchanged path)
    rec = _create_search_record(
        conn,
        booking_type=payload.booking_type,
        licence_number=payload.licence_number,
        booking_reference=payload.booking_reference,
        theory_pass=payload.theory_pass,
        date_window_from=payload.date_window_from,
        date_window_to=payload.date_window_to,
        time_window_from=payload.time_window_from,
        time_window_to=payload.time_window_to,
        phone=payload.phone,
        whatsapp=payload.whatsapp,
        email=payload.email,
        centres_json=json_dumps(centres_list),
        options_json=json_dumps(payload.options),
        notes=payload.notes,
        status="pending_payment",
        last_event="created"
    )
    conn.close()
    return {"ok": True, **rec}

# Disable (floating “Stop searching” button)
@app.post("/api/search/{sid}/disable")
def disable_search(sid: int, _: bool = Depends(_verify_worker)):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches
           SET status='disabled', last_event='manually_disabled', updated_at=?
         WHERE id=? AND status NOT IN ('booked','failed')
    """, (now_iso(), sid))
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Not found or cannot disable")
    return {"ok": True, "id": sid, "status": "disabled"}

# Explicit OPTIONS (some proxies like it)
@app.options("/api/pay/create-intent")
async def options_pay_create_intent():
    return Response(status_code=204)

# ---------- CHANGED: auto-create (or auto-recreate) a draft if search_id is missing/bad ----------
@app.post("/api/pay/create-intent")
async def pay_create_intent(body: PayCreateIntentIn, request: Request):
    amount = body.amount_cents if body.amount_cents is not None else body.amount
    if amount is None or amount <= 0:
        raise HTTPException(status_code=400, detail="amount_cents (or amount) must be > 0")

    conn = get_conn()
    _expire_unpaid(conn)
    cur = conn.cursor()

    def _make_draft(email: Optional[str], last_event: str) -> int:
        rec = _create_search_record(
            conn,
            email=email,
            centres_json="[]",
            options_json="{}",
            notes=None,
            status="pending_payment",
            last_event=last_event
        )
        return rec["search_id"]

    # Resolve/repair search id (create when missing or invalid)
    if body.search_id is None:
        sid = _make_draft(body.email, "created_via_create_intent")
    else:
        cur.execute("SELECT id, email, booking_type, status, paid FROM searches WHERE id = ?", (body.search_id,))
        row = cur.fetchone()
        if not row:
            sid = _make_draft(body.email, "recreated_via_create_intent_not_found")
        elif row["paid"]:
            sid = _make_draft(row["email"] or body.email, "recreated_via_create_intent_already_paid")
        elif row["status"] in ("disabled", "expired", "failed", "booked"):
            sid = _make_draft(row["email"] or body.email, f"recreated_via_create_intent_{row['status']}")
        else:
            sid = row["id"]

    # Re-fetch canonical row
    cur.execute("SELECT id, email, booking_type, status, paid FROM searches WHERE id = ?", (sid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=500, detail="internal: draft create failed")

    amount = int(amount)

    # Amount guardrails: accept £70/£150 base and +£30 steps
    if not (_band_ok(amount, 7000) or _band_ok(amount, 15000)):
        conn.close()
        raise HTTPException(status_code=400, detail="amount not in allowed range")

    # Infer/repair booking_type if needed
    inferred = _infer_booking_type(amount)
    existing_type = (row["]()_
