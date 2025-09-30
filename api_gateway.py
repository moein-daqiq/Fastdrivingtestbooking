# api_gateway.py
import os
import json
import html
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
# NEW: stale searching reclaim window (minutes). Default 5.
STALE_SEARCH_MIN = int(os.environ.get("STALE_SEARCH_MIN", "5"))

stripe.api_key = STRIPE_SECRET_KEY
stripe.max_network_retries = 2

# ---------- APP ----------
app = FastAPI(title="FastDrivingTestFinder API", version="1.5.0")

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
            queued_at TEXT,
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
        "created_at","updated_at","queued_at","status","last_event","paid",
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

    # One-off backfill so fresh paid jobs don't look stale to worker
    try:
        cur.execute("""
            UPDATE searches
               SET queued_at = COALESCE(queued_at, updated_at, created_at)
             WHERE paid = 1 AND queued_at IS NULL
        """)
    except Exception:
        pass

    # Idempotency table for Stripe events
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_events (
            id TEXT PRIMARY KEY,
            type TEXT,
            created_at TEXT
        )
    """)

    # NEW: Global worker controls (kill switch / pause_all)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS worker_controls (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            pause_all INTEGER DEFAULT 0,
            updated_at TEXT
        )
    """)
    # Ensure singleton row exists
    cur.execute("INSERT OR IGNORE INTO worker_controls (id, pause_all, updated_at) VALUES (1, 0, ?)", (now_iso(),))

    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_status ON searches(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_paid ON searches(paid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_expires ON searches(expires_at)")
    conn.commit()
    conn.close()

migrate()

# ---------- MODELS ----------
class SearchIn(BaseModel):
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
    email: Optional[str] = None
    booking_type: Optional[Literal["new","swap"]] = None

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

# NEW: Worker controls + Admin actions
class AdminPauseToggle(BaseModel):
    pause_all: bool

class AdminStopSome(BaseModel):
    ids: List[int] = Field(default_factory=list)

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
    cur = conn.cursor()
    ts = now_iso()
    cur.execute("""
        UPDATE searches
           SET status='expired', last_event='auto_expired', updated_at=?
         WHERE paid=0 AND expires_at IS NOT NULL AND expires_at < ?
           AND status IN ('pending_payment','new')
    """, (ts, ts))
    conn.commit()

# ---- Amount helpers ----
def _band_ok(amount_cents: int, base_cents: int) -> bool:
    """Accept whole-pound amounts at/above base in £30 steps."""
    if amount_cents <= 0 or amount_cents % 100 != 0:
        return False
    if amount_cents < base_cents:
        return False
    return (amount_cents - base_cents) % 3000 == 0

def _infer_booking_type(amount_cents: int) -> Optional[str]:
    if amount_cents >= 15000:
        return "new"
    if amount_cents >= 7000:
        return "swap"
    return None

# ---- Global controls helpers ----
def _get_pause_all(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT pause_all FROM worker_controls WHERE id=1")
    row = cur.fetchone()
    return bool(row["pause_all"]) if row else False

def _set_pause_all(conn: sqlite3.Connection, value: bool):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO worker_controls (id, pause_all, updated_at)
        VALUES (1, ?, ?)
        ON CONFLICT(id) DO UPDATE SET pause_all=excluded.pause_all, updated_at=excluded.updated_at
    """, (1 if value else 0, now_iso()))
    conn.commit()

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
    notes
