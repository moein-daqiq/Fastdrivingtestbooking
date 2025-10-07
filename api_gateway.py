# api_gateway.py
import os
import json
import html
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional

from zoneinfo import ZoneInfo  # for Europe/London conversion

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
# stale reclaim window (minutes). Default 5.
STALE_SEARCH_MIN = int(os.environ.get("STALE_SEARCH_MIN", "5"))

stripe.api_key = STRIPE_SECRET_KEY
stripe.max_network_retries = 2

# ---------- APP ----------
app = FastAPI(title="FastDrivingTestFinder API", version="1.7.1")

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

    # searches table
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

    # ensure queued_at backfill
    try:
        cur.execute("""
            UPDATE searches
               SET queued_at = COALESCE(queued_at, updated_at, created_at)
             WHERE paid = 1 AND queued_at IS NULL
        """)
    except Exception:
        pass

    # controls table (global pause + priority centres)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_controls (
            id INTEGER PRIMARY KEY CHECK (id=1),
            pause_all INTEGER DEFAULT 0,
            priority_centres TEXT DEFAULT ''
        )
    """)
    cur.execute("INSERT OR IGNORE INTO admin_controls (id, pause_all, priority_centres) VALUES (1, 0, '')")

    # Stripe events idempotency
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stripe_events (
            id TEXT PRIMARY KEY,
            type TEXT,
            created_at TEXT
        )
    """)

    # worker_status table for /api/worker/pulse
    cur.execute("""
        CREATE TABLE IF NOT EXISTS worker_status (
            worker_id TEXT PRIMARY KEY,
            last_seen TEXT NOT NULL,
            state TEXT,
            note TEXT,
            rps REAL,
            jobs_active INTEGER
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
    surname: Optional[str] = None

    centres: Any = Field(default_factory=list)   # list[str] or CSV
    options: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None

class StartSearchIn(BaseModel):
    email: Optional[str] = None
    booking_type: Optional[Literal["new","swap"]] = None

class PayCreateIntentIn(BaseModel):
    search_id: Optional[int] = None
    amount_cents: Optional[int] = None
    amount:      Optional[int] = None
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

class AdminControlsIn(BaseModel):
    pause_all: Optional[bool] = None
    priority_centres: Optional[List[str] | str] = None

# admin bulk pause/resume payload
class AdminBatchIds(BaseModel):
    ids: List[int]

# worker upgrade NEW -> SWAP payload
class UpgradeToSwapIn(BaseModel):
    booking_reference: str
    desired_centres: Optional[List[str]] = None  # keep existing if None

# worker pulse payload
class WorkerPulseIn(BaseModel):
    worker_id: str
    state: Optional[str] = "idle"
    note: Optional[str] = None
    rps: Optional[float] = None
    jobs_active: Optional[int] = None

# ---------- HELPERS ----------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return utcnow().replace(microsecond=0).isoformat()

def plus_minutes_iso(m: int) -> str:
    return (utcnow() + timedelta(minutes=m)).replace(microsecond=0).isoformat()

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
        raise HTTPException(status_code=401, detail="Auth required")
    import base64
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=401, detail="Bad auth")
    user, _, pwd = decoded.partition(":")
    if user != ADMIN_USER or pwd != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Invalid credentials")

def _verify_worker(authorization: str | None = Header(default=None)):
    if not WORKER_TOKEN:
        return True
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1]
    if token != WORKER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")
    return True

def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default

def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def _expire_unpaid(conn: sqlite3.Connection):
    cur = conn.cursor()
    ts = now_iso()
    cur.execute("""
        UPDATE searches
           SET status='expired', last_event='auto_expired', updated_at=?
         WHERE paid=0 AND expires_at IS NOT NULL AND expires_at < ?
           AND status IN ('pending_payment','new')
    """, (ts, ts))
    conn.commit()

# small helpers for live status
def _parse_iso_safe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def live_status_for(row: sqlite3.Row) -> str:
    """
    Derives a compact, human-readable live status for a search row based on:
    - status, updated_at age, last_event markers, and active cooldown (expires_at)
    """
    status = (row["status"] or "").lower()
    if status == "pending_payment":
        return "Awaiting Payment"

    now = utcnow()
    updated = _parse_iso_safe(row["updated_at"]) or (now - timedelta(days=365))
    age_min = (now - updated).total_seconds() / 60.0

    last_event = (row["last_event"] or "").lower()
    expires_at = _parse_iso_safe(row["expires_at"])

    # Cooldowns
    if last_event.startswith("layout_issue:") and expires_at and expires_at > now:
        return "Layout Cooldown"

    if "captcha" in last_event:
        return "Captcha Cooldown"
    if "ip_blocked" in last_event or "blocked" in last_event:
        return "IP Blocked"
    if "slot_found" in last_event:
        return "Slot Found (awaiting action)"

    # Stale must win over "Scanning"
    if age_min >= STALE_SEARCH_MIN:
        return f"Stale (>{STALE_SEARCH_MIN}m)"

    if "worker_claimed" in last_event or status == "searching":
        return "Scanning"

    return "Idle / Queued"

# ---- Amount helpers ----
def _band_ok(amount_cents: int, base_cents: int) -> bool:
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

# ====================== SEARCH-ID CREATION (SERVER) ======================
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
    cur.execute(
        """
        INSERT INTO searches (
            created_at, updated_at, status, last_event, paid,
            booking_type, licence_number, booking_reference, theory_pass,
            date_window_from, date_window_to, time_window_from, time_window_to,
            phone, whatsapp, email, centres_json, options_json, notes, expires_at
        ) VALUES (?,?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?, ?,?,?)
        """,
        (
            ts, ts, status, last_event, 0,
            booking_type, licence_number, booking_reference, theory_pass,
            date_window_from, date_window_to, time_window_from, time_window_to,
            phone, whatsapp, email, centres_json, options_json, notes, expires,
        ),
    )
    search_id = cur.lastrowid
    conn.commit()
    return {"search_id": search_id, "status": status, "expires_at": expires}
# ==================== END SEARCH-ID CREATION BLOCK =====================

# ---------- HEALTH ----------
@app.get("/api/health")
def health():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT MAX(last_seen) AS last FROM worker_status")
    row = cur.fetchone()
    last = row["last"] if row else None
    conn.close()
    return {"ok": True, "ts": now_iso(), "version": "1.7.1", "last_worker_pulse": last}

# ---------- CONTROLS (Admin + Worker) ----------
def _get_controls(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT pause_all, priority_centres FROM admin_controls WHERE id=1")
    row = cur.fetchone()
    if not row:
        return {"pause_all": False, "priority_centres": []}
    pcs = [s.strip() for s in (row["priority_centres"] or "").split(",") if s.strip()]
    return {"pause_all": bool(row["pause_all"]), "priority_centres": pcs}

def _set_controls(conn: sqlite3.Connection, pause_all: Optional[bool], priority_centres: Optional[List[str] | str]):
    cur = conn.cursor()
    fields = []
    vals: list[Any] = []
    if pause_all is not None:
        fields.append("pause_all=?")
        vals.append(1 if pause_all else 0)
    if priority_centres is not None:
        if isinstance(priority_centres, list):
            pc = ",".join([s.strip().lower() for s in priority_centres if str(s).strip()])
        else:
            pc = ",".join([s.strip().lower() for s in str(priority_centres).split(",") if s.strip()])
        fields.append("priority_centres=?")
        vals.append(pc)
    if fields:
        vals.append(1)
        cur.execute(f"UPDATE admin_controls SET {', '.join(fields)} WHERE id=?", vals)
        conn.commit()

@app.get("/api/worker/controls")
def worker_controls(_: bool = Depends(_verify_worker)):
    conn = get_conn()
    controls = _get_controls(conn)
    conn.close()

    dvsa_rps = max(0.2, min(2.0, _env_float("DVSA_RPS", 0.5)))
    dvsa_jitter = max(0.0, min(0.9, _env_float("DVSA_RPS_JITTER", 0.35)))
    target_interval_sec = round(1.0 / dvsa_rps, 3)

    headless = _env_bool("DVSA_HEADLESS", True)
    nav_timeout_ms = int(os.environ.get("DVSA_NAV_TIMEOUT_MS", "25000"))
    ready_max_ms = int(os.environ.get("DVSA_READY_MAX_MS", "30000"))
    post_nav_settle_ms = int(os.environ.get("DVSA_POST_NAV_SETTLE_MS", "800"))
    click_settle_ms = int(os.environ.get("DVSA_CLICK_SETTLE_MS", "700"))

    url_change = os.environ.get("DVSA_URL_CHANGE", "")
    url_book = os.environ.get("DVSA_URL_BOOK", "")
    ua_full = os.environ.get("DVSA_USER_AGENT", "")
    ua_short = (ua_full[:80] + "...") if ua_full else ""

    api_base = os.environ.get("API_BASE", "")
    pw_path = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "")

    return {
        "pause_all": controls["pause_all"],
        "priority_centres": controls["priority_centres"],
        "dvsa": {
            "rps": dvsa_rps,
            "jitter": dvsa_jitter,
            "target_interval_sec": target_interval_sec,
            "headless": headless,
            "timeouts": {
                "nav_timeout_ms": nav_timeout_ms,
                "ready_max_ms": ready_max_ms,
                "post_nav_settle_ms": post_nav_settle_ms,
                "click_settle_ms": click_settle_ms,
            },
            "urls": {"change": url_change, "book": url_book},
            "user_agent": ua_short,
        },
        "service": {"api_base": api_base, "playwright_path": pw_path},
    }

@app.get("/api/admin/controls")
def admin_get_controls(request: Request):
    require_admin(request)
    conn = get_conn()
    data = _get_controls(conn)
    conn.close()
    return data

@app.post("/api/admin/controls")
def admin_set_controls(payload: AdminControlsIn, request: Request):
    require_admin(request)
    conn = get_conn()
    _set_controls(conn, payload.pause_all, payload.priority_centres)
    data = _get_controls(conn)
    conn.close()
    return data

# ---------- ROUTES ----------
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
        last_event="created",
    )
    conn.close()
    return {"ok": True, **rec}

@app.post("/api/search")
async def create_search(payload: SearchIn):
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
        last_event="created",
    )
    conn.close()
    return {"ok": True, **rec}

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

@app.options("/api/pay/create-intent")
async def options_pay_create_intent():
    return Response(status_code=204)

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

    cur.execute("SELECT id, email, booking_type, status, paid FROM searches WHERE id = ?", (sid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=500, detail="internal: draft create failed")

    amount = int(amount)

    if not (_band_ok(amount, 7000) or _band_ok(amount, 15000)):
        conn.close()
        raise HTTPException(status_code=400, detail="amount not in allowed range")

    inferred = _infer_booking_type(amount)
    existing_type = (row["booking_type"] or "").strip() or None


