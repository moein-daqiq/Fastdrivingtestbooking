# api_gateway.py
# FastDTF API Gateway — with Admin "Live Status" column and /worker/pulse
# Compatible with Python 3.11+ (uses zoneinfo)

import os
import json
import html
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Optional, Tuple

from zoneinfo import ZoneInfo

import stripe
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

# ========================== ENV ==========================
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
DB_FILE = os.environ.get("SEARCHES_DB", "searches.db")

ADMIN_USER = os.environ.get("ADMIN_BASIC_AUTH_USER")
ADMIN_PASS = os.environ.get("ADMIN_BASIC_AUTH_PASS")

WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

# stale reclaim window (minutes). Default 5.
STALE_SEARCH_MIN = int(os.environ.get("STALE_SEARCH_MIN", "5"))

# optional CORS (adjust as needed)
ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "*").split(",")

stripe.api_key = STRIPE_SECRET_KEY or None

app = FastAPI(title="FastDTF API Gateway", version="2025.10.07")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================== TIME HELPERS ==========================
UTC = timezone.utc
LON = ZoneInfo("Europe/London")

def utcnow() -> datetime:
    return datetime.now(UTC)

def london_now() -> datetime:
    return utcnow().astimezone(LON)

def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat()

def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

# ---- minimal additions used only to harden admin rendering ----
def parse_iso_safe(s: Optional[str]) -> Optional[datetime]:
    if not s or not str(s).strip():
        return None
    try:
        return datetime.fromisoformat(str(s).strip())
    except Exception:
        return None

def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))

def aware(dt: Optional[datetime]) -> datetime:
    # Ensure timezone-aware (assume UTC if naive); fallback to now if None
    if dt is None:
        return utcnow()
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt
# ----------------------------------------------------------------

# ========================== DB ==========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db() -> None:
    con = db()
    cur = con.cursor()
    # Primary searches table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS searches (
      id TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      booking_type TEXT CHECK(booking_type IN ('new','swap')) NOT NULL,
      status TEXT NOT NULL, -- pending_payment | active | paused | completed | cancelled | error
      amount_cents INTEGER DEFAULT 0,
      payment_intent_id TEXT,
      paid_at TEXT,
      -- User/applicant fields (extend as you need)
      first_name TEXT,
      last_name TEXT,
      phone TEXT,
      email TEXT,
      licence_number TEXT,
      booking_reference TEXT, -- for swap
      theory_pass TEXT,       -- for new
      centre_prefs TEXT,      -- JSON list of centre ids/names
      date_window_from TEXT,
      date_window_to TEXT,
      -- Worker lock
      locked_by TEXT,
      locked_at TEXT,
      lock_expires_at TEXT,
      -- Live feed
      last_event TEXT,
      last_event_at TEXT
    )
    """)
    # Stripe events (for audit)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stripe_events (
      id TEXT PRIMARY KEY,
      type TEXT,
      created_at TEXT NOT NULL,
      payload TEXT NOT NULL
    )
    """)
    # Worker status table (for /worker/pulse)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS worker_status (
      worker_id TEXT PRIMARY KEY,
      last_seen TEXT NOT NULL,
      state TEXT,    -- e.g., idle | scanning | cooldown | blocked | error
      note TEXT,
      rps REAL,
      jobs_active INTEGER
    )
    """)
    # Admin events audit (optional)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS admin_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at TEXT NOT NULL,
      search_id TEXT,
      worker_id TEXT,
      kind TEXT,
      details TEXT
    )
    """)
    con.commit()
    con.close()

init_db()

# ========================== MODELS ==========================
class CreateSearchBody(BaseModel):
    booking_type: Literal["new", "swap"]
    amount_cents: int = Field(ge=0)
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    licence_number: Optional[str] = None
    booking_reference: Optional[str] = None
    theory_pass: Optional[str] = None
    centre_prefs: Optional[List[str]] = None
    date_window_from: Optional[str] = None  # ISO yyyy-mm-dd or datetime
    date_window_to: Optional[str] = None

class WorkerPulseBody(BaseModel):
    worker_id: str
    state: Optional[str] = "idle"
    note: Optional[str] = None
    rps: Optional[float] = None
    jobs_active: Optional[int] = None

class WorkerEventBody(BaseModel):
    worker_id: str
    search_id: str
    kind: str  # checked:<centre> no_slots|<count> | captcha_cooldown:<centre|stage> | slot_found | booked | ip_blocked | service_closed | login_form_ready | error
    details: Optional[str] = None

class NextJobResponse(BaseModel):
    search_id: Optional[str]
    payload: Optional[Dict[str, Any]] = None  # include applicant & prefs
    lock_expires_at: Optional[str] = None

# ========================== AUTH HELPERS ==========================
def require_admin(request: Request) -> None:
    if not ADMIN_USER or not ADMIN_PASS:
        # If admin auth not set, deny by default (safer)
        raise HTTPException(status_code=403, detail="Admin locked (no credentials configured).")
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Basic "):
        raise HTTPException(status_code=401, detail="Basic auth required", headers={"WWW-Authenticate": "Basic"})
    import base64
    try:
        decoded = base64.b64decode(auth[6:]).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid auth", headers={"WWW-Authenticate": "Basic"})
    if ":" not in decoded:
        raise HTTPException(status_code=401, detail="Invalid auth format", headers={"WWW-Authenticate": "Basic"})
    user, pw = decoded.split(":", 1)
    if user != ADMIN_USER or pw != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Unauthorized", headers={"WWW-Authenticate": "Basic"})

def require_worker(request: Request) -> str:
    token = request.headers.get("X-Worker-Token") or request.headers.get("Authorization")
    # Allow "Bearer <token>" or raw header equal to token
    if token and token.startswith("Bearer "):
        token = token[7:].strip()
    if not WORKER_TOKEN or token != WORKER_TOKEN:
        raise HTTPException(status_code=401, detail="Worker token invalid.")
    # worker may also pass X-Worker-Id for routing/metrics
    wid = request.headers.get("X-Worker-Id") or "worker-1"
    return wid

# ========================== CORE HELPERS ==========================
def new_id(prefix: str) -> str:
    return f"{prefix}_{int(utcnow().timestamp())}_{os.urandom(3).hex()}"

def lock_is_stale(row: sqlite3.Row) -> bool:
    le = row["lock_expires_at"]
    if not le:
        return True
    try:
        return parse_iso(le) <= utcnow()
    except Exception:
        return True

def reclaim_stale_locks(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    now = utcnow()
    cur.execute("SELECT id, lock_expires_at FROM searches WHERE lock_expires_at IS NOT NULL")
    rows = cur.fetchall()
    reclaimed = 0
    for r in rows:
        try:
            if parse_iso(r["lock_expires_at"]) <= now:
                cur.execute("UPDATE searches SET locked_by=NULL, locked_at=NULL, lock_expires_at=NULL, updated_at=? WHERE id=?",
                            (iso(now), r["id"]))
                reclaimed += 1
        except Exception:
            # if invalid timestamp, clear lock
            cur.execute("UPDATE searches SET locked_by=NULL, locked_at=NULL, lock_expires_at=NULL, updated_at=? WHERE id=?",
                        (iso(now), r["id"]))
            reclaimed += 1
    con.commit()
    return reclaimed

def live_status_for(row: sqlite3.Row) -> str:
    """Compute the Admin 'Live Status' string."""
    status = row["status"] or ""
    if status == "pending_payment":
        return "Awaiting Payment"
    # Stale: if updated_at older than STALE_SEARCH_MIN
    try:
        updated = parse_iso(row["updated_at"])
    except Exception:
        updated = utcnow() - timedelta(minutes=999)
    age_min = (utcnow() - updated).total_seconds() / 60.0
    locked_by = row["locked_by"]
    lock_exp = row["lock_expires_at"]
    last_event = row["last_event"] or ""
    if locked_by and lock_exp:
        try:
            exp = parse_iso(lock_exp)
            if exp > utcnow():
                return f"Scanning by {locked_by}"
        except Exception:
            pass
    # event-derived states
    le = last_event.lower()
    if "captcha" in le:
        return "Captcha Cooldown"
    if "ip_blocked" in le or "blocked" in le:
        return "IP Blocked"
    if "slot_found" in le:
        return "Slot Found (awaiting action)"
    if age_min >= STALE_SEARCH_MIN:
        return f"Stale (>{STALE_SEARCH_MIN}m)"
    return "Idle / Queued"

# ========================== ROUTES: SEARCH FLOW ==========================
@app.post("/search", response_model=Dict[str, Any])
def create_search(body: CreateSearchBody):
    sid = new_id("srch")
    now = iso(utcnow())
    con = db()
    cur = con.cursor()
    centre_prefs_json = json.dumps(body.centre_prefs or [])
    status = "pending_payment" if (body.amount_cents or 0) > 0 else "active"
    cur.execute("""
        INSERT INTO searches (
          id, created_at, updated_at, booking_type, status, amount_cents,
          first_name, last_name, phone, email, licence_number,
          booking_reference, theory_pass, centre_prefs, date_window_from, date_window_to
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        sid, now, now, body.booking_type, status, body.amount_cents or 0,
        body.first_name, body.last_name, body.phone, body.email, body.licence_number,
        body.booking_reference, body.theory_pass, centre_prefs_json, body.date_window_from, body.date_window_to
    ))
    con.commit()
    con.close()
    return {"id": sid, "status": status, "created_at": now}

@app.get("/search/{sid}", response_model=Dict[str, Any])
def get_search(sid: str):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM searches WHERE id=?", (sid,))
    row = cur.fetchone()
    con.close()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")
    d = dict(row)
    d["centre_prefs"] = json.loads(d["centre_prefs"] or "[]")
    d["live_status"] = live_status_for(row)
    return d

# ========================== ROUTES: STRIPE (SAFE BY DEFAULT) ==========================
class CreateIntentBody(BaseModel):
    search_id: str
    amount_cents: int = Field(ge=0)
    currency: str = "gbp"
    description: Optional[str] = None

@app.post("/pay/create-intent", response_model=Dict[str, Any])
def create_intent(body: CreateIntentBody):
    con = db()
    cur = con.cursor()
    cur.execute("SELECT * FROM searches WHERE id=?", (body.search_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        raise HTTPException(status_code=404, detail="Search not found")
    if STRIPE_SECRET_KEY:
        try:
            pi = stripe.PaymentIntent.create(
                amount=body.amount_cents,
                currency=body.currency,
                description=body.description or f"FastDTF #{body.search_id}",
                metadata={"search_id": body.search_id},
                automatic_payment_methods={"enabled": True},
            )
            client_secret = pi.client_secret
            pid = pi.id
        except Exception as e:
            con.close()
            raise HTTPException(status_code=500, detail=f"Stripe error: {e}")
    else:
        # Simulated local dev intent
        pid = f"pi_sim_{body.search_id}"
        client_secret = f"{pid}_secret_{os.urandom(3).hex()}"
    now = iso(utcnow())
    cur.execute("UPDATE searches SET amount_cents=?, payment_intent_id=?, updated_at=? WHERE id=?",
                (body.amount_cents, pid, now, body.search_id))
    con.commit()
    con.close()
    return {"payment_intent_id": pid, "client_secret": client_secret}

@app.post("/pay/webhook")
async def stripe_webhook(request: Request):
    if not STRIPE_WEBHOOK_SECRET:
        # Accept everything in dev; mark as paid on simulated events
        payload = await request.body()
        try:
            data = json.loads(payload.decode("utf-8"))
        except Exception:
            data = {}
        etype = (data.get("type") or "").lower()
        obj = data.get("data", {}).get("object", {})
        pid = obj.get("id")
        sid = (obj.get("metadata") or {}).get("search_id")
        con = db()
        cur = con.cursor()
        cur.execute("INSERT OR REPLACE INTO stripe_events (id, type, created_at, payload) VALUES (?, ?, ?, ?)",
                    (new_id("evt"), etype, iso(utcnow()), json.dumps(data)))
        if etype.endswith("payment_intent.succeeded") or etype == "payment_intent.succeeded":
            if sid:
                cur.execute("UPDATE searches SET status='active', paid_at=?, updated_at=? WHERE id=?",
                            (iso(utcnow()), iso(utcnow()), sid))
        con.commit()
        con.close()
        return Response(status_code=200)

    # Real verification
    payload = await request.body()
    sig = request.headers.get("Stripe-Signature", "")
    try:
        event = stripe.Webhook.construct_event(
            payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Webhook error: {e}")
    etype = event["type"]
    obj = event["data"]["object"]
    pid = obj.get("id")
    sid = (obj.get("metadata") or {}).get("search_id")
    con = db()
    cur = con.cursor()
    cur.execute("INSERT OR REPLACE INTO stripe_events (id, type, created_at, payload) VALUES (?, ?, ?, ?)",
                (new_id("evt"), etype, iso(utcnow()), json.dumps(event)))
    if etype == "payment_intent.succeeded" and sid:
        cur.execute("UPDATE searches SET status='active', paid_at=?, updated_at=? WHERE id=?",
                    (iso(utcnow()), iso(utcnow()), sid))
    con.commit()
    con.close()
    return Response(status_code=200)

# ========================== ROUTES: WORKER ==========================
@app.post("/worker/pulse", response_model=Dict[str, Any])
def worker_pulse(body: WorkerPulseBody, request: Request):
    require_worker(request)
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO worker_status (worker_id, last_seen, state, note, rps, jobs_active)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_id) DO UPDATE SET
          last_seen=excluded.last_seen,
          state=excluded.state,
          note=excluded.note,
          rps=excluded.rps,
          jobs_active=excluded.jobs_active
    """, (
        body.worker_id, iso(utcnow()), body.state, body.note, body.rps, body.jobs_active
    ))
    con.commit()
    con.close()
    return {"ok": True}

@app.get("/worker/next", response_model=NextJobResponse)
def worker_next(request: Request, max_lock_min: int = Query(default=STALE_SEARCH_MIN, ge=1, le=30)):
    wid = require_worker(request)
    con = db()
    cur = con.cursor()
    reclaim_stale_locks(con)
    # a ready job: active status, unlocked, or stale lock; prefer oldest updated
    cur.execute("""
        SELECT * FROM searches
        WHERE status='active'
          AND (locked_by IS NULL OR lock_expires_at IS NULL OR lock_expires_at <= ?)
        ORDER BY updated_at ASC
        LIMIT 1
    """, (iso(utcnow()),))
    row = cur.fetchone()
    if not row:
        con.close()
        return NextJobResponse(search_id=None)
    sid = row["id"]
    now = utcnow()
    expires = now + timedelta(minutes=max_lock_min)
    cur.execute("""
        UPDATE searches
           SET locked_by=?, locked_at=?, lock_expires_at=?, updated_at=?
         WHERE id=?
    """, (wid, iso(now), iso(expires), iso(now), sid))
    con.commit()
    # Return payload for worker
    d = dict(row)
    d["centre_prefs"] = json.loads(d["centre_prefs"] or "[]")
    con.close()
    return NextJobResponse(
        search_id=sid,
        payload={
            "booking_type": d["booking_type"],
            "first_name": d["first_name"],
            "last_name": d["last_name"],
            "phone": d["phone"],
            "email": d["email"],
            "licence_number": d["licence_number"],
            "booking_reference": d["booking_reference"],
            "theory_pass": d["theory_pass"],
            "centre_prefs": d["centre_prefs"],
            "date_window_from": d["date_window_from"],
            "date_window_to": d["date_window_to"],
        },
        lock_expires_at=iso(expires),
    )

@app.post("/worker/event", response_model=Dict[str, Any])
def worker_event(body: WorkerEventBody, request: Request):
    wid = require_worker(request)
    con = db()
    cur = con.cursor()
    now = iso(utcnow())
    # audit
    cur.execute("""
        INSERT INTO admin_events (created_at, search_id, worker_id, kind, details)
        VALUES (?, ?, ?, ?, ?)
    """, (now, body.search_id, wid, body.kind, body.details))
    # reflect to search row
    cur.execute("""
        UPDATE searches
           SET last_event=?, last_event_at=?, updated_at=?
         WHERE id=?
    """, (f"{body.kind}", now, now, body.search_id))
    # if booked -> mark completed + clear lock
    if body.kind.startswith("booked"):
        cur.execute("""
            UPDATE searches
               SET status='completed', locked_by=NULL, locked_at=NULL, lock_expires_at=NULL, updated_at=?
             WHERE id=?
        """, (now, body.search_id))
    # if severe block -> pause
    if "ip_blocked" in body.kind or "service_closed" in body.kind:
        cur.execute("""
            UPDATE searches
               SET status='paused', updated_at=?
             WHERE id=?
        """, (now, body.search_id))
    con.commit()
    con.close()
    return {"ok": True}

@app.post("/worker/release/{sid}", response_model=Dict[str, Any])
def worker_release(sid: str, request: Request):
    require_worker(request)
    con = db()
    cur = con.cursor()
    cur.execute("""
        UPDATE searches
           SET locked_by=NULL, locked_at=NULL, lock_expires_at=NULL, updated_at=?
         WHERE id=?
    """, (iso(utcnow()), sid))
    con.commit()
    con.close()
    return {"ok": True}

# ========================== ROUTES: ADMIN ==========================
def fmt_age(ts: Optional[str]) -> str:
    dt = parse_iso_safe(ts)
    if not dt:
        return "—"
    dt = aware(dt)
    mins = int((utcnow() - dt).total_seconds() // 60)
    if mins < 1:
        return "just now"
    if mins == 1:
        return "1 min"
    return f"{mins} mins"

@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    require_admin(request)
    con = db()
    cur = con.cursor()
    # workers
    cur.execute("SELECT * FROM worker_status ORDER BY worker_id ASC")
    workers = cur.fetchall()
    # searches
    cur.execute("SELECT * FROM searches ORDER BY created_at DESC LIMIT 500")
    rows = cur.fetchall()
    con.close()

    # Build header with worker pulses
    worker_cards = []
    for w in workers:
        last_seen = w["last_seen"]
        state = html.escape(w["state"] or "—")
        note = html.escape(w["note"] or "")
        rps = w["rps"]
        jobs = w["jobs_active"]
        age = fmt_age(last_seen)
        worker_cards.append(f"""
        <div class="wcard">
          <div class="wtitle">Worker: <b>{html.escape(w['worker_id'])}</b></div>
          <div>State: {state}</div>
          <div>RPS: {rps if rps is not None else '—'} | Jobs: {jobs if jobs is not None else '—'}</div>
          <div>Last pulse: {age}</div>
          <div class="wnote">{note}</div>
        </div>
        """)

    # Table rows
    trs = []
    for r in rows:
        live = live_status_for(r)
        _created = parse_iso_safe(r["created_at"])
        _updated = parse_iso_safe(r["updated_at"])
        created_lon = aware(_created).astimezone(LON)
        updated_lon = aware(_updated).astimezone(LON)
        paid_badge = "Yes" if r["paid_at"] else ("No" if r["amount_cents"] else "N/A")
        lock = "—"
        if r["locked_by"]:
            lock = f"{html.escape(r['locked_by'])} (until {html.escape(r['lock_expires_at'] or '')})"
        last_event = html.escape(r["last_event"] or "—")
        name = " ".join([r["first_name"] or "", r["last_name"] or ""]).strip() or "—"
        trs.append(f"""
        <tr>
          <td><code>{html.escape(r['id'])}</code></td>
          <td>{created_lon.strftime("%Y-%m-%d %H:%M")}</td>
          <td>{html.escape(r['booking_type'])}</td>
          <td>{html.escape(name)}<br><small>{html.escape(r['phone'] or '—')}</small></td>
          <td>{html.escape(r['status'])}</td>
          <td><b>{html.escape(live)}</b></td>
          <td>{last_event}</td>
          <td>{updated_lon.strftime("%Y-%m-%d %H:%M")}<br><small>{fmt_age(r['updated_at'])}</small></td>
          <td>{lock}</td>
          <td>{paid_badge}</td>
        </tr>
        """)

    html_doc = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>FastDTF Admin</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 16px; }}
    h1 {{ margin: 0 0 6px 0; }}
    .muted {{ color:#666; font-size: 12px; }}
    .workers {{ display:flex; gap:12px; flex-wrap:wrap; margin: 12px 0 20px; }}
    .wcard {{ border:1px solid #ddd; border-radius:10px; padding:10px 12px; min-width:220px; background:#fafafa; }}
    .wtitle {{ font-size:14px; margin-bottom:6px; }}
    .wnote {{ color:#555; font-size:12px; margin-top:6px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ border-bottom:1px solid #eee; padding:8px; text-align:left; vertical-align:top; }}
    th {{ background:#f7f7f7; position: sticky; top:0; z-index: 1; }}
    code {{ background:#f3f3f3; padding:1px 4px; border-radius:4px; }}
    .tips {{ margin-bottom: 10px; color:#444; }}
  </style>
</head>
<body>
  <h1>FastDTF Admin</h1>
  <div class="muted">
    Server Time (London): <b>{london_now().strftime("%Y-%m-%d %H:%M:%S")}</b> ·
    Stale Reclaim Window: <b>{STALE_SEARCH_MIN}m</b>
  </div>

  <div class="tips">Workers heartbeat via <code>POST /worker/pulse</code>. Jobs are claimed via <code>GET /worker/next</code> and auto-reclaimed when locks expire.</div>

  <div class="workers">
    {''.join(worker_cards) or '<i>No worker pulses yet</i>'}
  </div>

  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Created (London)</th>
        <th>Type</th>
        <th>Applicant</th>
        <th>Status</th>
        <th>Live Status</th>
        <th>Last Event</th>
        <th>Updated</th>
        <th>Worker Lock</th>
        <th>Paid</th>
      </tr>
    </thead>
    <tbody>
      {''.join(trs) or '<tr><td colspan="10"><i>No searches yet</i></td></tr>'}
    </tbody>
  </table>
</body>
</html>
    """
    return HTMLResponse(html_doc)

# ========================== HEALTH ==========================
@app.get("/health", response_model=Dict[str, Any])
def health():
    con = db()
    cur = con.cursor()
    # small check: rows count and recent worker pulse
    cur.execute("SELECT COUNT(*) AS c FROM searches")
    c = cur.fetchone()["c"]
    cur.execute("SELECT MAX(last_seen) AS last FROM worker_status")
    last = cur.fetchone()["last"]
    con.close()
    return {
        "ok": True,
        "now_utc": iso(utcnow()),
        "searches": c,
        "last_worker_pulse": last,
        "stale_search_min": STALE_SEARCH_MIN,
    }

# ========================== MISC (PAUSE/RESUME) ==========================
@app.post("/admin/search/{sid}/pause")
def admin_pause_search(sid: str, request: Request):
    require_admin(request)
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE searches SET status='paused', updated_at=? WHERE id=?", (iso(utcnow()), sid))
    con.commit()
    con.close()
    return {"ok": True}

@app.post("/admin/search/{sid}/resume")
def admin_resume_search(sid: str, request: Request):
    require_admin(request)
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE searches SET status='active', updated_at=? WHERE id=?", (iso(utcnow()), sid))
    con.commit()
    con.close()
    return {"ok": True}
