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

stripe.api_key = STRIPE_SECRET_KEY
stripe.max_network_retries = 2

# ---------- APP ----------
app = FastAPI(title="FastDrivingTestFinder API", version="1.4.4")

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
    return {"ok": True, "ts": now_iso(), "version": "1.4.4"}

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
        last_event="created"
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
        last_event="created"
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
    if inferred and inferred != existing_type:
        try:
            cur.execute(
                "UPDATE searches SET booking_type=?, updated_at=? WHERE id=?",
                (inferred, now_iso(), sid),
            )
            conn.commit()
        except Exception:
            pass

    md = {"search_id": str(sid), "email": (row["email"] or body.email or "")}
    if body.metadata:
        try:
            for k, v in body.metadata.items():
                if k != "search_id":
                    md[k] = v
        except Exception:
            pass

    try:
        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=body.currency,
            description=body.description or f"Search #{sid}",
            metadata=md,
            automatic_payment_methods={"enabled": True},
            idempotency_key=f"pi_search_{sid}",
        )
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))

    cur.execute(
        "UPDATE searches SET payment_intent_id = ?, amount = ?, last_event = ?, updated_at = ? WHERE id = ?",
        (intent["id"], amount, "payment_intent.created", now_iso(), sid)
    )
    conn.commit()
    conn.close()
    return {
        "search_id": sid,
        "client_secret": intent["client_secret"],
        "clientSecret": intent["client_secret"],
        "paymentIntentId": intent["id"]
    }

# ---------- STRIPE WEBHOOK ----------
@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not STRIPE_WEBHOOK_SECRET:
        try:
            event = json.loads(payload.decode())
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid payload")
    else:
        try:
            event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Webhook error: {e}")

    event_id = event.get("id")
    etype = event.get("type")
    data = event.get("data", {}).get("object", {}) or {}

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO stripe_events (id, type, created_at) VALUES (?,?,?)",
            (event_id, etype, now_iso())
        )
        conn.commit()
    except Exception:
        pass

    if etype == "payment_intent.succeeded":
        pi_id = data.get("id")
        sid = (data.get("metadata") or {}).get("search_id")
        amount_received = int(data.get("amount_received") or data.get("amount") or 0)
        if sid:
            _mark_paid_and_queue(int(sid), pi_id, "payment_intent.succeeded", amount_received)
    elif etype in ("payment_intent.canceled", "payment_intent.payment_failed"):
        pi_id = data.get("id")
        sid = (data.get("metadata") or {}).get("search_id")
        if sid:
            _fail(int(sid), f"{etype} ({pi_id})")

    conn.close()
    return JSONResponse({"received": True})

def _mark_paid_and_queue(search_id: int, payment_intent_id: str, event_label: str, amount_received: int = 0):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches
           SET paid = 1,
               status = 'queued',
               last_event = ?,
               payment_intent_id = ?,
               amount = COALESCE(NULLIF(?,0), amount),
               updated_at = ?,
               expires_at = NULL
         WHERE id = ?
    """, (event_label, payment_intent_id, amount_received, now_iso(), search_id))
    conn.commit()
    conn.close()

def _fail(search_id: int, reason: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE searches SET status='failed', last_event=?, updated_at=? WHERE id=?",
                (reason, now_iso(), search_id))
    conn.commit()
    conn.close()

# ---------- ADMIN ----------
def _badge(s: str) -> str:
    colors = {
        "pending_payment":"#64748b","new":"#64748b","queued":"#2563eb",
        "searching":"#a855f7","found":"#10b981","booked":"#16a34a",
        "failed":"#ef4444","expired":"#334155","disabled":"#6b7280"
    }
    c = colors.get(s, "#334155")
    return f'<span style="background:{c};color:white;padding:2px 8px;border-radius:999px;font-size:12px">{s}</span>'

def _derive_flags(row: sqlite3.Row):
    """
    Compute Admin flags from last_event only (no schema changes):
    - reached: True if we saw a DVSA interaction event.
    - last_centre: parsed from known event prefixes that include a centre.
    - captcha: True when last_event starts with 'captcha_cooldown:'.
    """
    ev = (row["last_event"] or "").strip()
    reached = False
    last_centre = ""
    captcha = False

    prefixes_with_centre = (
        "checked:",
        "slot_found:",
        "booked:",
        "booking_failed:",
        "captcha_cooldown:",
        "no_slots_this_round:",
        "slot_unchanged:",
        "trying:",
    )

    for pfx in prefixes_with_centre:
        if ev.startswith(pfx):
            try:
                payload = ev.split(":", 1)[1]
                last_centre = (payload.split("·", 1)[0] or "").strip()
            except Exception:
                last_centre = ""
            break

    if ev.startswith(("checked:", "slot_found:", "booked:", "booking_failed:")):
        reached = True

    if ev.startswith("captcha_cooldown:"):
        captcha = True

    return reached, last_centre, captcha

def _fmt_booking_ref(row: sqlite3.Row) -> str:
    ref = (row["booking_reference"] or "").strip()
    if (row["booking_type"] or "") == "swap":
        ok = len(ref) == 8 and ref.isdigit()
        if ok:
            return f"{ref} ✅"
        return f"{ref or '—'} <span title='Missing or not 8 digits'>⚠</span>"
    return ref or ""

@app.get("/api/admin", response_class=HTMLResponse)
async def admin(request: Request, status: Optional[str] = Query(None)):
    require_admin(request)
    conn = get_conn()
    _expire_unpaid(conn)

    cur = conn.cursor()
    cur.execute("SELECT status, COUNT(*) as c FROM searches GROUP BY status")
    counts = {r["status"]: r["c"] for r in cur.fetchall()}

    if status:
        cur.execute("SELECT * FROM searches WHERE status=? ORDER BY created_at DESC", (status,))
    else:
        cur.execute("SELECT * FROM searches ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()

    def td(v): return f"<td style='border-bottom:1px solid #eee;padding:8px;vertical-align:top'>{v}</td>"

    statuses = ["pending_payment","new","queued","searching","found","booked","failed","expired","disabled"]
    pills = " ".join(f'<a href="?status={s}" style="text-decoration:none">{_badge(s)}</a>' for s in statuses)

    rows_html = []
    for r in rows:
        centres = ", ".join((json.loads(r["centres_json"] or "[]")))
        opts_raw = r["options_json"] or ""
        opts = html.escape(opts_raw)

        reached, last_centre, captcha = _derive_flags(r)
        reached_html = "✅" if reached else "—"
        captcha_html = "🚧" if captcha else "—"

        rows_html.append(
            "<tr>"
            + td(r["id"])
            + td(_badge(r["status"]))
            + td(r["booking_type"] or "")
            + td(r["licence_number"] or "")
            + td(_fmt_booking_ref(r))
            + td(r["theory_pass"] or "")
            + td(f'{r["date_window_from"] or ""} → {r["date_window_to"] or ""}<br>{r["time_window_from"] or ""} → {r["time_window_to"] or ""}')
            + td(r["phone"] or "")
            + td(r["email"] or "")
            + td(centres)
            + td(f"<code style='font-size:12px'>{opts}</code>")
            + td(r["last_event"] or "")
            + td(r["paid"])
            + td(r["payment_intent_id"] or "")
            + td(reached_html)
            + td(last_centre or "—")
            + td(captcha_html)
            + td(r["created_at"] or "")
            + td(r["updated_at"] or "")
            + "</tr>"
        )

    html_doc = f"""
    <html>
    <head>
      <title>Admin · FastDrivingTestFinder</title>
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <style>
        body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;padding:16px;color:#0f172a}}
        .wrap{{max-width:1200px;margin:0 auto}}
        h1{{margin:0 0 8px 0}}
        table{{border-collapse:collapse;width:100%;font-size:14px}}
        th,td{{border-bottom:1px solid #eee;padding:8px;text-align:left;vertical-align:top}}
        th{{background:#f8fafc;position:sticky;top:0}}
        .counts span{{margin-right:10px}}
        .bar{{display:flex;align-items:center;gap:8px;margin-bottom:12px}}
        .pill a{{margin-right:6px}}
        code{{background:#f1f5f9;padding:2px 4px;border-radius:6px}}
      </style>
    </head>
    <body>
      <div class="wrap">
        <h1>Searches</h1>
        <div class="bar">
          <div class="pill">{pills}<a href="/api/admin" style="margin-left:8px">Clear</a></div>
        </div>
        <div class="counts">
          {"".join(f"<span>{_badge(k)} {v}</span>" for k,v in counts.items())}
        </div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Status</th><th>Type</th><th>Licence</th><th>Booking Ref</th><th>Theory</th>
              <th>Date/Time Window</th><th>Phone</th><th>Email</th><th>Centres</th><th>Options</th>
              <th>Last Event</th><th>Paid</th><th>PI</th>
              <th>Reached?</th><th>Last centre</th><th>Captcha?</th>
              <th>Created</th><th>Updated</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(html_doc)

# ---------- WORKER ENDPOINTS ----------
@app.post("/api/worker/claim")
def worker_claim(body: WorkerClaimRequest, _: bool = Depends(_verify_worker)):
    conn = get_conn()
    _expire_unpaid(conn)

    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM searches
         WHERE paid=1 AND status IN ('queued','new')
         ORDER BY updated_at ASC
         LIMIT ?
    """, (body.limit,))
    rows = cur.fetchall()

    claimed = []
    for r in rows:
        cur.execute("""
            UPDATE searches
               SET status='searching', last_event='worker_claimed', updated_at=?
             WHERE id=? AND paid=1 AND status IN ('queued','new')
        """, (now_iso(), r["id"]))
        if cur.rowcount:
            cur.execute("SELECT * FROM searches WHERE id=?", (r["id"],))
            claimed.append(cur.fetchone())
    conn.commit()
    conn.close()

    items = [dict(row) for row in claimed]
    return {"items": items}

@app.post("/api/worker/searches/{sid}/event")
def worker_event(sid: int, body: WorkerEvent, _: bool = Depends(_verify_worker)):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE searches SET last_event=?, updated_at=? WHERE id=?",
                (body.event, now_iso(), sid))
    conn.commit()
    conn.close()
    return {"ok": True}

@app.post("/api/worker/searches/{sid}/status")
def worker_status(sid: int, b: WorkerStatus, _: bool = Depends(_verify_worker)):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE searches SET status=?, last_event=?, updated_at=? WHERE id=?",
                (b.status, b.event, now_iso(), sid))
    conn.commit()
    conn.close()
    return {"ok": True}
