import os
import json
import hmac
import hashlib
import sqlite3
import stripe
from datetime import datetime
from typing import List, Optional, Dict, Any, Literal

from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

# ---------- ENV ----------
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
DB_FILE = os.environ.get("SEARCHES_DB", "searches.db")
ADMIN_USER = os.environ.get("ADMIN_BASIC_AUTH_USER")
ADMIN_PASS = os.environ.get("ADMIN_BASIC_AUTH_PASS")

stripe.api_key = STRIPE_SECRET_KEY

# ---------- APP ----------
app = FastAPI(title="FastDrivingTestFinder API", version="1.1.0")

# CORS (adjust if needed)
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "https://www.fastdrivingtestfinder.co.uk")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN, "http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- DB ----------
def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def migrate():
    """
    Lightweight, idempotent SQLite migration.
    Creates table if missing; adds new columns if they don't exist.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT,
            updated_at TEXT,
            status TEXT DEFAULT 'new',               -- new → queued → searching → found/booked/failed
            last_event TEXT,
            paid INTEGER DEFAULT 0,

            -- Stripe linkage
            payment_intent_id TEXT,
            amount INTEGER,                          -- in minor units (pence)

            -- Core fields from your form
            booking_type TEXT,                       -- 'new' | 'swap'
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

            centres_json TEXT,                       -- JSON array of strings
            options_json TEXT,                       -- JSON object of toggles
            notes TEXT
        )
    """)

    # Ensure columns exist (safe to run repeatedly)
    expected_cols = {
        "created_at","updated_at","status","last_event","paid",
        "payment_intent_id","amount",
        "booking_type","licence_number","booking_reference","theory_pass",
        "date_window_from","date_window_to","time_window_from","time_window_to",
        "phone","whatsapp","email","centres_json","options_json","notes"
    }
    cur.execute("PRAGMA table_info(searches)")
    have = {row["name"] for row in cur.fetchall()}
    missing = expected_cols - have
    for col in missing:
        # Minimal types; SQLite is dynamic
        coltype = "TEXT"
        if col in {"paid","amount"}:
            coltype = "INTEGER"
        cur.execute(f"ALTER TABLE searches ADD COLUMN {col} {coltype}")

    # Simple status index helps the worker
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_status ON searches(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_searches_paid ON searches(paid)")
    conn.commit()
    conn.close()

migrate()

# ---------- MODELS ----------
class SearchIn(BaseModel):
    booking_type: Literal["new", "swap"] = "new"
    licence_number: Optional[str] = None
    booking_reference: Optional[str] = None  # for swap flows
    theory_pass: Optional[str] = None        # for new flows (optional per your spec)

    date_window_from: Optional[str] = Field(None, description="YYYY-MM-DD")
    date_window_to: Optional[str]   = Field(None, description="YYYY-MM-DD")
    time_window_from: Optional[str] = Field(None, description="HH:MM (24h)")
    time_window_to: Optional[str]   = Field(None, description="HH:MM (24h)")

    phone: Optional[str] = None
    whatsapp: Optional[str] = None
    email: Optional[str] = None

    centres: List[str] = []
    options: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None

class PayCreateIntentIn(BaseModel):
    search_id: int
    amount: int  # pence
    currency: str = "gbp"
    # Optionally allow a description
    description: Optional[str] = None

# ---------- HELPERS ----------
def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def json_dumps(v) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "null"

def require_admin(request: Request):
    if not ADMIN_USER or not ADMIN_PASS:
        return  # no auth set
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        raise HTTPException(status_code=401, detail="Auth required", headers={"WWW-Authenticate":"Basic"})
    import base64
    try:
        decoded = base64.b64decode(auth.split(" ",1)[1]).decode("utf-8")
    except Exception:
        raise HTTPException(status_code=401, detail="Bad auth", headers={"WWW-Authenticate":"Basic"})
    user, _, pwd = decoded.partition(":")
    if user != ADMIN_USER or pwd != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Invalid credentials", headers={"WWW-Authenticate":"Basic"})

# ---------- ROUTES (paths unchanged) ----------
@app.post("/api/search")
async def create_search(payload: SearchIn):
    """
    Stores ALL form fields. Returns search_id.
    Status starts as 'new'. Payment webhook will flip to 'queued'.
    """
    conn = get_conn()
    cur = conn.cursor()
    ts = now_iso()
    cur.execute("""
        INSERT INTO searches (
            created_at, updated_at, status, last_event, paid,
            booking_type, licence_number, booking_reference, theory_pass,
            date_window_from, date_window_to, time_window_from, time_window_to,
            phone, whatsapp, email, centres_json, options_json, notes
        ) VALUES (?,?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?,
                  ?,?,?,?, ?,?)
    """, (
        ts, ts, "new", "created", 0,
        payload.booking_type, payload.licence_number, payload.booking_reference, payload.theory_pass,
        payload.date_window_from, payload.date_window_to, payload.time_window_from, payload.time_window_to,
        payload.phone, payload.whatsapp, payload.email, json_dumps(payload.centres), json_dumps(payload.options), payload.notes
    ))
    search_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"ok": True, "search_id": search_id, "status": "new"}

@app.post("/api/pay/create-intent")
async def pay_create_intent(body: PayCreateIntentIn):
    """
    Creates a Stripe PaymentIntent and stores payment_intent_id on the search.
    Expects amount in pence and a valid search_id.
    """
    # Basic existence check
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM searches WHERE id = ?", (body.search_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="search_id not found")

    try:
        intent = stripe.PaymentIntent.create(
            amount=body.amount,
            currency=body.currency,
            description=body.description or f"Search #{body.search_id}",
            metadata={"search_id": str(body.search_id)},
            automatic_payment_methods={"enabled": True},
        )
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))

    cur.execute(
        "UPDATE searches SET payment_intent_id = ?, amount = ?, updated_at = ? WHERE id = ?",
        (intent["id"], body.amount, now_iso(), body.search_id)
    )
    conn.commit()
    conn.close()
    return {"clientSecret": intent["client_secret"], "paymentIntentId": intent["id"]}

@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    """
    Marks paid + queues the search when the PaymentIntent succeeds.
    """
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    if not STRIPE_WEBHOOK_SECRET:
        # If unset (dev), accept without verification
        event = json.loads(payload.decode())
    else:
        try:
            event = stripe.Webhook.construct_event(
                payload=payload, sig_header=sig_header, secret=STRIPE_WEBHOOK_SECRET
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Webhook error: {e}")

    etype = event.get("type")
    data = event.get("data", {}).get("object", {})

    if etype == "payment_intent.succeeded":
        pi_id = data.get("id")
        meta = data.get("metadata", {}) or {}
        sid = meta.get("search_id")
        if sid:
            _mark_paid_and_queue(int(sid), pi_id, "payment_intent.succeeded")
    elif etype == "payment_intent.payment_failed":
        pi_id = data.get("id")
        meta = data.get("metadata", {}) or {}
        sid = meta.get("search_id")
        if sid:
            _fail(int(sid), f"payment_failed ({pi_id})")
    # Acknowledge
    return JSONResponse({"received": True})

def _mark_paid_and_queue(search_id: int, payment_intent_id: str, event_label: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches
           SET paid = 1,
               status = CASE WHEN status='new' THEN 'queued' ELSE status END,
               last_event = ?,
               payment_intent_id = ?,
               updated_at = ?
         WHERE id = ?
    """, (event_label, payment_intent_id, now_iso(), search_id))
    conn.commit()
    conn.close()

def _fail(search_id: int, reason: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches SET status='failed', last_event=?, updated_at=? WHERE id=?
    """, (reason, now_iso(), search_id))
    conn.commit()
    conn.close()

# ---------- ADMIN ----------
def _badge(s: str) -> str:
    colors = {
        "new": "#64748b", "queued": "#2563eb", "searching": "#a855f7",
        "found": "#10b981", "booked": "#16a34a", "failed": "#ef4444"
    }
    c = colors.get(s, "#334155")
    return f'<span style="background:{c};color:white;padding:2px 8px;border-radius:999px;font-size:12px">{s}</span>'

@app.get("/api/admin", response_class=HTMLResponse)
async def admin(request: Request, status: Optional[str] = Query(None, description="Filter by status")):
    require_admin(request)  # no-op if env not set

    conn = get_conn()
    cur = conn.cursor()

    # Summary counts
    cur.execute("SELECT status, COUNT(*) as c FROM searches GROUP BY status")
    counts = {r["status"]: r["c"] for r in cur.fetchall()}

    if status:
        cur.execute("SELECT * FROM searches WHERE status = ? ORDER BY created_at DESC", (status,))
    else:
        cur.execute("SELECT * FROM searches ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()

    # Simple filters UI
    statuses = ["new","queued","searching","found","booked","failed"]
    pills = " ".join(
        f'<a href="?status={s}" style="text-decoration:none">{_badge(s)}</a>'
        for s in statuses
    )
    clear = '<a href="/api/admin" style="margin-left:8px">Clear</a>'

    # Table
    def td(v): return f"<td style='border-bottom:1px solid #eee;padding:8px;vertical-align:top'>{v}</td>"

    rows_html = []
    for r in rows:
        centres = ", ".join((json.loads(r["centres_json"] or "[]")))
        opts = r["options_json"]
        rows_html.append(
            "<tr>" +
            td(r["id"]) +
            td(_badge(r["status"])) +
            td(r["booking_type"] or "") +
            td(r["licence_number"] or "") +
            td(r["booking_reference"] or "") +
            td(r["theory_pass"] or "") +
            td(f'{r["date_window_from"] or ""} → {r["date_window_to"] or ""}<br>{r["time_window_from"] or ""} → {r["time_window_to"] or ""}') +
            td(r["phone"] or "") +
            td(r["email"] or "") +
            td(centres) +
            td(f'<code style="font-size:12px">{opts}</code>') +
            td(r["last_event"] or "") +
            td(r["paid"]) +
            td(r["payment_intent_id"] or "") +
            td(r["created_at"] or "") +
            td(r["updated_at"] or "") +
            "</tr>"
        )

    html = f"""
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
          <div class="pill">{pills}{clear}</div>
        </div>
        <div class="counts">
          {"".join(f"<span>{_badge(k)} {v}</span>" for k,v in counts.items())}
        </div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Status</th><th>Type</th><th>Licence</th><th>Booking Ref</th><th>Theory</th>
              <th>Date/Time Window</th><th>Phone</th><th>Email</th><th>Centres</th><th>Options</th>
              <th>Last Event</th><th>Paid</th><th>PI</th><th>Created</th><th>Updated</th>
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
    return HTMLResponse(html)
