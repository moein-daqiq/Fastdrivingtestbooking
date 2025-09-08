from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from dotenv import load_dotenv
import stripe

# ------------------------------------------------------------------------------
# Env & Stripe config
# ------------------------------------------------------------------------------
load_dotenv()  # loads .env in the current working directory

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ------------------------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------------------------
app = FastAPI(title="DVSA Bot API", version="1.2.0")
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.fastdrivingtestfinder.co.uk"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_FILE = "searches.db"

# ------------------------------------------------------------------------------
# DB helpers (schema + connections)
# ------------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db_schema():
    """
    Create the `searches` table if missing; add new columns if the DB existed
    from an earlier version.
    """
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS searches (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          created_at  TEXT NOT NULL,
          surname     TEXT,
          email       TEXT,
          centres     TEXT,
          status      TEXT NOT NULL DEFAULT 'new',
          last_event  TEXT,
          paid        INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    conn.commit()

    # Ensure all columns exist (safe no-ops if they already do)
    cur.execute("PRAGMA table_info(searches)")
    cols = {row["name"] for row in cur.fetchall()}

    def ensure(col_sql: str):
        try:
            cur.execute(col_sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # already exists

    if "status" not in cols:
        ensure("ALTER TABLE searches ADD COLUMN status TEXT NOT NULL DEFAULT 'new'")
    if "last_event" not in cols:
        ensure("ALTER TABLE searches ADD COLUMN last_event TEXT")
    if "paid" not in cols:
        ensure("ALTER TABLE searches ADD COLUMN paid INTEGER NOT NULL DEFAULT 0")

    conn.close()


_init_db_schema()

# ------------------------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------------------------

class CreateSearch(BaseModel):
    surname: str = Field(..., min_length=1, max_length=80)
    email: EmailStr
    centres: str = Field(..., min_length=1, max_length=400)   # comma-separated list
    # (Optional) You can extend with more fields later, e.g. time window, phone, etc.


class CheckoutRequest(BaseModel):
    search_id: int
    amount_cents: int = Field(..., ge=100, le=100000)   # £1.00 .. £1000.00
    currency: str = Field(default="gbp", pattern=r"^[a-z]{3}$")
    success_url: Optional[str] = None
    cancel_url: Optional[str] = None


# ------------------------------------------------------------------------------
# Core routes
# ------------------------------------------------------------------------------

@app.get("/api/health")
def health_check():
    return {"ok": True, "ts": int(time.time())}


@app.post("/api/search")
def create_search(data: CreateSearch):
    """
    Creates a row in `searches`, returns its ID.
    Frontend will submit here _after_ the user fills the form (and before payment).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO searches (created_at, surname, email, centres, status, last_event, paid)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (now_iso(), data.surname.strip(), data.email.strip(), data.centres.strip(), "new", "submitted " + now_iso(), 0)
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"id": new_id, "ok": True}


@app.get("/api/admin", response_class=Response)
def admin_view():
    """
    Very simple HTML table of latest searches (most recent first).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, created_at, surname, email, centres, status, paid, COALESCE(last_event,'') AS last_event
        FROM searches
        ORDER BY id DESC
        LIMIT 200
        """
    )
    rows = cur.fetchall()
    conn.close()

    def h(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    out = []
    out.append("<!doctype html><meta charset='utf-8'><title>Admin — Searches</title>")
    out.append("<h2 style='font-family:system-ui,Segoe UI,Arial;'>Recent Searches</h2>")
    out.append("<table border='1' cellspacing='0' cellpadding='6' style='font-family:monospace;border-collapse:collapse'>")
    out.append("<tr><th>ID</th><th>Surname</th><th>Email</th><th>Paid</th><th>Status</th><th>Last Event</th></tr>")
    for r in rows:
        out.append(
            "<tr>"
            f"<td>{r['id']}</td>"
            f"<td>{h(r['surname'])}</td>"
            f"<td>{h(r['email'])}</td>"
            f"<td>{'Yes' if r['paid'] else 'No'}</td>"
            f"<td>{h(r['status'])}</td>"
            f"<td>{h(r['last_event'])}</td>"
            "</tr>"
        )
    out.append("</table>")
    return Response("".join(out), media_type="text/html")


# ------------------------------------------------------------------------------
# Stripe: create checkout + webhook
# ------------------------------------------------------------------------------

@app.post("/api/pay/create-checkout")
def create_checkout(req: CheckoutRequest):
    """
    Creates a Stripe Checkout Session and returns the hosted URL.

    - `search_id` is stored in client_reference_id so we can update the DB on webhook.
    - Amount is in pence (cents). We clamp/validate on server for safety.
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe is not configured (missing STRIPE_SECRET_KEY).")

    # Basic sanity: ensure search exists
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, paid FROM searches WHERE id = ?", (req.search_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Search not found.")
    if row["paid"]:
        conn.close()
        raise HTTPException(status_code=400, detail="Already paid.")

    # Defaults for success/cancel URLs (local dev)
    success_url = req.success_url or "http://localhost:8787/api/pay/success"
    cancel_url = req.cancel_url or "http://localhost:8787/api/pay/cancel"

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": req.currency.lower(),
                    "product_data": {"name": "DVSA search service"},
                    "unit_amount": int(req.amount_cents),
                },
                "quantity": 1,
            }],
            client_reference_id=str(req.search_id),
            success_url=success_url + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=cancel_url,
        )
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=502, detail=f"Stripe error: {e}")

    # Record event (optional)
    cur.execute(
        "UPDATE searches SET last_event=? WHERE id=?",
        (f"checkout_created {now_iso()}", req.search_id)
    )
    conn.commit()
    conn.close()

    return {"ok": True, "checkout_url": session.url, "session_id": session.id}


@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    """
    Verifies Stripe signature and updates DB on successful Checkout.
    """
    if not STRIPE_WEBHOOK_SECRET:
        # You can run without verifying (not recommended). If you want that:
        # raw = await request.body()
        # event = stripe.Event.construct_from(json.loads(raw), stripe.api_key)
        raise HTTPException(status_code=500, detail="Missing STRIPE_WEBHOOK_SECRET.")

    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Webhook signature error: {e}")

    # Handle only events we care about
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        search_id = session.get("client_reference_id")
        if search_id:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("UPDATE searches SET paid=1, status=?, last_event=? WHERE id=?",
                        ("paid", f"paid {now_iso()}", int(search_id)))
            conn.commit()
            conn.close()

    return {"ok": True}


# ------------------------------------------------------------------------------
# (Optional) simple placeholders for success/cancel (handy in dev)
# ------------------------------------------------------------------------------

@app.get("/api/pay/success")
def pay_success():
    return Response("<h3>Payment successful.</h3>", media_type="text/html")


@app.get("/api/pay/cancel")
def pay_cancel():
    return Response("<h3>Payment canceled.</h3>", media_type="text/html")


# ------------------------------------------------------------------------------
# Uvicorn entry-point (so you can `uvicorn api_gateway:app --reload --port 8787`)
# ------------------------------------------------------------------------------

# No `if __name__ == "__main__":` block needed, but you can add it if you prefer.

