from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime
from typing import Optional

# --- FastAPI / CORS ---
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware

# --- Pydantic models ---
from pydantic import BaseModel, EmailStr, Field

# --- Env loader (safe no-op if python-dotenv not installed) ---
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    def load_dotenv() -> None:  # fallback
        return

# --- Payments ---
import stripe
import requests


# ==============================================================================
# Environment & third-party configuration
# ==============================================================================

load_dotenv()  # loads a local .env if present (Render env vars also work)

# CORS: comma-separated list, e.g. "https://fastdrivingtestfinder.co.uk,https://www.fastdrivingtestfinder.co.uk"
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv("ALLOWED_ORIGINS", "").split(",")
    if o.strip()
]
if not ALLOWED_ORIGINS:
    # Sensible defaults if env not set (keeps prod domains)
    ALLOWED_ORIGINS = [
        "https://fastdrivingtestfinder.co.uk",
        "https://www.fastdrivingtestfinder.co.uk",
    ]

# Stripe
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# PayPal
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "")
PAYPAL_SECRET = os.getenv("PAYPAL_SECRET", "")


# ==============================================================================
# FastAPI app
# ==============================================================================

app = FastAPI(title="DVSA Bot API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==============================================================================
# SQLite helpers
# ==============================================================================

DB_FILE = "searches.db"


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db_schema() -> None:
    """Create table / add columns if missing."""
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

    cur.execute("PRAGMA table_info(searches)")
    cols = {row["name"] for row in cur.fetchall()}

    def ensure(sql: str) -> None:
        try:
            cur.execute(sql)
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


# ==============================================================================
# Pydantic models
# ==============================================================================

class CreateSearch(BaseModel):
    surname: str = Field(..., min_length=1, max_length=80)
    email: EmailStr
    centres: str = Field(..., min_length=1, max_length=400)  # comma-separated list


class CheckoutRequest(BaseModel):
    search_id: int
    amount_cents: int = Field(..., ge=100, le=100000)  # £1.00 .. £1000.00
    currency: str = Field(default="gbp", pattern=r"^[a-z]{3}$")
    success_url: Optional[str] = None
    cancel_url: Optional[str] = None


# ==============================================================================
# Core routes
# ==============================================================================

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
        (
            now_iso(),
            data.surname.strip(),
            data.email.strip(),
            data.centres.strip(),
            "new",
            "submitted " + now_iso(),
            0,
        ),
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    return {"id": new_id, "ok": True}


@app.get("/api/admin", response_class=Response)
def admin_view():
    """Very simple HTML table of latest searches (most recent first)."""
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
    out.append(
        "<table border='1' cellspacing='0' cellpadding='6' "
        "style='font-family:monospace;border-collapse:collapse'>"
    )
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


# ==============================================================================
# Stripe: Checkout + Webhook
# ==============================================================================

@app.post("/api/pay/create-checkout")
def create_checkout(req: CheckoutRequest):
    """
    Creates a Stripe Checkout Session and returns the hosted URL.

    - `search_id` stored in client_reference_id so we can mark paid in webhook.
    - Amount is in pence (cents). We clamp/validate on server for safety.
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe is not configured (missing STRIPE_SECRET_KEY).")

    # Ensure search exists and not already paid
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

    # Defaults for success/cancel URLs (helpful in local dev)
    success_url = req.success_url or "http://localhost:8787/api/pay/success"
    cancel_url = req.cancel_url or "http://localhost:8787/api/pay/cancel"

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[
                {
                    "price_data": {
                        "currency": req.currency.lower(),
                        "product_data": {"name": "DVSA search service"},
                        "unit_amount": int(req.amount_cents),
                    },
                    "quantity": 1,
                }
            ],
            client_reference_id=str(req.search_id),
            success_url=success_url + "?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=cancel_url,
        )
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=502, detail=f"Stripe error: {e}")

    cur.execute(
        "UPDATE searches SET last_event=? WHERE id=?",
        (f"checkout_created {now_iso()}", req.search_id),
    )
    conn.commit()
    conn.close()

    return {"ok": True, "checkout_url": session.url, "session_id": session.id}


@app.post("/api/stripe/webhook")
async def stripe_webhook(request: Request):
    """Verify Stripe signature and update DB on successful Checkout."""
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Missing STRIPE_WEBHOOK_SECRET.")

    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Webhook signature error: {e}")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        search_id = session.get("client_reference_id")
        if search_id:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE searches SET paid=1, status=?, last_event=? WHERE id=?",
                ("paid", f"paid {now_iso()}", int(search_id)),
            )
            conn.commit()
            conn.close()

    return {"ok": True}


# ==============================================================================
# Simple placeholders (handy in dev)
# ==============================================================================

@app.get("/api/pay/success")
def pay_success():
    return Response("<h3>Payment successful.</h3>", media_type="text/html")


@app.get("/api/pay/cancel")
def pay_cancel():
    return Response("<h3>Payment canceled.</h3>", media_type="text/html")


# ==============================================================================
# PayPal LIVE capture + verify
# ==============================================================================

def _paypal_access_token() -> str:
    if not PAYPAL_CLIENT_ID or not PAYPAL_SECRET:
        raise HTTPException(status_code=500, detail="PayPal env vars missing")
    r = requests.post(
        "https://api-m.paypal.com/v1/oauth2/token",
        data={"grant_type": "client_credentials"},
        headers={"Accept": "application/json", "Accept-Language": "en_US"},
        auth=(PAYPAL_CLIENT_ID, PAYPAL_SECRET),
        timeout=20,
    )
    if not r.ok:
        raise HTTPException(status_code=500, detail="PayPal OAuth failed")
    return r.json()["access_token"]


@app.post("/api/pay/paypal-capture-verify")
def paypal_capture_verify(payload: dict):
    """
    Body: { "order_id": "<paypal order id>", "expected_gbp": "12.34" }
    Returns 200 only if the order is COMPLETED and GBP amount matches.
    """
    order_id = (payload or {}).get("order_id")
    expected = (payload or {}).get("expected_gbp")
    if not order_id:
        raise HTTPException(status_code=400, detail="order_id required")

    token = _paypal_access_token()

    # 1) Try to CAPTURE
    cap = requests.post(
        f"https://api-m.paypal.com/v2/checkout/orders/{order_id}/capture",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "PayPal-Request-Id": order_id,  # idempotency
        },
        timeout=30,
    )
    data = cap.json()

    # If already captured, fall back to GET the order
    if not cap.ok:
        reason = (data.get("details", [{}])[0].get("issue") if isinstance(data, dict) else "")
        if reason == "ORDER_ALREADY_CAPTURED":
            getr = requests.get(
                f"https://api-m.paypal.com/v2/checkout/orders/{order_id}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
            )
            if not getr.ok:
                raise HTTPException(status_code=400, detail=getr.text)
            data = getr.json()
        else:
            raise HTTPException(status_code=400, detail=data)

    # 2) Verify status + amount (GBP)
    status = data.get("status")
    if status != "COMPLETED":
        raise HTTPException(status_code=400, detail=f"Unexpected status: {status}")

    pu = (data.get("purchase_units") or [None])[0] or {}
    captures = ((pu.get("payments") or {}).get("captures")) or []
    total = 0.0
    currency = "GBP"

    if captures:
        for c in captures:
            amt = c.get("amount") or {}
            currency = amt.get("currency_code", currency)
            try:
                total += float(amt.get("value") or 0)
            except Exception:
                pass
    else:
        amt = pu.get("amount") or {}
        currency = amt.get("currency_code", currency)
        try:
            total = float(amt.get("value") or 0)
        except Exception:
            total = 0.0

    if currency != "GBP":
        raise HTTPException(status_code=400, detail=f"Unexpected currency: {currency}")

    if expected:
        try:
            if round(float(expected), 2) != round(total, 2):
                raise HTTPException(
                    status_code=400,
                    detail=f"Amount mismatch: got {total:.2f}, expected {float(expected):.2f}",
                )
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid expected_gbp")

    return {
        "ok": True,
        "order_id": order_id,
        "status": status,
        "amount": f"{total:.2f}",
        "currency": currency,
    }
