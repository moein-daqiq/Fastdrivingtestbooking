# ... (imports and setup unchanged)

# ---------- HELPERS ----------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def now_iso() -> str:
    return utcnow().replace(microsecond=0).isoformat()

# unchanged helpers...

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

# keep original _expire_unpaid etc.

# ---------- ROUTES ----------

@app.get("/api/health")
def health():
    return {"ok": True, "ts": now_iso(), "version": "1.4.0"}

# NEW: light lookup used by the frontend to verify cached IDs
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

# ... create_search, disable unchanged ...

@app.post("/api/pay/create-intent")
async def pay_create_intent(body: PayCreateIntentIn, request: Request):
    amount = body.amount_cents if body.amount_cents is not None else body.amount
    if amount is None or amount <= 0:
        raise HTTPException(status_code=400, detail="amount_cents (or amount) must be > 0")

    conn = get_conn()
    _expire_unpaid(conn)

    cur = conn.cursor()
    cur.execute("SELECT id, email, booking_type, status, paid FROM searches WHERE id = ?", (body.search_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="search_id not found")

    if row["paid"]:
        conn.close()
        raise HTTPException(status_code=409, detail="already paid")

    if row["status"] in ("disabled", "expired", "failed", "booked"):
        conn.close()
        raise HTTPException(status_code=409, detail=f"invalid status: {row['status']}")

    amount = int(amount)

    # Accept either band (swap or book) and infer/repair booking_type if needed
    if not (_band_ok(amount, 7000) or _band_ok(amount, 15000)):
        conn.close()
        raise HTTPException(status_code=400, detail="amount not in allowed range")

    inferred = _infer_booking_type(amount)
    existing_type = (row["booking_type"] or "").strip() or None
    if inferred and inferred != existing_type:
        try:
            cur.execute(
                "UPDATE searches SET booking_type=?, updated_at=? WHERE id=?",
                (inferred, now_iso(), body.search_id),
            )
            conn.commit()
        except Exception:
            pass  # best-effort; not fatal

    md = {"search_id": str(body.search_id), "email": (row["email"] or "")}
    if body.metadata:
        try:
            for k, v in body.metadata.items():
                if k != "search_id":
                    md[k] = v
        except Exception:
            pass

    try:
        # One PaymentIntent per search idempotently
        intent = stripe.PaymentIntent.create(
            amount=amount,
            currency=body.currency,
            description=body.description or f"Search #{body.search_id}",
            metadata=md,
            automatic_payment_methods={"enabled": True},
            idempotency_key=f"pi_search_{body.search_id}",
        )
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=400, detail=str(e))

    cur.execute(
        "UPDATE searches SET payment_intent_id = ?, amount = ?, last_event = ?, updated_at = ? WHERE id = ?",
        (intent["id"], amount, "payment_intent.created", now_iso(), body.search_id)
    )
    conn.commit()
    conn.close()
    return {
        "client_secret": intent["client_secret"],
        "clientSecret": intent["client_secret"],
        "paymentIntentId": intent["id"]
    }

# ... webhook, admin, worker endpoints unchanged ...
