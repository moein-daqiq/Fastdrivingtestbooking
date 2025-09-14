import os
import json
import math
import time
import random
import sqlite3
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import httpx

# ==============================
# Config / Environment
# ==============================
DB_FILE = os.environ.get("SEARCHES_DB", "searches.db")

# Poll + concurrency
POLL_SEC = int(os.environ.get("WORKER_POLL_SEC", "30"))
CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "8"))
JOB_MAX_PARALLEL_CHECKS = int(os.environ.get("JOB_MAX_PARALLEL_CHECKS", "4"))

# HTTP tuning
MAX_CONNECTIONS = int(os.environ.get("WORKER_MAX_CONNECTIONS", "40"))
MAX_KEEPALIVE = int(os.environ.get("WORKER_MAX_KEEPALIVE", "20"))
REQUEST_TIMEOUT = float(os.environ.get("WORKER_TIMEOUT_SEC", "10"))
JITTER_MS = int(os.environ.get("WORKER_JITTER_MS", "400"))
USER_AGENT = os.environ.get("WORKER_USER_AGENT", "FastDTF/1.0 (+https://fastdrivingtestfinder.co.uk)")

# Notifications (generic webhook optional)
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "")

# WhatsApp (Twilio) — TO you; FROM must be your Twilio sender (sandbox or approved)
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
# Twilio Sandbox sender default; replace with your approved WA sender if you have one
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
# Your number (recipient) — default to your provided number
WHATSAPP_OWNER_TO = os.environ.get("WHATSAPP_OWNER_TO", "whatsapp:+447402597000")
ADMIN_URL = os.environ.get("ADMIN_URL", "")

# Assist window (owner pings during DVSA hold)
ASSIST_NOTIFY_WINDOW_MIN = int(os.environ.get("ASSIST_NOTIFY_WINDOW_MIN", "15"))     # minutes
ASSIST_NOTIFY_PING_SECONDS = int(os.environ.get("ASSIST_NOTIFY_PING_SECONDS", "60")) # seconds
ASSIST_NOTIFY_ENABLED = os.environ.get("ASSIST_NOTIFY_ENABLED", "true").lower() == "true"

# Auto-book flags
AUTOBOOK_ENABLED = os.environ.get("AUTOBOOK_ENABLED", "true").lower() == "true"
AUTOBOOK_MODE = os.environ.get("AUTOBOOK_MODE", "simulate")  # simulate | real
AUTOBOOK_SIM_SUCCESS_RATE = float(os.environ.get("AUTOBOOK_SIM_SUCCESS_RATE", "0.85"))

# Rate limiting / Circuit breaker
# Allow N requests/sec per "host" bucket (conceptual; stubs keep the shape)
DVSA_RPS = float(os.environ.get("DVSA_RPS", "4.0"))  # global per-worker cap
CB_FAILS_THRESHOLD = int(os.environ.get("CB_FAILS_THRESHOLD", "12"))
CB_COOLDOWN_SEC = int(os.environ.get("CB_COOLDOWN_SEC", "120"))

# Priority weights (1–10 ideas baked in)
W_DATE_URGENCY = float(os.environ.get("W_DATE_URGENCY", "5.0"))
W_DATE_WINDOW_WIDTH = float(os.environ.get("W_DATE_WINDOW_WIDTH", "3.0"))
W_TIME_WINDOW_WIDTH = float(os.environ.get("W_TIME_WINDOW_WIDTH", "2.0"))
W_AGE_BOOST = float(os.environ.get("W_AGE_BOOST", "1.5"))
W_SWAP_BONUS = float(os.environ.get("W_SWAP_BONUS", "0.5"))

# Lazily import Twilio if creds exist
_twilio_client = None
def _get_twilio():
    global _twilio_client
    if _twilio_client is None and TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
        try:
            from twilio.rest import Client
            _twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        except Exception as e:
            print(f"[whatsapp] Twilio init failed: {e}")
            _twilio_client = False
    return _twilio_client

# ==============================
# Utils
# ==============================
def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def parse_date(d: Optional[str]) -> Optional[datetime]:
    if not d:
        return None
    try:
        return datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return None

def parse_time(t: Optional[str]) -> Optional[Tuple[int,int]]:
    if not t:
        return None
    try:
        hh, mm = t.split(":")
        return int(hh), int(mm)
    except Exception:
        return None

def time_diff_minutes(a: Optional[Tuple[int,int]], b: Optional[Tuple[int,int]]) -> Optional[int]:
    if not a or not b:
        return None
    return (b[0]*60 + b[1]) - (a[0]*60 + a[1])

def safe_json_loads(s: Optional[str], default):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default

# ==============================
# DB
# ==============================
def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_state_tables():
    """
    Extra state for the worker that doesn't require changing the main API schema.
    - search_state: last slot signature to implement diff-based alerts (idea #5)
    - centre_health: simple circuit breaker counters per centre (idea #7)
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS search_state (
            search_id INTEGER PRIMARY KEY,
            last_slot_sig TEXT,
            last_found_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS centre_health (
            centre TEXT PRIMARY KEY,
            fail_count INTEGER DEFAULT 0,
            cooldown_until TEXT
        )
    """)
    conn.commit()
    conn.close()

ensure_state_tables()

def set_last_event(sid: int, event: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE searches SET last_event=?, updated_at=? WHERE id=?",
                (event, now_iso(), sid))
    conn.commit()
    conn.close()

def set_status(sid: int, status: str, event: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE searches SET status=?, last_event=?, updated_at=? WHERE id=?",
                (status, event, now_iso(), sid))
    conn.commit()
    cur.execute("SELECT * FROM searches WHERE id=?", (sid,))
    row = cur.fetchone()
    conn.close()

    payload = {
        "id": sid,
        "status": status,
        "last_event": event,
        "booking_type": row["booking_type"],
        "phone": row["phone"],
        "email": row["email"],
        "centres": safe_json_loads(row["centres_json"], [])
    }
    notify("search.status", payload)

    # Notify YOU for new bookings (action needed)
    if row["booking_type"] == "new":
        if status == "found":
            centres = payload["centres"]
            slot_txt = event.replace("slot_found:", "")
            admin_link = f"\nAdmin: {ADMIN_URL}?status=found" if ADMIN_URL else ""
            wa_owner(
                f"🔔 FASTDTF: Slot FOUND (new booking)\n"
                f"Search #{sid}\n"
                f"{slot_txt}\n"
                f"Centres: {', '.join(centres) if centres else '-'}{admin_link}\n"
                f"Act now: open DVSA and pay to confirm."
            )
        elif status == "booked":
            wa_owner(f"✅ FASTDTF: Booking CONFIRMED for search #{sid}\n{event.replace('booked:', '')}")
        elif status == "failed":
            wa_owner(f"❌ FASTDTF: Booking FAILED for search #{sid}\n{event.replace('booking_failed:', '')}")

def get_status_tuple(sid: int) -> Tuple[Optional[str], Optional[str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT status, last_event FROM searches WHERE id=?", (sid,))
    row = cur.fetchone()
    conn.close()
    return (row["status"], row["last_event"]) if row else (None, None)

def save_last_slot_sig(sid: int, sig: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO search_state(search_id, last_slot_sig, last_found_at)
        VALUES (?,?,?)
        ON CONFLICT(search_id) DO UPDATE SET last_slot_sig=excluded.last_slot_sig, last_found_at=excluded.last_found_at
    """, (sid, sig, now_iso()))
    conn.commit()
    conn.close()

def get_last_slot_sig(sid: int) -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_slot_sig FROM search_state WHERE search_id=?", (sid,))
    row = cur.fetchone()
    conn.close()
    return row["last_slot_sig"] if row else None

def claim_candidates(limit: int) -> List[sqlite3.Row]:
    """
    Claim up to `limit` jobs, but **prioritize** using a score derived from:
      - date urgency (earlier date_from)
      - narrower date window
      - narrower time window
      - age (older 'updated_at' gets a boost)
      - swap bonus (fast, no payment)
    Implements idea #1, #2, #3, #4, #8, #10 indirectly.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM searches
         WHERE paid=1 AND status IN ('queued','new')
    """)
    rows = cur.fetchall()
    conn.close()

    scored = []
    now = datetime.utcnow()
    for r in rows:
        # Date urgency
        d_from = parse_date(r["date_window_from"])
        d_to   = parse_date(r["date_window_to"])
        t_from = parse_time(r["time_window_from"])
        t_to   = parse_time(r["time_window_to"])

        date_urg = 0.0
        if d_from:
            days = (d_from - now).days
            date_urg = 1.0 / max(1.0, days + 1)  # earlier => bigger

        # Date-window narrowness
        date_span = 0.0
        if d_from and d_to:
            span_days = max(0, (d_to - d_from).days + 1)
            date_span = 1.0 / max(1.0, span_days)  # narrower => bigger

        # Time-window narrowness
        time_span_min = time_diff_minutes(t_from, t_to)
        time_span = 0.0
        if time_span_min is not None and time_span_min > 0:
            time_span = 1.0 / max(30.0, float(time_span_min))  # narrower => bigger

        # Age boost
        try:
            updated = datetime.fromisoformat((r["updated_at"] or "").replace("Z",""))
            age_min = max(1.0, (now - updated).total_seconds() / 60.0)
        except Exception:
            age_min = 60.0
        age_boost = math.log10(age_min + 10.0) / 2.0

        # Swap bonus
        swap_bonus = 1.0 if (r["booking_type"] or "") == "swap" else 0.0

        score = (W_DATE_URGENCY*date_urg +
                 W_DATE_WINDOW_WIDTH*date_span +
                 W_TIME_WINDOW_WIDTH*time_span +
                 W_AGE_BOOST*age_boost +
                 W_SWAP_BONUS*swap_bonus)

        scored.append((score, r))

    # Highest scores first, then trim
    scored.sort(key=lambda x: x[0], reverse=True)
    chosen = [r for _, r in scored[:limit]]

    # Claim chosen (flip to searching atomically)
    conn = get_conn()
    cur = conn.cursor()
    claimed: List[sqlite3.Row] = []
    for r in chosen:
        cur.execute("""
            UPDATE searches
               SET status='searching', last_event='worker_claimed', updated_at=?
             WHERE id=? AND status IN ('queued','new') AND paid=1
        """, (now_iso(), r["id"]))
        if cur.rowcount:
            cur.execute("SELECT * FROM searches WHERE id=?", (r["id"],))
            claimed.append(cur.fetchone())
    conn.commit()
    conn.close()
    return claimed

# ==============================
# Notifications
# ==============================
def notify(event: str, payload: dict):
    if NOTIFY_WEBHOOK_URL:
        try:
            with httpx.Client(timeout=5.0) as c:
                c.post(NOTIFY_WEBHOOK_URL, json={"event": event, "data": payload})
        except Exception as e:
            print(f"[notify] webhook failed: {e}")
    else:
        print(f"[notify] {event}: {json.dumps(payload, ensure_ascii=False)}")

def wa_owner(message: str):
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM and WHATSAPP_OWNER_TO):
        return
    client = _get_twilio()
    if not client:
        return
    try:
        client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=WHATSAPP_OWNER_TO,
            body=message[:1500]
        )
    except Exception as e:
        print(f"[whatsapp] send failed: {e}")

# ==============================
# Rate Limiter & Circuit Breaker
# ==============================
class TokenBucket:
    """Simple per-host token bucket for RPS limiting (idea #4)."""
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.last = time.monotonic()

    async def acquire(self):
        while True:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep(0.01)

bucket_dvsa = TokenBucket(DVSA_RPS, DVSA_RPS)  # simple single-bucket

def centre_fail(centre: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT fail_count, cooldown_until FROM centre_health WHERE centre=?", (centre,))
    row = cur.fetchone()
    fc = (row["fail_count"] if row else 0) + 1
    cooldown_until = None
    if fc >= CB_FAILS_THRESHOLD:
        cooldown_until = (datetime.utcnow() + timedelta(seconds=CB_COOLDOWN_SEC)).isoformat() + "Z"
        fc = 0  # reset after applying cooldown
    if row:
        cur.execute("UPDATE centre_health SET fail_count=?, cooldown_until=? WHERE centre=?", (fc, cooldown_until, centre))
    else:
        cur.execute("INSERT INTO centre_health(centre, fail_count, cooldown_until) VALUES (?,?,?)", (centre, fc, cooldown_until))
    conn.commit()
    conn.close()

def centre_allowed(centre: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT cooldown_until FROM centre_health WHERE centre=?", (centre,))
    row = cur.fetchone()
    conn.close()
    if not row or not row["cooldown_until"]:
        return True
    try:
        until = datetime.fromisoformat(row["cooldown_until"].replace("Z",""))
        return datetime.utcnow() >= until
    except Exception:
        return True

# ==============================
# DVSA Stubs (replace later)
# Lightweight parsing; raw HTTP preferred (idea #6, #8)
# ==============================
async def dvsa_check_centre(client: httpx.AsyncClient, centre: str, row) -> List[str]:
    if not centre_allowed(centre):
        # Respect circuit breaker cooldown
        await asyncio.sleep(0.05)
        return []

    # Rate limit + jitter (idea #4)
    await bucket_dvsa.acquire()
    await asyncio.sleep(random.randint(0, JITTER_MS) / 1000.0)

    # ---- SIMULATED CALL ----
    # Keep this extremely light; when replaced with real call, parse only what you need.
    t0 = time.perf_counter()
    try:
        await asyncio.sleep(0.2 + random.random() * 0.25)  # simulate latency
        set_last_event(row["id"], f"checked:{centre}")
        # pseudo-availability pattern
        found = (hash(f"{row['id']}::{centre}") % 7 == 0)
        slots = [f"{centre} · {datetime.utcnow().date()} 08:10"] if found else []
        return slots
    except Exception:
        centre_fail(centre)
        return []
    finally:
        # observability (idea #10)
        dt = (time.perf_counter() - t0) * 1000
        print(f"[check] centre='{centre}' dt_ms={dt:.1f}")

async def dvsa_swap_to_slot(client: httpx.AsyncClient, row, slot: str) -> bool:
    await asyncio.sleep(0.2 + random.random() * 0.2)
    return True  # swaps are usually straightforward

async def dvsa_book_and_pay(client: httpx.AsyncClient, row, slot: str) -> bool:
    if AUTOBOOK_MODE == "simulate":
        await asyncio.sleep(0.3 + random.random() * 0.4)
        return random.random() < AUTOBOOK_SIM_SUCCESS_RATE
    else:
        # Real autopay requires an approved DVSA integration (not Stripe).
        raise NotImplementedError("REAL DVSA autopay requires DVSA-approved flow.")

# ==============================
# Assist window (15-min alert loop)
# ==============================
async def assist_window_monitor(sid: int, slot_txt: str):
    if not ASSIST_NOTIFY_ENABLED:
        return
    end_at = datetime.utcnow() + timedelta(minutes=ASSIST_NOTIFY_WINDOW_MIN)
    while datetime.utcnow() < end_at:
        status, _ = get_status_tuple(sid)
        if status in ("booked", "failed"):
            return
        mins_left = int((end_at - datetime.utcnow()).total_seconds() // 60)
        secs_left = int((end_at - datetime.utcnow()).total_seconds() % 60)
        wa_owner(
            f"⏳ FASTDTF: Slot still available? Search #{sid}\n"
            f"{slot_txt}\n"
            f"Time left (~DVSA hold): {mins_left:02d}:{secs_left:02d}\n"
            f"If you’ve paid already, ignore this."
        )
        await asyncio.sleep(ASSIST_NOTIFY_PING_SECONDS)
    # final ping if unresolved
    status, _ = get_status_tuple(sid)
    if status not in ("booked", "failed"):
        wa_owner(f"⌛ FASTDTF: Assist window ended for search #{sid}. If not booked, the slot may be gone.")

# ==============================
# Core Job Processing
# ==============================
async def process_job(client: httpx.AsyncClient, row):
    sid = row["id"]
    centres: List[str] = safe_json_loads(row["centres_json"], [])
    if not centres:
        centres = ["(no centre provided)"]

    # Limit the search surface strictly to selected centres (idea #2)
    # Per-job parallelism cap (idea #4)
    sem = asyncio.Semaphore(JOB_MAX_PARALLEL_CHECKS)
    found_slot: Optional[str] = None

    async def check_one(c: str):
        nonlocal found_slot
        async with sem:
            if found_slot is not None:
                return
            slots = await dvsa_check_centre(client, c, row)
            if slots and found_slot is None:
                found_slot = slots[0]

    await asyncio.gather(*(asyncio.create_task(check_one(c)) for c in centres))

    # No slot found this round → return to queue (idea #5 diff-based alerts handled below)
    if not found_slot:
        set_status(sid, "queued", "no_slots_this_round")
        return

    # Diff-based alert: avoid spamming the same "found" repeatedly (idea #5)
    sig = f"{found_slot}"
    prev = get_last_slot_sig(sid)
    if prev == sig:
        # We already alerted this exact slot; just re-queue (light nudge)
        set_status(sid, "queued", "slot_unchanged")
        return
    save_last_slot_sig(sid, sig)

    # Found a slot — act according to options
    options = safe_json_loads(row["options_json"], {})
    if AUTOBOOK_ENABLED and options.get("auto_book", False):
        if row["booking_type"] == "swap":
            ok = await dvsa_swap_to_slot(client, row, found_slot)
        else:
            ok = await dvsa_book_and_pay(client, row, found_slot)
        if ok:
            set_status(sid, "booked", f"booked:{found_slot}")
        else:
            set_status(sid, "failed", f"booking_failed:{found_slot}")
    else:
        set_status(sid, "found", f"slot_found:{found_slot}")
        asyncio.create_task(assist_window_monitor(sid, found_slot))

# ==============================
# Main loop
# ==============================
async def run():
    limits = httpx.Limits(max_connections=MAX_CONNECTIONS, max_keepalive_connections=MAX_KEEPALIVE)
    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    headers = {"User-Agent": USER_AGENT}

    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout, headers=headers) as client:
        print(f"[worker] up db={DB_FILE} poll={POLL_SEC}s conc={CONCURRENCY} http2=on rps={DVSA_RPS} mode={AUTOBOOK_MODE} autobook={AUTOBOOK_ENABLED}")
        while True:
            try:
                jobs = claim_candidates(CONCURRENCY)
                if not jobs:
                    await asyncio.sleep(POLL_SEC)
                    continue
                await asyncio.gather(*(process_job(client, j) for j in jobs))
            except Exception as e:
                print(f"[worker] error: {e}")
                await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(run())
