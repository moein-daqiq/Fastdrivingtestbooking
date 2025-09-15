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
from dvsa_client import DVSAClient, CaptchaDetected  # <-- uses your dvsa_client.py

# ==============================
# Config / Environment
# ==============================
# Worker keeps its own tiny state DB in /tmp (no PD needed)
DB_FILE = os.environ.get("SEARCHES_DB", "/tmp/worker_state.db")

# API bridge (claim jobs + post status/events)
API_BASE = os.environ.get("API_BASE", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

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
# Your number (recipient)
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
DVSA_RPS = float(os.environ.get("DVSA_RPS", "4.0"))  # global per-worker cap
CB_FAILS_THRESHOLD = int(os.environ.get("CB_FAILS_THRESHOLD", "12"))
CB_COOLDOWN_SEC = int(os.environ.get("CB_COOLDOWN_SEC", "120"))

# Priority weights
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
# DB (worker-only small state)
# ==============================
def get_conn():
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_state_tables():
    """
    Extra state for the worker that doesn't require changing the main API schema.
    - search_state: last slot signature to implement diff-based alerts
    - centre_health: simple circuit breaker counters per centre
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

# ==============================
# API bridge (claim + events + status)
# ==============================
async def _api_post(client: httpx.AsyncClient, path: str, payload: dict):
    if not API_BASE:
        raise RuntimeError("API_BASE not set")
    headers = {}
    if WORKER_TOKEN:
        headers["Authorization"] = f"Bearer {WORKER_TOKEN}"
    r = await client.post(f"{API_BASE}{path}", json=payload, headers=headers)
    r.raise_for_status()
    return r.json()

async def claim_candidates_api(client: httpx.AsyncClient, limit: int) -> List[dict]:
    data = await _api_post(client, "/api/worker/claim", {"limit": limit})
    return data.get("items", [])

async def post_event_api(client: httpx.AsyncClient, sid: int, event: str):
    try:
        await _api_post(client, f"/api/worker/searches/{sid}/event", {"event": event})
    except Exception as e:
        print(f"[worker:event] {sid}: {e}")

_status_cache: dict[int, Tuple[str, str]] = {}

async def set_status_api(client: httpx.AsyncClient, job: dict, status: str, event: str):
    await _api_post(client, f"/api/worker/searches/{job['id']}/status", {"status": status, "event": event})
    _status_cache[job["id"]] = (status, event)

    # Owner notifications for NEW bookings
    if (job.get("booking_type") or "") == "new":
        centres = safe_json_loads(job.get("centres_json"), [])
        if status == "found":
            slot_txt = event.replace("slot_found:", "")
            admin_link = f"\nAdmin: {ADMIN_URL}?status=found" if ADMIN_URL else ""
            wa_owner(
                f"🔔 FASTDTF: Slot FOUND (new booking)\n"
                f"Search #{job['id']}\n"
                f"{slot_txt}\n"
                f"Centres: {', '.join(centres) if centres else '-'}{admin_link}\n"
                f"Act now: open DVSA and pay to confirm."
            )
        elif status == "booked":
            wa_owner(f"✅ FASTDTF: Booking CONFIRMED for search #{job['id']}\n{event.replace('booked:', '')}")
        elif status == "failed":
            wa_owner(f"❌ FASTDTF: Booking FAILED for search #{job['id']}\n{event.replace('booking_failed:', '')}")

def get_status_tuple_from_cache(sid: int) -> Tuple[Optional[str], Optional[str]]:
    return _status_cache.get(sid, (None, None))

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
    """Simple per-host token bucket for RPS limiting."""
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

bucket_dvsa = TokenBucket(DVSA_RPS, DVSA_RPS)

def centre_fail(centre: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT fail_count, cooldown_until FROM centre_health WHERE centre=?", (centre,))
    row = cur.fetchone()
    fc = (row["fail_count"] if row else 0) + 1
    cooldown_until = None
    if fc >= CB_FAILS_THRESHOLD:
        cooldown_until = (datetime.utcnow() + timedelta(seconds=CB_COOLDOWN_SEC)).isoformat() + "Z"
        fc = 0
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
# DVSA integration
# ==============================
async def dvsa_check_centre(client_unused, centre: str, row) -> List[str]:
    """Open DVSA with Playwright and read availability for one centre."""
    if not centre_allowed(centre):
        await asyncio.sleep(0.05)
        return []
    # polite rate limit + jitter
    await bucket_dvsa.acquire()
    await asyncio.sleep(random.randint(0, JITTER_MS) / 1000.0)

    try:
        async with DVSAClient(headless=True) as dvsa:
            if (row.get("booking_type") or "") == "swap":
                await dvsa.login_swap(
                    licence_number=row.get("licence_number") or "",
                    booking_reference=row.get("booking_reference") or "",
                    email=row.get("email") or None,
                )
            else:
                await dvsa.login_new(
                    licence_number=row.get("licence_number") or "",
                    theory_pass=row.get("theory_pass") or None,
                    email=row.get("email") or None,
                )
            # record check event back to API
            await post_event_api(client_unused, row["id"], f"checked:{centre}")
            return await dvsa.search_centre_slots(centre)
    except CaptchaDetected:
        return []
    except Exception as e:
        print(f"[dvsa_check_centre] {centre}: {e}")
        centre_fail(centre)
        return []

async def dvsa_swap_to_slot(client_unused, row, slot: str) -> bool:
    """Attempt an automatic swap when options.auto_book=true and booking_type='swap'."""
    try:
        async with DVSAClient(headless=True) as dvsa:
            await dvsa.login_swap(
                licence_number=row.get("licence_number") or "",
                booking_reference=row.get("booking_reference") or "",
                email=row.get("email") or None,
            )
            return await dvsa.swap_to(slot)
    except CaptchaDetected:
        return False
    except Exception as e:
        print(f"[dvsa_swap_to_slot] {e}")
        return False

async def dvsa_book_and_pay(client_unused, row, slot: str) -> bool:
    """
    New bookings remain assisted (you pay on DVSA).
    We do not automate DVSA payment/SCA, so always return False.
    """
    if AUTOBOOK_MODE == "simulate":
        await asyncio.sleep(0.3 + random.random() * 0.4)
        return random.random() < AUTOBOOK_SIM_SUCCESS_RATE
    return False

# ==============================
# Assist window (15-min alert loop)
# ==============================
async def assist_window_monitor(sid: int, slot_txt: str):
    if not ASSIST_NOTIFY_ENABLED:
        return
    end_at = datetime.utcnow() + timedelta(minutes=ASSIST_NOTIFY_WINDOW_MIN)
    while datetime.utcnow() < end_at:
        status, _ = get_status_tuple_from_cache(sid)
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
    status, _ = get_status_tuple_from_cache(sid)
    if status not in ("booked", "failed"):
        wa_owner(f"⌛ FASTDTF: Assist window ended for search #{sid}. If not booked, the slot may be gone.")

# ==============================
# Core Job Processing
# ==============================
async def process_job(client: httpx.AsyncClient, row: dict):
    sid = row["id"]
    centres: List[str] = safe_json_loads(row.get("centres_json"), [])
    if not centres:
        centres = ["(no centre provided)"]

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

    if not found_slot:
        await set_status_api(client, row, "queued", "no_slots_this_round")
        return

    sig = f"{found_slot}"
    prev = get_last_slot_sig(sid)
    if prev == sig:
        await set_status_api(client, row, "queued", "slot_unchanged")
        return
    save_last_slot_sig(sid, sig)

    options = safe_json_loads(row.get("options_json"), {})
    if AUTOBOOK_ENABLED and options.get("auto_book", False):
        if (row.get("booking_type") or "") == "swap":
            ok = await dvsa_swap_to_slot(client, row, found_slot)
        else:
            ok = await dvsa_book_and_pay(client, row, found_slot)
        if ok:
            await set_status_api(client, row, "booked", f"booked:{found_slot}")
        else:
            await set_status_api(client, row, "failed", f"booking_failed:{found_slot}")
    else:
        await set_status_api(client, row, "found", f"slot_found:{found_slot}")
        asyncio.create_task(assist_window_monitor(sid, found_slot))

# ==============================
# Main loop
# ==============================
async def run():
    limits = httpx.Limits(max_connections=MAX_CONNECTIONS, max_keepalive_connections=MAX_KEEPALIVE)
    timeout = httpx.Timeout(REQUEST_TIMEOUT)
    headers = {"User-Agent": USER_AGENT}

    async with httpx.AsyncClient(http2=True, limits=limits, timeout=timeout, headers=headers) as client:
        print(f"[worker] up state_db={DB_FILE} poll={POLL_SEC}s conc={CONCURRENCY} http2=on rps={DVSA_RPS} mode={AUTOBOOK_MODE} autobook={AUTOBOOK_ENABLED}")
        while True:
            try:
                jobs = await claim_candidates_api(client, CONCURRENCY)
                if not jobs:
                    await asyncio.sleep(POLL_SEC)
                    continue
                # seed cache for assist window
                for j in jobs:
                    _status_cache[j["id"]] = (j.get("status") or "searching", j.get("last_event") or "")
                await asyncio.gather(*(process_job(client, j) for j in jobs))
            except Exception as e:
                print(f"[worker] error: {e}")
                await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(run())
