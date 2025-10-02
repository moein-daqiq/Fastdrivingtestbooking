# --- worker.py (priority centres + kill-switch + CAPTCHA WhatsApp alert + resume link + better logging + more attempts) ---

import os, json, math, time, random, sqlite3, asyncio, subprocess, hashlib, re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import httpx
from dvsa_client import DVSAClient, CaptchaDetected  # <-- your dvsa_client.py

import hmac
import base64
from urllib.parse import urlencode

# ==============================
# Config / Environment
# ==============================
DB_FILE = os.environ.get("SEARCHES_DB", "/tmp/worker_state.db")
API_BASE = os.environ.get("API_BASE", "").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

POLL_SEC = int(os.environ.get("WORKER_POLL_SEC", "30"))
CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "8"))
JOB_MAX_PARALLEL_CHECKS = int(os.environ.get("JOB_MAX_PARALLEL_CHECKS", "4"))

MAX_CONNECTIONS = int(os.environ.get("WORKER_MAX_CONNECTIONS", "40"))
MAX_KEEPALIVE = int(os.environ.get("WORKER_MAX_KEEPALIVE", "20"))
REQUEST_TIMEOUT = float(os.environ.get("WORKER_TIMEOUT_SEC", "10"))
JITTER_MS = int(os.environ.get("WORKER_JITTER_MS", "400"))
USER_AGENT = os.environ.get("WORKER_USER_AGENT", "FastDTF/1.0 (+https://fastdrivingtestfinder.co.uk)")

NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "")

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
WHATSAPP_OWNER_TO = os.environ.get("WHATSAPP_OWNER_TO", "whatsapp:+447402597000")
ADMIN_URL = os.environ.get("ADMIN_URL", "")

ASSIST_NOTIFY_WINDOW_MIN = int(os.environ.get("ASSIST_NOTIFY_WINDOW_MIN", "15"))
ASSIST_NOTIFY_PING_SECONDS = int(os.environ.get("ASSIST_NOTIFY_PING_SECONDS", "60"))
ASSIST_NOTIFY_ENABLED = os.environ.get("ASSIST_NOTIFY_ENABLED", "true").lower() == "true"

AUTOBOOK_ENABLED = os.environ.get("AUTOBOOK_ENABLED", "true").lower() == "true"
AUTOBOOK_MODE = os.environ.get("AUTOBOOK_MODE", "simulate")  # simulate | real
AUTOBOOK_SIM_SUCCESS_RATE = float(os.environ.get("AUTOBOOK_SIM_SUCCESS_RATE", "0.85"))

# IMPORTANT:
# Let dvsa_client.py enforce the per-request DVSA_RPS (default there is 0.5) to avoid *double throttling*.
# If you want to keep THIS worker's token-bucket instead, set HONOUR_CLIENT_RPS=false.
HONOUR_CLIENT_RPS = os.environ.get("HONOUR_CLIENT_RPS", "true").lower() == "true"

DVSA_RPS = float(os.environ.get("DVSA_RPS", "4.0"))  # only used if HONOUR_CLIENT_RPS=false
CB_FAILS_THRESHOLD = int(os.environ.get("CB_FAILS_THRESHOLD", "12"))
CB_COOLDOWN_SEC = int(os.environ.get("CB_COOLDOWN_SEC", "120"))

CAPTCHA_COOLDOWN_MIN = int(os.environ.get("CAPTCHA_COOLDOWN_MIN", "30"))
QUIET_HOURS = os.environ.get("QUIET_HOURS", "").strip()

# DVSA entry points (overrideable from env, used in CAPTCHA alerts)
DVSA_URL_CHANGE = os.environ.get("DVSA_URL_CHANGE", "https://driverpracticaltest.dvsa.gov.uk/manage")
DVSA_URL_BOOK   = os.environ.get("DVSA_URL_BOOK",   "https://driverpracticaltest.dvsa.gov.uk/application")

# Optional backend "resume now" endpoint (used in WhatsApp)
RESUME_URL = os.environ.get("RESUME_URL", "").rstrip("/")

# Stale search expiry
STALE_MINUTES = int(os.environ.get("STALE_MINUTES", "5"))

# Priority centres env (comma-separated)
PRIORITY_CENTRES = [
    c.strip().lower() for c in os.environ.get("PRIORITY_CENTRES", "").split(",") if c.strip()
]
CURRENT_PRIORITY_CENTRES = PRIORITY_CENTRES[:]  # live-updated from API controls

# Global controls (kill-switch + live priority)
CTRL: Dict[str, object] = {"pause_all": False, "priority_centres": CURRENT_PRIORITY_CENTRES[:]}

os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", "/opt/render/.cache/ms-playwright")
os.environ.setdefault("FASTDTF_LAUNCH_ARGS", "--no-sandbox --disable-dev-shm-usage")

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
# One-time self-heal for Playwright
# ==============================
def ensure_browser():
    try:
        subprocess.run(
            ["python", "-m", "playwright", "install", "chromium"],
            check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        print("[worker] playwright chromium ready")
    except Exception as e:
        print(f"[worker] playwright install warning: {e}")

ensure_browser()

# ==============================
# Utils
# ==============================
def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def safe_json_loads(s: Optional[str], default):
    try:
        return json.loads(s) if s else default
    except Exception:
        return default

def _parse_quiet_hours(spec: str):
    out = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part or "-" not in part:
            continue
        a, b = part.split("-", 1)
        try: out.append((int(a), int(b)))
        except: pass
    return out

def is_quiet_now() -> bool:
    if not QUIET_HOURS: return False
    now_h = datetime.now(timezone.utc).hour
    for s, e in _parse_quiet_hours(QUIET_HOURS):
        if s == e: return True
        if s < e and s <= now_h < e: return True
        if s > e and (now_h >= s or now_h < e): return True
    return False

def normalize_centres(centres: List[str]) -> List[str]:
    cleaned: List[str] = []
    for c in centres:
        c0 = (c or "").strip()
        if not c0: continue
        parts = c0.split()
        if len(parts) >= 2 and parts[0].lower() in {"london", "city", "borough"}:
            c0 = " ".join(parts[1:])
        cleaned.append(c0)
    return cleaned

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts: return None
    try:
        ts2 = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts2)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _is_stale(created_ts: Optional[str], minutes: int) -> bool:
    dt = _parse_iso(created_ts)
    if not dt: return False
    return (datetime.now(timezone.utc) - dt) > timedelta(minutes=minutes)

def _human(e: Exception) -> str:
    s = str(e).strip().split("\n")[0]
    return s[:220]

def _missing_required_fields(row: dict) -> Optional[str]:
    booking_type = (row.get("booking_type") or "").strip()
    licence = (row.get("licence_number") or "").strip()
    ref = (row.get("booking_reference") or "").strip()
    if not licence:
        return "missing_licence_number"
    if booking_type == "swap":
        if not (len(ref) == 8 and ref.isdigit()):
            return "missing_or_bad_booking_reference"
    return None

# ==============================
# DB (tiny local state)
# ==============================
def get_conn():
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception: pass
    return conn

def ensure_state_tables():
    conn = get_conn(); cur = conn.cursor()
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
    conn.commit(); conn.close()

ensure_state_tables()

def save_last_slot_sig(sid: int, sig: str):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO search_state(search_id, last_slot_sig, last_found_at)
        VALUES (?,?,?)
        ON CONFLICT(search_id) DO UPDATE
          SET last_slot_sig=excluded.last_slot_sig,
              last_found_at=excluded.last_found_at
    """, (sid, sig, now_iso()))
    conn.commit(); conn.close()

def get_last_slot_sig(sid: int) -> Optional[str]:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT last_slot_sig FROM search_state WHERE search_id=?", (sid,))
    row = cur.fetchone(); conn.close()
    return row["last_slot_sig"] if row else None

# ==============================
# API bridge
# ==============================
async def _api_post(client: httpx.AsyncClient, path: str, payload: dict):
    if not API_BASE: raise RuntimeError("API_BASE not set")
    url = f"{API_BASE}{path}"
    headers = {"Authorization": f"Bearer {WORKER_TOKEN}"} if WORKER_TOKEN else {}
    attempts, last_err = 6, None
    for i in range(attempts):
        try:
            r = await client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if i in (0, attempts - 1):
                print(f"[api] POST {path} attempt {i+1}/{attempts} failed: {_human(e)}")
            await asyncio.sleep(min(2.5, 0.25 * (2 ** i)) + random.random() * 0.2)
    raise last_err

async def _api_get(client: httpx.AsyncClient, path: str):
    if not API_BASE: raise RuntimeError("API_BASE not set")
    url = f"{API_BASE}{path}"
    headers = {"Authorization": f"Bearer {WORKER_TOKEN}"} if WORKER_TOKEN else {}
    attempts, last_err = 3, None
    for i in range(attempts):
        try:
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.3 * (i + 1))
    raise last_err

async def claim_candidates_api(client: httpx.AsyncClient, limit: int) -> List[dict]:
    data = await _api_post(client, "/api/worker/claim", {"limit": limit})
    return data.get("items", [])

async def post_event_api(client: httpx.AsyncClient, sid: int, event: str):
    try:
        await _api_post(client, f"/api/worker/searches/{sid}/event", {"event": event})
    except Exception as e:
        print(f"[worker:event] {sid}: {_human(e)}")

async def get_controls_api(client: httpx.AsyncClient) -> dict:
    try:
        return await _api_get(client, "/api/worker/controls")
    except Exception:
        return {}

_status_cache: dict[int, Tuple[Optional[str], Optional[str]]] = {}
async def set_status_api(client: httpx.AsyncClient, job: dict, status: str, event: str):
    await _api_post(client, f"/api/worker/searches/{job['id']}/status", {"status": status, "event": event})
    _status_cache[job["id"]] = (status, event)

    if (job.get("booking_type") or "") == "new":
        centres = safe_json_loads(job.get("centres_json"), [])
        if status == "found":
            slot_txt = event.replace("slot_found:", "")
            admin_link = f"\nAdmin: {ADMIN_URL}?status=found" if ADMIN_URL else ""
            wa_owner(
                f"🔔 FASTDTF: Slot FOUND (new booking)\n"
                f"Search #{job['id']}\n{slot_txt}\n"
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
    if not client: return
    try:
        client.messages.create(from_=TWILIO_WHATSAPP_FROM, to=WHATSAPP_OWNER_TO, body=message[:1500])
    except Exception as e:
        print(f"[whatsapp] send failed: {e}")

# ==============================
# Rate Limiter & Circuit Breaker
# ==============================
class TokenBucket:
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

class _NoopBucket:
    async def acquire(self):
        return

# Only use the worker bucket when HONOUR_CLIENT_RPS is False
bucket_dvsa = TokenBucket(DVSA_RPS, DVSA_RPS) if not HONOUR_CLIENT_RPS else _NoopBucket()

def centre_fail(centre: str):
    conn = get_conn(); cur = conn.cursor()
    new_until = (datetime.now(timezone.utc) + timedelta(seconds=CB_COOLDOWN_SEC)).isoformat()
    cur.execute(
        """
        INSERT INTO centre_health(centre, fail_count, cooldown_until)
        VALUES (?, 1, NULL)
        ON CONFLICT(centre) DO UPDATE SET
          fail_count = CASE
              WHEN COALESCE(centre_health.fail_count,0) + 1 >= ?
                  THEN 0
              ELSE centre_health.fail_count + 1
          END,
          cooldown_until = CASE
              WHEN COALESCE(centre_health.fail_count,0) + 1 >= ?
                  THEN ?
              ELSE centre_health.cooldown_until
          END
        """,
        (centre, CB_FAILS_THRESHOLD, CB_FAILS_THRESHOLD, new_until),
    )
    conn.commit(); conn.close()

def centre_allowed(centre: str) -> bool:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT cooldown_until FROM centre_health WHERE centre=?", (centre,))
    row = cur.fetchone(); conn.close()
    if not row or not row["cooldown_until"]: return True
    try:
        until = datetime.fromisoformat(row["cooldown_until"].replace("Z","+00:00"))
        return datetime.now(timezone.utc) >= until.astimezone(timezone.utc)
    except Exception:
        return True

def global_cooldown(extra_min: int):
    conn = get_conn(); cur = conn.cursor()
    until = (datetime.now(timezone.utc) + timedelta(minutes=max(1, extra_min))).isoformat()
    cur.execute("""
        INSERT INTO centre_health(centre, fail_count, cooldown_until)
        VALUES ('*global*', 0, ?)
        ON CONFLICT(centre) DO UPDATE SET cooldown_until=excluded.cooldown_until
    """, (until,))
    conn.commit(); conn.close()

def global_allowed() -> bool:
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT cooldown_until FROM centre_health WHERE centre='*global*'")
    row = cur.fetchone(); conn.close()
    if not row or not row["cooldown_until"]: return True
    try:
        return datetime.now(timezone.utc) >= datetime.fromisoformat(row["cooldown_until"].replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return True

# ==============================
# Session identity helpers
# ==============================
def session_base(row: dict) -> str:
    # new -> licence, swap -> booking reference
    if (row.get("booking_type") or "").strip() == "swap":
        return (row.get("booking_reference") or f"job{row['id']}").strip()
    return (row.get("licence_number") or f"job{row['id']}").strip()

def make_session_key(row: dict, centre: Optional[str] = None) -> str:
    base = session_base(row)
    trunk = re.sub(r"[^A-Za-z0-9]+", "", base)[:12] or "anon"
    digest_src = f"{base}|{row.get('id')}|{centre or ''}"
    h = hashlib.sha1(digest_src.encode("utf-8")).hexdigest()[:8]
    return f"{trunk}-{h}"

# ==============================
# CAPTCHA alert helper (WhatsApp with DVSA link) + debounce + resume link
# ==============================
CAPTCHA_ALERT_SENT: Dict[str, float] = {}  # key = session_base(row); value=epoch seconds

def _captcha_url_for(row: dict) -> str:
    """Return the best DVSA entry link for this job."""
    typ = (row.get("booking_type") or "").strip().lower()
    base = DVSA_URL_CHANGE if typ == "swap" else DVSA_URL_BOOK
    # Include sid as a benign marker so you know which job triggered it
    sid = row.get("id")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}sid={sid}"

def _sign(s: str) -> str:
    """HMAC-SHA256 signature using WORKER_TOKEN (base64url, no padding)."""
    if not WORKER_TOKEN:
        return ""
    mac = hmac.new(WORKER_TOKEN.encode("utf-8"), s.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(mac).decode("ascii").rstrip("=")

def _resume_link_for(row: dict) -> Optional[str]:
    """Create a signed 'resume this job' link for your backend, or None if RESUME_URL empty."""
    if not RESUME_URL:
        return None
    sid = int(row.get("id"))
    ident = session_base(row)
    sig = _sign(f"{sid}|{ident}")
    q = {"sid": sid, "ident": ident, "sig": sig}
    return f"{RESUME_URL}?{urlencode(q)}"

def send_captcha_alert(row: dict, centre: str):
    """Send a single WhatsApp alert (debounced 10m per identity) with the DVSA link + optional Resume link."""
    ident = session_base(row)
    now = time.time()
    last = CAPTCHA_ALERT_SENT.get(ident, 0.0)
    if now - last < 600:  # 10 minutes debounce
        return
    CAPTCHA_ALERT_SENT[ident] = now

    url = _captcha_url_for(row)
    resume = _resume_link_for(row)
    sid = row.get("id")
    typ = (row.get("booking_type") or "").strip().lower() or "new"

    extra = f"\nResume this job now:\n{resume}\n" if resume else ""

    msg = (
        "🧩 FASTDTF: CAPTCHA required\n"
        f"Search #{sid} ({typ})\n"
        f"Centre: {centre}\n\n"
        f"Open DVSA and complete the CAPTCHA:\n{url}\n\n"
        f"After you pass the CAPTCHA, tap resume so I try again immediately."
        f"{extra}"
        f"(Also cooling for ~{CAPTCHA_COOLDOWN_MIN}m to be safe.)"
    )
    wa_owner(msg)

# ==============================
# DVSA integration
# ==============================
async def dvsa_check_centre(client_httpx: httpx.AsyncClient, centre: str, row) -> List[str]:
    # Optional resume override: if last event was 'resume_now', ignore cooldown checks once
    _, last_event = get_status_tuple_from_cache(row["id"])
    resume_override = (last_event == "resume_now")

    # NEW: obey global pause before doing anything
    if CTRL.get("pause_all"):
        try:
            await post_event_api(client_httpx, row["id"], "paused:global")
        except Exception:
            pass
        await asyncio.sleep(0.05)
        return []

    if not resume_override and not global_allowed():
        await post_event_api(client_httpx, row["id"], f"cooldown_skip:*global*")
        await asyncio.sleep(0.05); return []
    if not resume_override and not centre_allowed(centre):
        await post_event_api(client_httpx, row["id"], f"cooldown_skip:{centre}")
        await asyncio.sleep(0.05); return []

    # Worker-side throttle (NOOP if HONOUR_CLIENT_RPS=true)
    await bucket_dvsa.acquire()
    await asyncio.sleep(random.randint(0, JITTER_MS) / 1000.0)

    # Double-check pause just before network activity
    if CTRL.get("pause_all"):
        try:
            await post_event_api(client_httpx, row["id"], "paused:global")
        except Exception:
            pass
        return []

    session_key = make_session_key(row, centre)
    attempts = 5  # a bit more persistent

    for attempt in range(1, attempts + 1):
        try:
            async with DVSAClient(headless=True, session_key=session_key) as dvsa:
                # IMPORTANT: attach sid so dvsa_client can post events/status to API
                dvsa.attach_job(int(row["id"]))

                await post_event_api(client_httpx, row["id"], f"login_start:{centre}")
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
                await post_event_api(client_httpx, row["id"], f"login_ok:{centre}")

                slots = await dvsa.search_centre_slots(centre)
                await post_event_api(client_httpx, row["id"], f"checked:{centre} ({len(slots)} slots)")
                return slots

        except CaptchaDetected:
            # mark health + global cooldown
            centre_fail(centre); global_cooldown(CAPTCHA_COOLDOWN_MIN)
            # WhatsApp with DVSA link (+ resume)
            send_captcha_alert(row, centre)
            # admin-side event
            await post_event_api(client_httpx, row["id"], f"captcha_cooldown:{centre}")
            # re-raise so caller can treat as hard stop for the pass
            raise

        except Exception as e:
            msg = _human(e).lower()
            layout_like = any(k in msg for k in [
                "timeout","not found","selector","locator","no node found for selector","failed to find",
            ])
            if layout_like:
                await post_event_api(client_httpx, row["id"], f"layout_issue:{centre}")
            if attempt >= attempts:
                print(f"[dvsa_check_centre] {centre}: {_human(e)} (gave up after {attempts})")
                centre_fail(centre)
                return []
            await asyncio.sleep(0.5 * attempt)

    return []

async def dvsa_swap_to_slot(client_unused, row, slot: str) -> bool:
    session_key = make_session_key(row)
    try:
        async with DVSAClient(headless=True, session_key=session_key) as dvsa:
            dvsa.attach_job(int(row["id"]))  # instrumentation
            await dvsa.login_swap(
                licence_number=row.get("licence_number") or "",
                booking_reference=row.get("booking_reference") or "",
                email=row.get("email") or None,
            )
            return await dvsa.swap_to(slot)
    except CaptchaDetected:
        send_captcha_alert(row, "(during swap confirm)")
        return False
    except Exception as e:
        print(f"[dvsa_swap_to_slot] {e}")
        return False

async def dvsa_book_and_pay(client_unused, row, slot: str) -> bool:
    if AUTOBOOK_MODE == "simulate":
        await asyncio.sleep(0.3 + random.random() * 0.4)
        return random.random() < AUTOBOOK_SIM_SUCCESS_RATE
    return False

# ==============================
# Assist window (15-min alert loop)
# ==============================
async def assist_window_monitor(sid: int, slot_txt: str):
    if not ASSIST_NOTIFY_ENABLED: return
    end_at = datetime.now(timezone.utc) + timedelta(minutes=ASSIST_NOTIFY_WINDOW_MIN)
    while datetime.now(timezone.utc) < end_at:
        status, _ = get_status_tuple_from_cache(sid)
        if status in ("booked","failed"): return
        secs_left_total = int((end_at - datetime.now(timezone.utc)).total_seconds())
        mins_left, secs_left = secs_left_total // 60, secs_left_total % 60
        wa_owner(
            f"⏳ FASTDTF: Slot still available? Search #{sid}\n"
            f"{slot_txt}\nTime left (~DVSA hold): {mins_left:02d}:{secs_left:02d}\n"
            f"If you’ve paid already, ignore this."
        )
        await asyncio.sleep(ASSIST_NOTIFY_PING_SECONDS)
    status, _ = get_status_tuple_from_cache(sid)
    if status not in ("booked","failed"):
        wa_owner(f"⌛ FASTDTF: Assist window ended for search #{sid}. If not booked, the slot may be gone.")

# ---------------- Per-identity mutual exclusion ----------------
LICENCE_LOCKS: dict[str, asyncio.Lock] = {}
def _lic_lock(key: str) -> asyncio.Lock:
    lock = LICENCE_LOCKS.get(key)
    if lock is None:
        lock = LICENCE_LOCKS[key] = asyncio.Lock()
    return lock

# ==============================
# Core Job Processing
# ==============================
async def process_job(client: httpx.AsyncClient, row: dict):
    # NEW: bail out early if globally paused (handles pre-claimed jobs)
    if CTRL.get("pause_all"):
        try:
            await post_event_api(client, row["id"], "paused:global")
            await set_status_api(client, row, "queued", "paused:global")
        except Exception:
            pass
        return

    sid = row["id"]

    # Mutex: new->licence, swap->booking reference
    session_key_for_lock = session_base(row).replace(" ", "")[:20]
    async with _lic_lock(session_key_for_lock):

        # stale guard only for active 'searching'
        status_now = (row.get("status") or "").strip()
        if status_now == "searching":
            updated_ts = row.get("updated_at") or row.get("created_at")
            if _is_stale(updated_ts, minutes=STALE_MINUTES):
                await set_status_api(client, row, "queued", f"stale_skipped:>{STALE_MINUTES}m")
                return

        # require credentials before doing DVSA checks
        missing_reason = _missing_required_fields(row)
        if missing_reason:
            await set_status_api(client, row, "queued", missing_reason)
            return

        centres: List[str] = normalize_centres(safe_json_loads(row.get("centres_json"), []))
        if not centres: centres = ["(no centre provided)"]

        # Priority centres first (prefix match), then the rest (shuffled per job)
        high, low = [], []
        prio = CURRENT_PRIORITY_CENTRES
        for c in centres:
            cname = c.lower()
            if any(cname.startswith(p) for p in prio):
                high.append(c)
            else:
                low.append(c)
        random.Random(sid).shuffle(low)

        found_slot: Optional[str] = None
        captcha_hit = False
        last_checked: Optional[str] = None

        async def check_one(c: str):
            nonlocal found_slot, captcha_hit, last_checked
            if CTRL.get("pause_all"):  # hard stop mid-pass if paused while running
                return
            if found_slot is not None or captcha_hit:
                return
            last_checked = c
            try:
                await post_event_api(client, row["id"], f"trying:{c}")
            except Exception:
                pass
            try:
                slots = await dvsa_check_centre(client, c, row)
            except CaptchaDetected:
                captcha_hit = True
                return
            if slots and found_slot is None:
                found_slot = slots[0]

        async def run_pass(items: List[str]):
            if not items or found_slot is not None or captcha_hit:
                return
            # If the client is doing RPS, force serial checks (one Playwright context at a time).
            effective_parallel = 1 if HONOUR_CLIENT_RPS else max(1, min(JOB_MAX_PARALLEL_CHECKS, len(items)))
            sem = asyncio.Semaphore(effective_parallel)
            async def worker(c):
                async with sem:
                    await check_one(c)
            await asyncio.gather(*(worker(c) for c in items))

        # 1) STRICT priority pass (e.g., Elgin first)
        await run_pass(high)
        # 2) Only if nothing found and no captcha, check the rest
        if not found_slot and not captcha_hit and not CTRL.get("pause_all"):
            await run_pass(low)

        if CTRL.get("pause_all"):
            await set_status_api(client, row, "queued", "paused:global")
            return

        if captcha_hit:
            await set_status_api(client, row, "queued", f"captcha_cooldown:{last_checked or ''}")
            return

        if not found_slot:
            await set_status_api(client, row, "queued", f"no_slots_this_round:{last_checked or ''}")
            return

        sig = f"{found_slot}"
        if get_last_slot_sig(sid) == sig:
            await set_status_api(client, row, "queued", f"slot_unchanged:{last_checked or ''}")
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

    async with httpx.AsyncClient(http2=False, limits=limits, timeout=timeout, headers=headers) as client:
        print(f"[worker] up state_db={DB_FILE} poll={POLL_SEC}s conc={CONCURRENCY} http2=off "
              f"client_rps={'ON' if HONOUR_CLIENT_RPS else 'OFF'} "
              f"worker_rps={('disabled' if HONOUR_CLIENT_RPS else DVSA_RPS)} "
              f"mode={AUTOBOOK_MODE} autobook={AUTOBOOK_ENABLED} stale={STALE_MINUTES}m api={API_BASE or '(unset!)'} "
              f"priority_env={PRIORITY_CENTRES or '-'}")
        while True:
            try:
                # Pull latest controls each loop (cheap GET)
                controls = await get_controls_api(client)
                if controls:
                    # update globals atomically
                    global CTRL, CURRENT_PRIORITY_CENTRES
                    CTRL = {"pause_all": bool(controls.get("pause_all", False)),
                            "priority_centres": [str(x).strip().lower()
                                                 for x in controls.get("priority_centres", [])
                                                 if str(x).strip()]}
                    if CTRL["priority_centres"]:
                        CURRENT_PRIORITY_CENTRES = CTRL["priority_centres"][:]  # live update

                if CTRL.get("pause_all"):
                    # soft idle while paused
                    await asyncio.sleep(POLL_SEC)
                    continue

                if is_quiet_now() or not global_allowed():
                    await asyncio.sleep(POLL_SEC); continue

                jobs = await claim_candidates_api(client, CONCURRENCY)
                if not jobs:
                    await asyncio.sleep(POLL_SEC); continue
                for j in jobs:
                    _status_cache[j["id"]] = (j.get("status") or "searching", j.get("last_event") or "")
                await asyncio.gather(*(process_job(client, j) for j in jobs))
            except Exception as e:
                print(f"[worker] error: {_human(e)}")
                await asyncio.sleep(POLL_SEC)

if __name__ == "__main__":
    asyncio.run(run())
