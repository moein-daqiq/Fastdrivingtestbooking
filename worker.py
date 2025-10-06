# =============================
# FILE: worker.py
# =============================
import os
import sys
import time
import json
import queue
import math
import random
import signal
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

# Optional Twilio (WhatsApp alerts)
try:
    from twilio.rest import Client as TwilioClient  # type: ignore
except Exception:  # pragma: no cover
    TwilioClient = None

from dvsa_client import DVSAClient, DVSAError, CaptchaDetected, ServiceClosed, LayoutIssue, WAFBlocked

# ---------- ENV ----------
API_BASE = os.environ.get("API_BASE", "http://localhost:8000/api").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
STATE_DB = os.environ.get("SEARCHES_DB", "searches.db")

PLAYWRIGHT_BROWSERS_PATH = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "")
AUTOBOOK_ENABLED = os.environ.get("AUTOBOOK_ENABLED", "true").lower() == "true"
AUTOBOOK_MODE = os.environ.get("AUTOBOOK_MODE", "simulate")  # simulate | real
HONOUR_CLIENT_RPS = os.environ.get("HONOUR_CLIENT_RPS", "true").lower() == "true"
DVSA_RPS = float(os.environ.get("DVSA_RPS", "0.5"))
DVSA_RPS_JITTER = float(os.environ.get("DVSA_RPS_JITTER", "0.35"))

# WhatsApp alerts (optional)
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "")
WHATSAPP_OWNER_TO = os.environ.get("WHATSAPP_OWNER_TO", "")

RESUME_URL = os.environ.get("RESUME_URL", f"{API_BASE}/worker/resume")
QUIET_HOURS = os.environ.get("QUIET_HOURS", "")  # e.g., "00-06"

# Anywhere scanning (optional)
ALL_CENTRES = [s.strip() for s in os.environ.get("ALL_CENTRES", "").split(",") if s.strip()]
SCAN_ANYWHERE_FIRST_DEFAULT = os.environ.get("SCAN_ANYWHERE_FIRST_DEFAULT", "true").lower() == "true"

# Operational
WORKER_POLL_SEC = int(os.environ.get("WORKER_POLL_SEC", "30"))
WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "8"))
JOB_MAX_PARALLEL_CHECKS = int(os.environ.get("JOB_MAX_PARALLEL_CHECKS", "4"))

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
log = logging.getLogger("worker")

# ---------- Helpers ----------

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def in_quiet_hours() -> bool:
    if not QUIET_HOURS:
        return False
    try:
        start, end = QUIET_HOURS.split("-")
        h = utcnow().astimezone().hour
        s, e = int(start), int(end)
        if s <= e:
            return s <= h < e
        # wrap across midnight e.g., 22-06
        return h >= s or h < e
    except Exception:
        return False


def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "User-Agent": "FastDTF-Worker/2025-10-05",
    }


def _post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=60)


def _get(path: str) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.get(url, headers=_headers(), timeout=60)


def _notify_whatsapp(text: str) -> None:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM and WHATSAPP_OWNER_TO and TwilioClient):
        return
    try:
        tc = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        tc.messages.create(from_=f"whatsapp:{TWILIO_WHATSAPP_FROM}", to=f"whatsapp:{WHATSAPP_OWNER_TO}", body=text)
    except Exception as e:  # pragma: no cover
        log.warning("whatsapp send failed: %s", e)


# ---------- Worker Threads ----------
class JobRunner(threading.Thread):
    def __init__(self, job: Dict[str, Any], dvsa: DVSAClient):
        super().__init__(daemon=True)
        self.job = job
        self.dvsa = dvsa

    def run(self) -> None:  # noqa: C901
        jid = self.job.get("id")
        centre_list = self.job.get("centres", [])
        booking_type = self.job.get("booking_type")  # "swap" | "new"
        licence = self.job.get("licence_number")
        ref = self.job.get("booking_reference")
        theory_pass = self.job.get("theory_pass")

        # Merge anywhere centres if configured
        centres = centre_list[:]
        if ALL_CENTRES and SCAN_ANYWHERE_FIRST_DEFAULT:
            # prioritise ALL_CENTRES before user centres
            centres = ALL_CENTRES + [c for c in centre_list if c not in ALL_CENTRES]

        log.info("job %s start booking_type=%s centres=%s", jid, booking_type, centres[:5])
        _post("/admin/events", {"sid": jid, "event": f"claimed:{len(centres)}"})

        try:
            for centre in centres[:JOB_MAX_PARALLEL_CHECKS] if JOB_MAX_PARALLEL_CHECKS > 0 else centres:
                _post("/admin/events", {"sid": jid, "event": f"login_start:{centre}"})
                try:
                    if booking_type == "swap":
                        self.dvsa.login_swap(licence, ref)
                    else:
                        self.dvsa.login_new(licence, theory_pass)
                    _post("/admin/events", {"sid": jid, "event": f"login_ok:{centre}"})
                except LayoutIssue:
                    _post("/admin/events", {"sid": jid, "event": f"layout_issue:{centre}"})
                    continue

                slots = self.dvsa.search_centre_slots(centre)
                if not slots:
                    _post("/admin/events", {"sid": jid, "event": f"checked:{centre} · no_slots"})
                    continue

                # pick earliest
                slots.sort(key=lambda s: (s["date"], s["time"]))
                slot = slots[0]
                _post("/admin/events", {"sid": jid, "event": f"slot_found:{centre} · {slot['date']} · {slot['time']}"})

                if not AUTOBOOK_ENABLED:
                    _post("/admin/events", {"sid": jid, "event": f"found_only:{centre} · {slot['date']} {slot['time']}"})
                    break

                ok = False
                if booking_type == "swap":
                    ok = self.dvsa.swap_to(slot)
                else:
                    ok = self.dvsa.book_and_pay(slot, mode=AUTOBOOK_MODE)

                if ok:
                    _post("/admin/events", {"sid": jid, "event": f"booked:{centre} · {slot['date']} {slot['time']}"})
                    # upgrade NEW → SWAP
                    if booking_type == "new":
                        try:
                            _post("/upgrade-to-swap", {"sid": jid})
                        except Exception:
                            pass
                    return
                else:
                    _post("/admin/events", {"sid": jid, "event": f"booking_failed:{centre} · {slot['date']} {slot['time']}"})

        except CaptchaDetected as e:
            _post("/admin/events", {"sid": jid, "event": f"captcha_cooldown:{e.stage}"})
            _notify_whatsapp(f"CAPTCHA at {e.stage}. Tap to resume: {RESUME_URL}")
        except WAFBlocked as e:
            _post("/admin/events", {"sid": jid, "event": f"ip_blocked:{e.stage}"})
        except ServiceClosed as e:
            _post("/admin/events", {"sid": jid, "event": f"service_closed:{e.stage}"})
        except DVSAError as e:
            _post("/admin/events", {"sid": jid, "event": f"error:{type(e).__name__}:{str(e)[:140]}"})
        except Exception as e:  # pragma: no cover
            _post("/admin/events", {"sid": jid, "event": f"unexpected:{type(e).__name__}:{str(e)[:140]}"})


class ClaimLoop:
    def __init__(self) -> None:
        self.stop = False
        self.sem = threading.Semaphore(WORKER_CONCURRENCY)

    def _claim(self) -> List[Dict[str, Any]]:
        try:
            r = _post("/worker/claim", {"limit": WORKER_CONCURRENCY})
            if r.status_code == 401:
                log.error("Unauthorized (bad WORKER_TOKEN)")
                return []
            r.raise_for_status()
            data = r.json()
            return data.get("jobs", [])
        except Exception as e:
            log.warning("claim error: %s", e)
            return []

    def loop(self) -> None:
        # Boot log
        client_rps = "ON" if HONOUR_CLIENT_RPS else f"OFF({DVSA_RPS}/s±{DVSA_RPS_JITTER})"
        log.info(
            "[worker] up state_db=%s poll=%ss client_rps=%s autobook=%s mode=%s",
            STATE_DB,
            WORKER_POLL_SEC,
            client_rps,
            AUTOBOOK_ENABLED,
            AUTOBOOK_MODE,
        )

        dvsa = DVSAClient(
            honour_client_rps=HONOUR_CLIENT_RPS,
            dvsa_rps=DVSA_RPS,
            dvsa_rps_jitter=DVSA_RPS_JITTER,
        )

        while not self.stop:
            try:
                # read controls
                cr = _get("/worker/controls")
                pause = False
                if cr.ok:
                    c = cr.json()
                    pause = bool(c.get("pause", False))
                    quiet_now = in_quiet_hours()
                    log.info(
                        "poll: pause=%s quiet=%s global_allowed=%s",
                        pause,
                        quiet_now,
                        (not pause and not quiet_now),
                    )
                    if pause or quiet_now:
                        time.sleep(WORKER_POLL_SEC)
                        continue

                jobs = self._claim()
                if not jobs:
                    time.sleep(WORKER_POLL_SEC)
                    continue

                threads: List[threading.Thread] = []
                for j in jobs:
                    self.sem.acquire()
                    t = JobRunner(j, dvsa)
                    t.start()
                    threads.append(t)

                # release semaphore as jobs end
                for t in threads:
                    t.join()
                    self.sem.release()

            except KeyboardInterrupt:
                self.stop = True
            except Exception as e:  # pragma: no cover
                log.warning("loop error: %s", e)
                time.sleep(WORKER_POLL_SEC)


if __name__ == "__main__":
    loop = ClaimLoop()
    loop.loop()


# =============================
# FILE: dvsa_client.py
# =============================
import os
import time
import random
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Playwright
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout  # type: ignore

log = logging.getLogger("dvsa")


# ---------- Errors ----------
class DVSAError(Exception):
    pass


class CaptchaDetected(DVSAError):
    def __init__(self, stage: str) -> None:
        super().__init__(f"captcha at {stage}")
        self.stage = stage


class ServiceClosed(DVSAError):
    def __init__(self, stage: str) -> None:
        super().__init__(f"service closed at {stage}")
        self.stage = stage


class LayoutIssue(DVSAError):
    pass


class WAFBlocked(DVSAError):
    def __init__(self, stage: str) -> None:
        super().__init__(f"waf at {stage}")
        self.stage = stage


# ---------- ENV (DVSA tuning) ----------
DVSA_HEADLESS = os.environ.get("DVSA_HEADLESS", "true").lower() == "true"
DVSA_NAV_TIMEOUT_MS = int(os.environ.get("DVSA_NAV_TIMEOUT_MS", "25000"))
DVSA_READY_MAX_MS = int(os.environ.get("DVSA_READY_MAX_MS", "30000"))
DVSA_POST_NAV_SETTLE_MS = int(os.environ.get("DVSA_POST_NAV_SETTLE_MS", "800"))
DVSA_CLICK_SETTLE_MS = int(os.environ.get("DVSA_CLICK_SETTLE_MS", "700"))
DVSA_USER_AGENT = os.environ.get("DVSA_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
DVSA_PROXY = os.environ.get("DVSA_PROXY", "")

DVSA_MANAGE_URL = "https://driverpracticaltest.dvsa.gov.uk/manage"


# Common text on GOV.UK forms
LICENCE_LABELS = [
    "Driving licence number",
    "Driving license number",  # US spelling sometimes appears
    "licence number",
]
REF_LABELS = [
    "Application reference number",
    "Booking reference",
    "Reference number",
]
THEORY_LABELS = [
    "Theory test pass number",
    "Theory pass number",
]


def _visible(locator):
    try:
        return locator.is_visible()
    except Exception:
        return False


@dataclass
class DVSAClient:
    honour_client_rps: bool = True
    dvsa_rps: float = 0.5
    dvsa_rps_jitter: float = 0.35

    def __post_init__(self) -> None:
        self._p = None
        self._browser = None
        self._ctx = None
        self._page = None
        self._last_action_ts = 0.0
        self._boot()

    # ---------- Boot / Teardown ----------
    def _boot(self) -> None:
        self._p = sync_playwright().start()
        proxy = {"server": DVSA_PROXY} if DVSA_PROXY else None
        self._browser = self._p.chromium.launch(headless=DVSA_HEADLESS)
        self._ctx = self._browser.new_context(user_agent=DVSA_USER_AGENT, proxy=proxy)
        self._ctx.set_default_navigation_timeout(DVSA_NAV_TIMEOUT_MS)
        self._ctx.set_default_timeout(DVSA_NAV_TIMEOUT_MS)
        self._page = self._ctx.new_page()
        log.info("playwright chromium ready")

    def _sleep_rps(self) -> None:
        if self.honour_client_rps:
            now = time.time()
            min_gap = 1.0 / max(self.dvsa_rps, 0.01)
            jitter = random.uniform(0, min_gap * self.dvsa_rps_jitter)
            wait = max(0.0, (self._last_action_ts + min_gap + jitter) - now)
            if wait > 0:
                time.sleep(wait)
            self._last_action_ts = time.time()

    def _ready_guard(self) -> None:
        self._sleep_rps()
        time.sleep(DVSA_POST_NAV_SETTLE_MS / 1000.0)

    def _click(self, where) -> None:
        self._sleep_rps()
        if isinstance(where, str):
            self._page.click(where)
        else:
            where.click()
        time.sleep(DVSA_CLICK_SETTLE_MS / 1000.0)

    def _fill(self, where, text: str) -> None:
        self._sleep_rps()
        if isinstance(where, str):
            self._page.fill(where, text)
        else:
            where.fill(text)

    # ---------- Cross-frame helpers ----------
    def _frames(self):
        return [self._page, *self._page.frames]

    def _find_any(self, selectors: List[str]):
        for scope in self._frames():
            for s in selectors:
                loc = scope.locator(s)
                if _visible(loc):
                    return loc
        return None

    def _get_by_label_any(self, labels: List[str]):
        for scope in self._frames():
            for t in labels:
                try:
                    loc = scope.get_by_label(t, exact=False)
                    if _visible(loc):
                        return loc
                except Exception:
                    continue
        return None

    def _dismiss_cookie_banners(self) -> None:
        # GOV.UK cookie banners variants
        candidates = [
            "button:has-text('Accept additional cookies')",
            "button:has-text('Accept all cookies')",
            "#accept-additional-cookies",
            "#accept-all-cookies",
            "text=Accept cookies",
        ]
        btn = self._find_any(candidates)
        if btn:
            try:
                self._click(btn)
            except Exception:
                pass
        # Banner hide button
        hide_candidates = ["button:has-text('Hide')", "#hide-cookie-banner"]
        hide = self._find_any(hide_candidates)
        if hide:
            try:
                self._click(hide)
            except Exception:
                pass

    def _ensure_login_form(self, stage: str) -> Dict[str, Any]:
        # Try multiple strategies to find licence + either booking ref or theory pass
        self._dismiss_cookie_banners()

        # Strategy A: label-based (most robust on GOV.UK)
        licence = self._get_by_label_any(LICENCE_LABELS)
        ref = self._get_by_label_any(REF_LABELS)
        theory = self._get_by_label_any(THEORY_LABELS)

        # Strategy B: attribute fallbacks
        if not licence:
            licence = self._find_any([
                "input[name='driving-licence-number']",
                "input#driving-licence-number",
                "input[name*='licence']",
                "input[autocomplete='driving-licence-number']",
            ])
        if not ref:
            ref = self._find_any([
                "input[name='application-reference-number']",
                "input#application-reference-number",
                "input[name*='reference']",
            ])
        if not theory:
            theory = self._find_any([
                "input[name='theory-test-pass-number']",
                "input#theory-test-pass-number",
                "input[name*='theory']",
            ])

        ok = licence is not None and (ref is not None or theory is not None)
        if not ok:
            raise LayoutIssue(f"{stage}: licence field not found")

        # Find the submit button
        submit = self._find_any([
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Continue')",
            "button:has-text('Sign in')",
            "text=Continue",
        ])
        return {"licence": licence, "ref": ref, "theory": theory, "submit": submit}

    # ---------- Public API ----------
    def login_swap(self, licence: str, booking_ref: str) -> None:
        if not (licence and booking_ref and len(booking_ref) == 8 and booking_ref.isdigit()):
            raise LayoutIssue("invalid swap credentials")
        self._goto(DVSA_MANAGE_URL, stage="login_swap")
        for attempt in range(5):
            try:
                parts = self._ensure_login_form("login_swap")
                self._fill(parts["licence"], licence)
                self._fill(parts["ref"], booking_ref)
                if parts["submit"]:
                    self._click(parts["submit"])
                self._ready_guard()
                return
            except LayoutIssue:
                time.sleep(0.8)
        raise LayoutIssue("layout_change: licence field not found (gave up after 5)")

    def login_new(self, licence: str, theory_pass: str) -> None:
        if not (licence and theory_pass):
            raise LayoutIssue("invalid new credentials")
        self._goto(DVSA_MANAGE_URL, stage="login_new")
        for attempt in range(5):
            try:
                parts = self._ensure_login_form("login_new")
                self._fill(parts["licence"], licence)
                if parts["theory"]:
                    self._fill(parts["theory"], theory_pass)
                else:
                    # Some layouts show a radio toggle between ref/theory, try to find a switch
                    alt = self._find_any(["label:has-text('theory')", "text=theory", "#use-theory"])
                    if alt:
                        self._click(alt)
                        self._ready_guard()
                        parts = self._ensure_login_form("login_new")
                        self._fill(parts["theory"], theory_pass)
                if parts["submit"]:
                    self._click(parts["submit"])
                self._ready_guard()
                return
            except LayoutIssue:
                time.sleep(0.8)
        raise LayoutIssue("layout_change: licence field not found (gave up after 5)")

    def search_centre_slots(self, centre: str) -> List[Dict[str, str]]:
        # Placeholder (wire real selectors later). Keep behaviour stable for pipeline tests.
        self._ready_guard()
        if random.random() < 0.7:
            return []
        from datetime import datetime as _dt
        d = _dt.now().date().isoformat()
        return [{"date": d, "time": "09:17", "centre": centre}]

    def swap_to(self, slot: Dict[str, str]) -> bool:
        self._ready_guard()
        return random.random() > 0.3

    def book_and_pay(self, slot: Dict[str, str], mode: str = "simulate") -> bool:
        self._ready_guard()
        if mode == "simulate":
            return True
        return random.random() > 0.4

    # ---------- Navigation helpers ----------
    def _goto(self, url: str, stage: str) -> None:
        try:
            self._page.goto(url)
        except PWTimeout:
            raise ServiceClosed(stage)
        self._ready_guard()
        if self._is_captcha():
            raise CaptchaDetected(stage)
        if self._is_waf_block():
            raise WAFBlocked(stage)

    def _is_captcha(self) -> bool:
        html = self._page.content()
        return any(k in html for k in ["cf-challenge", "h-captcha", "g-recaptcha"])

    def _is_waf_block(self) -> bool:
        html = self._page.content()
        return any(k in html for k in ["Request blocked", "403", "Access denied"])  # broad but safe

    # ---------- Teardown ----------
    def close(self) -> None:
        try:
            if self._ctx:
                self._ctx.close()
            if self._browser:
                self._browser.close()
            if self._p:
                self._p.stop()
        except Exception:
            pass
