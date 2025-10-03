# dvsa_client.py
"""
Minimal, SAFE DVSA automation for FastDTF (robust readiness gates + built-in RPS throttle):
- No CAPTCHA bypass: raises CaptchaDetected if a bot wall appears.
- Public API used by worker: login_swap, login_new, search_centre_slots, swap_to, book_and_pay.
- All selectors centralized in SEL with fallbacks.
- Optional session reuse (per licence) and optional proxy support.
- Wait-until-ready guards so we never click while DVSA says "please wait".
- Internal DVSA_RPS throttle with jitter to avoid machine-perfect cadence.

Instrumentation (optional; only active when sid is attached AND API creds present):
- Posts admin events/status to your backend.
  Events:
    checked:<centre> · no_slots|<count>
    captcha_cooldown:<centre|stage>
    slot_found:<centre> · <date> · <time>        (when swap_to() is called)
    booked:<centre> · <date> · <time>
    booking_failed:<centre> · <date> · <time>
    error:<centre> · <ExceptionName>

Env you may set on the worker host (optional):
    API_BASE=https://api.fastdrivingtestfinder.co.uk/api
    WORKER_TOKEN=<same secret as backend>

    # Behavioural tuning (optional; sensible defaults below)
    DVSA_HEADLESS=true
    DVSA_NAV_TIMEOUT_MS=25000
    DVSA_SLOWMO_MS=0
    DVSA_USER_AGENT=<string>
    DVSA_PROXY=http://user:pass@host:port
    DVSA_READY_MAX_MS=30000
    DVSA_POST_NAV_SETTLE_MS=800
    DVSA_CLICK_SETTLE_MS=700

    # Internal throttle
    DVSA_RPS=0.5
    DVSA_RPS_JITTER=0.35
"""

from __future__ import annotations

import os
import re
import time
import asyncio
import random
from typing import List, Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PWTimeout,
)

# --- Try to import aiohttp for non-blocking API posts (optional). Fallback to no-op. ---
try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # we'll no-op if unavailable


# ---------------- Exceptions ----------------
class DVSAError(Exception):
    pass


class CaptchaDetected(DVSAError):
    """Raised when a CAPTCHA/anti-bot wall is detected; carries the page URL."""
    def __init__(self, message: str = "Captcha/anti-bot detected", url: Optional[str] = None):
        super().__init__(message)
        self.url = url or ""


# ---------------- Config (override via env if needed) ----------------
URL_CHANGE_TEST = os.environ.get("DVSA_URL_CHANGE", "https://driverpracticaltest.dvsa.gov.uk/manage")
URL_BOOK_TEST   = os.environ.get("DVSA_URL_BOOK",   "https://driverpracticaltest.dvsa.gov.uk/application")

HEADLESS = os.environ.get("DVSA_HEADLESS", "true").lower() == "true"
NAV_TIMEOUT_MS = int(os.environ.get("DVSA_NAV_TIMEOUT_MS", "25000"))
SLOWMO_MS = int(os.environ.get("DVSA_SLOWMO_MS", "0"))
USER_AGENT = os.environ.get(
    "DVSA_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DVSA_PROXY = os.environ.get("DVSA_PROXY")  # e.g. http://user:pass@host:port

READY_MAX_MS = int(os.environ.get("DVSA_READY_MAX_MS", "30000"))
POST_NAV_SETTLE_MS = int(os.environ.get("DVSA_POST_NAV_SETTLE_MS", "800"))
CLICK_SETTLE_MS = int(os.environ.get("DVSA_CLICK_SETTLE_MS", "700"))

# Backend instrumentation config (optional)
API_BASE = os.environ.get("API_BASE", "https://api.fastdrivingtestfinder.co.uk/api").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

# Internal RPS throttle (with jitter)
def _safe_float(env_val: str, default: float) -> float:
    try:
        return float(env_val)
    except Exception:
        return default

_DVSA_RPS = _safe_float(os.environ.get("DVSA_RPS", "0.5"), 0.5)          # default 0.5 rps
_DVSA_RPS = max(0.2, min(2.0, _DVSA_RPS))                                 # clamp 0.2..2.0
_DVSA_RPS_JITTER = _safe_float(os.environ.get("DVSA_RPS_JITTER", "0.35"), 0.35)
_DVSA_RPS_JITTER = max(0.0, min(0.9, _DVSA_RPS_JITTER))                   # clamp 0..0.9
_DVSA_MIN_INTERVAL = 1.0 / _DVSA_RPS                                      # base seconds between requests

# Storage for session reuse
SESS_DIR = os.environ.get("DVSA_SESS_DIR", "/tmp/dvsa_sessions")
os.makedirs(SESS_DIR, exist_ok=True)

# ---------------- Robust selectors ----------------
SEL = {
    # GOV.UK landing “Start now”
    "start_now": (
        "a.govuk-button--start, "
        "a.govuk-button[href*='start'], "
        "a.button--start, "
        "a[href*='start now' i], "
        "a[class*='start'][class*='button']"
    ),

    # NEW booking – category page (pick Car)
    "car_button": "button:has-text('Car'), a:has-text('Car (manual'), .app-button:has-text('Car'), [role='button']:has-text('Car')",

    # Existing booking (swap) login (CSS fallbacks)
    "swap_licence":  "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "swap_ref":      "input[name='booking-reference'], input[name*='reference'], input[id*='reference'], input[name='bookingReference']",
    "swap_email":    "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "swap_continue": "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # New booking login (CSS fallbacks)
    "new_licence":   "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "new_theory":    "input[name='theory-pass-number'], input[name='theoryPassNumber'], input[name*='theory'], input[id*='theory']",
    "new_email":     "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "new_continue":  "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # Centre search & availability
    "centre_search_box":  "input[name*='test-centre'], input[id*='test-centre'], input[name*='centre'], input[name='testCentreSearch']",
    "centre_suggestions": "ul[role='listbox'] li, li[role='option'], ul li a",
    "centre_dates":       "[data-test='available-dates'] li, ul.available-dates li, .dates li, .available-dates li, a:has-text('view')",
    "centre_times":       "[data-test='available-times'] li, ul.available-times li, .times li, .available-times li, .slot, a:has-text(':')",

    # Confirm
    "confirm_button": "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # Errors
    "error_summary": ".govuk-error-summary, [role='alert']",
}

LABELS = {
    "licence": [
        "Driving licence number",
        "Driver number",
        "Licence number",
        "Driving licence number (as it appears on your licence)",
    ],
    "swap_ref": [
        "Application reference number",
        "Booking reference",
        "Reference number",
    ],
    "email": [
        "Email",
        "Email address",
        "Candidate email",
    ],
}
ROLE_BUTTONS = {
    "continue": ["Continue", "Sign in", "Next", "Find appointments", "Search"],
}

def _sel(key: str) -> str:
    s = SEL.get(key)
    if not s:
        raise RuntimeError(f"Selector missing: {key}")
    return s


# ---------------- Client ----------------
class DVSAClient:
    def __init__(self, headless: bool = HEADLESS, session_key: Optional[str] = None):
        self.headless = headless
        self.session_key = session_key
        self.pw = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self._storage_path: Optional[str] = None

        # instrumentation
        self.sid: Optional[int] = None  # attach_job() sets this
        self._http: Optional["aiohttp.ClientSession"] = None

        # small breadcrumb for better captcha events
        self._stage: str = "init"

        # internal RPS state
        self._last_rps_ts: Optional[float] = None
        self._rps_interval: float = _DVSA_MIN_INTERVAL
        self._rps_jitter: float = _DVSA_RPS_JITTER

    # ---------- context manager ----------
    async def __aenter__(self) -> "DVSAClient":
        self.pw = await async_playwright().start()
        launch_args = {
            "headless": self.headless,
            "slow_mo": SLOWMO_MS,
            "args": [],
        }
        if DVSA_PROXY:
            launch_args["proxy"] = {"server": DVSA_PROXY}

        self.browser = await self.pw.chromium.launch(**launch_args)

        # Persist storage per identity (keeps cookies/session)
        self._storage_path = None
        if self.session_key:
            self._storage_path = os.path.join(SESS_DIR, f"{re.sub(r'[^A-Za-z0-9_-]+','', self.session_key)}.json")
            # Create empty file if not present to avoid first-run errors
            if not os.path.exists(self._storage_path):
                with open(self._storage_path, "w", encoding="utf-8") as f:
                    f.write("{}")

        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
            storage_state=self._storage_path if self._storage_path and os.path.exists(self._storage_path) else None,
        )
        self.page = await self.context.new_page()
        self.page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            # Save storage for reuse
            if self._storage_path and self.context:
                try:
                    await self.context.storage_state(path=self._storage_path)
                except Exception:
                    pass
        finally:
            try:
                if self.page:
                    await self.page.close()
            except Exception:
                pass
            try:
                if self.context:
                    await self.context.close()
            except Exception:
                pass
            try:
                if self.browser:
                    await self.browser.close()
            except Exception:
                pass
            try:
                await self._close_http()
            except Exception:
                pass
            try:
                if self.pw:
                    await self.pw.stop()
            except Exception:
                pass

    # ------------- Instrumentation helpers (optional) -------------
    def attach_job(self, sid: int):
        """Attach a backend search id for event/status posting."""
        self.sid = int(sid)

    async def _ensure_http(self):
        if self._http is None and aiohttp and WORKER_TOKEN:
            self._http = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {WORKER_TOKEN}", "Content-Type": "application/json"}
            )

    async def _post_json(self, url: str, payload: dict):
        if not (self.sid and aiohttp and WORKER_TOKEN and API_BASE):
            return  # safely no-op
        try:
            await self._ensure_http()
            assert self._http is not None
            async with self._http.post(url, json=payload, timeout=15) as _:
                pass
        except Exception:
            # Instrumentation must never break core flow
            pass

    async def _event(self, text: str):
        if not self.sid: return
        await self._post_json(f"{API_BASE}/worker/searches/{self.sid}/event", {"event": text})

    async def _status(self, status: str, event: str):
        if not self.sid: return
        await self._post_json(f"{API_BASE}/worker/searches/{self.sid}/status", {"status": status, "event": event})

    async def _close_http(self):
        try:
            if self._http:
                await self._http.close()
        except Exception:
            pass
        finally:
            self._http = None

    # ------------- RPS throttle -------------
    async def _rps_pause(self, where: str = ""):
        """
        Ensure at most ~1 request / interval with ±jitter.
        Called after actions that likely hit the network (nav, form submit, click).
        """
        # Determine randomized target interval for this step
        factor = 1.0
        if self._rps_jitter > 0.0:
            # pick in [1 - j, 1 + j]
            low = max(0.1, 1.0 - self._rps_jitter)
            high = 1.0 + self._rps_jitter
            factor = random.uniform(low, high)
        interval = self._rps_interval * factor

        now = time.monotonic()
        if self._last_rps_ts is None:
            self._last_rps_ts = now
            return

        elapsed = now - self._last_rps_ts
        remain = interval - elapsed
        if remain > 0:
            try:
                await asyncio.sleep(remain)
            finally:
                self._last_rps_ts = time.monotonic()
        else:
            self._last_rps_ts = now

    # ------------- Timing helpers (human-like settle) -------------
    async def _human_pause(self, max_ms: int):
        """Small randomized settle to avoid machine-perfect cadence."""
        if max_ms <= 0:
            return
        await asyncio.sleep(random.uniform(0.25, max_ms / 1000.0))

    async def _wait_for_dvsa_ready(self) -> bool:
        """
        Gate interactions until DVSA has finished its initial "please wait" loading,
        hCaptcha (if any) has mounted, and a known interactive element is visible.
        """
        await self._captcha_guard("ready_gate_pre")

        # If DVSA shows a loading banner, wait for it to disappear
        try:
            banner = self.page.locator("text=/please\\s+wait|loading\\s+the\\s+page/i")
            if await banner.count() > 0:
                await banner.first.wait_for(state="detached", timeout=READY_MAX_MS)
        except Exception:
            pass  # continue with other readiness checks

        # If hCaptcha iframe appears, give it a moment to settle
        try:
            await self.page.wait_for_selector('iframe[src*="hcaptcha.com"]', timeout=7000)
            await self._human_pause(700)
        except Exception:
            pass

        # Look for elements that signal the page is actually usable
        candidates = [
            'form button[type="submit"]',
            'a[href*="start"]',
            'input[name="booking-reference"]',
            'input[name="email"]',
            'input[name*="licence"], input[id*="licence"]',
        ]
        for sel in candidates:
            try:
                await self.page.wait_for_selector(sel, state="visible", timeout=READY_MAX_MS)
                await self._human_pause(650)
                return True
            except Exception:
                continue

        # As a last resort: near-idle network
        try:
            await self.page.wait_for_load_state("networkidle", timeout=15000)
            await self._human_pause(600)
            return True
        except Exception:
            return False

    # ------------- Low-level helpers -------------
    async def _goto(self, url: str, marker: Optional[str] = None, stage: str = "nav"):
        self._stage = stage
        await self.page.goto(url, wait_until="domcontentloaded")
        await self._rps_pause("after_goto")
        await self._accept_cookies()
        await self._captcha_guard()
        if marker:
            await self.page.get_by_text(marker, exact=False).first.wait_for()
        await self._human_pause(POST_NAV_SETTLE_MS)
        await self._wait_for_dvsa_ready()

    async def _fill_css(self, sel: str, value: str):
        await self.page.wait_for_selector(sel)
        await self.page.fill(sel, value)
        await self._human_pause(450)

    async def _click_css(self, sel: str):
        await self.page.wait_for_selector(sel)
        try:
            await self.page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        await self.page.click(sel)
        await self._human_pause(CLICK_SETTLE_MS)
        await self._rps_pause("after_click_css")

    async def _click_continue_role_fallback(self) -> bool:
        # Prefer accessible role/name to resist copy changes
        for name in ROLE_BUTTONS["continue"]:
            try:
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=8000)
                except Exception:
                    pass
                await self.page.get_by_role("button", name=name).click(timeout=4000)
                await self._human_pause(CLICK_SETTLE_MS)
                await self._rps_pause("after_click_continue_role")
                return True
            except Exception:
                continue
        # CSS fallback
        for sel in (_sel("new_continue"), _sel("swap_continue")):
            try:
                await self._click_css(sel)
                return True
            except Exception:
                continue
        # very last-chance: any primary govuk button
        try:
            await self.page.locator("button.govuk-button").first.click(timeout=3000)
            await self._human_pause(CLICK_SETTLE_MS)
            await self._rps_pause("after_click_continue_govuk")
            return True
        except Exception:
            return False

    async def _fill_by_label_variants(self, names: list[str], value: str) -> bool:
        """
        Try (1) label-based; (2) ARIA/placeholder; (3) nearest-input to text node.
        This covers DVSA pages that use legends, fieldsets or custom markup.
        """
        # 1) Labels
        for n in names:
            try:
                await self.page.get_by_label(n, exact=False).fill(value, timeout=3500)
                await self._human_pause(400)
                return True
            except Exception:
                continue

        # 2) Common placeholders/aria-labels
        for ph in [n for n in names] + ["Driver number", "Licence", "Booking reference", "Reference"]:
            try:
                await self.page.get_by_placeholder(ph, exact=False).fill(value, timeout=2500)
                await self._human_pause(350)
                return True
            except Exception:
                continue
            try:
                await self.page.locator(f"input[aria-label*='{ph}'], input[aria-labelledby*='{ph}']").first.fill(value, timeout=2500)
                await self._human_pause(350)
                return True
            except Exception:
                continue

        # 3) Nearest input after visible text node
        for n in names:
            try:
                lab = self.page.get_by_text(n, exact=False).first
                await lab.wait_for(timeout=1500)
                container = lab.locator("xpath=ancestor-or-self::*[self::label or self::div or self::fieldset][1]")
                await container.locator("input").first.fill(value, timeout=2500)
                await self._human_pause(350)
                return True
            except Exception:
                continue

        return False

    async def _accept_cookies(self):
        """Dismiss DVSA cookie banners if present (quietly)."""
        for name in ["Accept all cookies", "Accept analytics cookies", "Accept cookies", "I agree"]:
            try:
                await self.page.get_by_role("button", name=name).click(timeout=1500)
                await self._human_pause(300)
                await self._rps_pause("after_accept_cookies")
                break
            except Exception:
                pass

    async def _captcha_guard(self, where: Optional[str] = None):
        """
        Inspect current DOM for anti-bot widgets/messages.
        If found: emit admin event (when sid is attached), save a screenshot, raise CaptchaDetected(url=...).
        """
        html = (await self.page.content()).lower()
        indicators = [
            "recaptcha", "hcaptcha", "turnstile",  # common widgets
            "not a robot", "unusual traffic", "additional security check", "enter the characters", "are you human",
            "verify you are human", "security challenge", "captcha"
        ]
        if any(k in html for k in indicators):
            # best-effort screenshot
            try:
                ts = str(int(asyncio.get_event_loop().time()))
                await self.page.screenshot(path=f"/tmp/captcha_{ts}.png", full_page=True)
            except Exception:
                pass
            # post event immediately if we know the job
            stage = where or self._stage or "unknown"
            try:
                await self._event(f"captcha_cooldown:{stage}")
            except Exception:
                pass
            # raise with URL for the worker to include in WhatsApp (if desired)
            raise CaptchaDetected(url=(self.page.url if self.page else ""))

    async def _expect_ok(self):
        await self._captcha_guard()
        err = await self.page.query_selector(_sel("error_summary"))
        if err:
            msg = (await err.text_content() or "").strip()
            raise DVSAError(f"DVSA error: {msg}")

    async def _maybe_click_start_now(self):
        """
        If we land on the GOV.UK explainer, click Start now to reach the form.
        Tries role-based (accessible) lookup first, then CSS, then loose text.
        """
        try:
            # Role/name first (more resilient to markup tweaks)
            for name in ["Start now", "Start", "Begin", "Continue"]:
                try:
                    await self.page.get_by_role("link", name=re.compile(name, re.I)).click(timeout=2000)
                    await self.page.wait_for_load_state("domcontentloaded", timeout=6000)
                    await self._captcha_guard("start_now")
                    await self._human_pause(POST_NAV_SETTLE_MS)
                    await self._rps_pause("after_start_now_role")
                    await self._wait_for_dvsa_ready()
                    return
                except Exception:
                    pass

            # CSS fallbacks
            try:
                el = await self.page.query_selector(_sel("start_now"))
                if el:
                    await el.click()
                    await self.page.wait_for_load_state("domcontentloaded", timeout=6000)
                    await self._captcha_guard("start_now_css")
                    await self._human_pause(POST_NAV_SETTLE_MS)
                    await self._rps_pause("after_start_now_css")
                    await self._wait_for_dvsa_ready()
                    return
            except Exception:
                pass

            # Loose text search
            try:
                el = await self.page.get_by_text(re.compile(r"\bstart now\b", re.I)).first
                await el.click(timeout=1500)
                await self.page.wait_for_load_state("domcontentloaded", timeout=6000)
                await self._captcha_guard("start_now_text")
                await self._human_pause(POST_NAV_SETTLE_MS)
                await self._rps_pause("after_start_now_text")
                await self._wait_for_dvsa_ready()
            except Exception:
                pass
        except Exception:
            pass  # never fail flow just on this

    async def _select_car_category_if_present(self):
        try:
            btn = await self.page.query_selector(_sel("car_button"))
            if btn:
                await btn.click()
                await self._captcha_guard("category_car")
                await self._human_pause(CLICK_SETTLE_MS)
                await self._rps_pause("after_car_category")
        except PWTimeout:
            pass
        except Exception:
            pass

    async def _answer_no_no_if_present(self):
        """
        On the licence details page, DVSA asks:
          - Have you been ordered by a court to take an extended test?
          - Do you have any special requirements?
        We answer No/No when those controls exist.
        """
        try:
            no_radios = self.page.get_by_label("No", exact=True)
            await no_radios.nth(0).check(timeout=2000)
            await asyncio.sleep(0.05)
            await no_radios.nth(1).check(timeout=2000)
            await self._human_pause(350)
        except Exception:
            try:
                radios = await self.page.query_selector_all("input[type='radio'][value='no'], input[type='radio'][aria-label='No']")
                for r in radios[:2]:
                    try:
                        await r.check()
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
                await self._human_pause(300)
            except Exception:
                pass

    async def _licence_form_present(self) -> bool:
        """Return True if any known licence input is present (swap/new)."""
        css = (
            "input[name='driving-licence-number'], "
            "input[name*='licence'], input[id*='licence'], "
            "input[name='driverLicenceNumber']"
        )
        try:
            el = await self.page.query_selector(css)
            return el is not None
        except Exception:
            return False

    # ------------- Public API used by worker -------------
    async def login_swap(self, licence_number: str, booking_reference: str, email: Optional[str] = None):
        self._stage = "login_swap_nav"
        await self._goto(URL_CHANGE_TEST, stage="login_swap_nav")
        await self._maybe_click_start_now()
        if not await self._licence_form_present():
            await asyncio.sleep(0.3)
            await self._maybe_click_start_now()
        await self._wait_for_dvsa_ready()

        # Fill licence
        self._stage = "login_swap_licence"
        ok_lic = await self._fill_by_label_variants(LABELS["licence"], licence_number)
        if not ok_lic:
            try:
                await self._fill_css(_sel("swap_licence"), licence_number)
            except Exception:
                raise DVSAError("layout_change: licence field not found")

        # Fill booking reference
        self._stage = "login_swap_reference"
        ok_ref = await self._fill_by_label_variants(LABELS["swap_ref"], booking_reference)
        if not ok_ref:
            try:
                await self._fill_css(_sel("swap_ref"), booking_reference)
            except Exception:
                raise DVSAError("layout_change: booking reference field not found")

        # Optional email
        if email:
            self._stage = "login_swap_email"
            ok_email = await self._fill_by_label_variants(LABELS["email"], email)
            if not ok_email:
                try:
                    await self._fill_css(_sel("swap_email"), email)
                except Exception:
                    pass  # not fatal

        # Continue button
        self._stage = "login_swap_continue"
        if not await self._click_continue_role_fallback():
            raise DVSAError("layout_change: continue button not found")

        await self._expect_ok()

    async def login_new(self, licence_number: str, theory_pass: Optional[str] = None, email: Optional[str] = None):
        self._stage = "login_new_nav"
        await self._goto(URL_BOOK_TEST, stage="login_new_nav")
        await self._maybe_click_start_now()
        if not await self._licence_form_present():
            await asyncio.sleep(0.3)
            await self._maybe_click_start_now()
        await self._wait_for_dvsa_ready()

        # Category (if present)
        await self._select_car_category_if_present()

        # Licence (robust)
        self._stage = "login_new_licence"
        ok_lic = await self._fill_by_label_variants(LABELS["licence"], licence_number)
        if not ok_lic:
            try:
                await self._fill_css(_sel("new_licence"), licence_number)
            except Exception:
                raise DVSAError("layout_change: licence field not found")

        # Optional theory pass
        if theory_pass:
            self._stage = "login_new_theory"
            filled = await self._fill_by_label_variants(
                ["Theory pass number", "Theory test pass number", "Theory pass certificate number"], theory_pass
            )
            if not filled:
                try:
                    await self._fill_css(_sel("new_theory"), theory_pass)
                except Exception:
                    pass

        # Optional email
        if email:
            self._stage = "login_new_email"
            ok_email = await self._fill_by_label_variants(LABELS["email"], email)
            if not ok_email:
                try:
                    await self._fill_css(_sel("new_email"), email)
                except Exception:
                    pass

        await self._answer_no_no_if_present()

        # Continue
        self._stage = "login_new_continue"
        if not await self._click_continue_role_fallback():
            raise DVSAError("layout_change: continue button not found")

        await self._expect_ok()

    async def _open_centre(self, centre_name: str) -> bool:
        self._stage = f"centre_open:{centre_name}"
        # Some flows require a link to the centre search first; try common link names.
        for name in ["Test centre availability", "Change test centre", "Find a test centre", "Change location"]:
            try:
                await self.page.get_by_role("link", name=name).click(timeout=2000)
                await self._human_pause(CLICK_SETTLE_MS)
                await self._rps_pause("after_open_centre_link")
                break
            except Exception:
                pass

        await self.page.wait_for_selector(_sel("centre_search_box"))
        await self._wait_for_dvsa_ready()
        await self.page.fill(_sel("centre_search_box"), centre_name)
        await self._human_pause(450)

        # Use suggestions if present; else submit
        try:
            await self.page.wait_for_selector(_sel("centre_suggestions"), timeout=3000)
            suggestions = await self.page.query_selector_all(_sel("centre_suggestions"))
            chosen = None
            target = centre_name.strip().lower()
            for s in suggestions:
                t = (await s.text_content() or "").strip().lower()
                if target in t:
                    chosen = s
                    break
            if not chosen and suggestions:
                chosen = suggestions[0]
            if chosen:
                await chosen.click()
                await self._human_pause(CLICK_SETTLE_MS)
                await self._rps_pause("after_centre_suggestion_click")
            else:
                await self.page.keyboard.press("Enter")
                await self._human_pause(CLICK_SETTLE_MS)
                await self._rps_pause("after_centre_enter")
        except PWTimeout:
            await self.page.keyboard.press("Enter")
            await self._human_pause(CLICK_SETTLE_MS)
            await self._rps_pause("after_centre_enter_timeout")

        # If there is an explicit Search/Continue button, click it
        await self._click_continue_role_fallback()

        await self._expect_ok()
        return True

    async def search_centre_slots(self, centre_name: str) -> List[str]:
        """
        Return human-readable slots like:
            "Pinner · 2025-10-02 · 08:10"
        or (if date list isn’t present):
            "Pinner · (date unknown) · 08:10"
        Posts admin events when a sid is attached:
            checked:<centre> · no_slots|<n>
            captcha_cooldown:<centre|stage>
            error:<centre> · <ExceptionName>
        """
        try:
            await self._captcha_guard(f"pre_open:{centre_name}")
            if not await self._open_centre(centre_name):
                await self._event(f"error:{centre_name} · OpenFailed")
                return []

            slots: List[str] = []

            # Try dates → times
            try:
                date_nodes = await self.page.query_selector_all(_sel("centre_dates"))
            except PWTimeout:
                date_nodes = []

            if date_nodes:
                for d in date_nodes[:5]:  # only the first few dates per poll
                    dtxt = (await d.text_content() or "").strip()
                    if not dtxt:
                        continue
                    try:
                        await d.click()
                        await self._human_pause(CLICK_SETTLE_MS)
                        await self._rps_pause("after_date_click")
                    except PWTimeout:
                        continue
                    time_nodes = await self.page.query_selector_all(_sel("centre_times"))
                    for t in time_nodes:
                        ttxt = (await t.text_content() or "").strip()
                        if ttxt:
                            # normalise "view" rows
                            if ttxt.lower() == "view":
                                try:
                                    await t.click()
                                    await self._human_pause(CLICK_SETTLE_MS)
                                    await self._rps_pause("after_view_click")
                                    time_nodes2 = await self.page.query_selector_all(_sel("centre_times"))
                                    for tt in time_nodes2:
                                        vtxt = (await tt.text_content() or "").strip()
                                        if vtxt:
                                            slots.append(f"{centre_name} · {dtxt} · {vtxt}")
                                    continue
                                except Exception:
                                    pass
                            slots.append(f"{centre_name} · {dtxt} · {ttxt}")
            else:
                # Times only
                time_nodes = await self.page.query_selector_all(_sel("centre_times"))
                for t in time_nodes:
                    ttxt = (await t.text_content() or "").strip()
                    if ttxt:
                        slots.append(f"{centre_name} · (date unknown) · {ttxt}")

            await self._event(f"checked:{centre_name} · {('no_slots' if not slots else str(len(slots)))}")
            return slots

        except CaptchaDetected:
            await self._event(f"captcha_cooldown:{centre_name}")
            raise
        except Exception as e:
            await self._event(f"error:{centre_name} · {type(e).__name__}")
            raise

    async def swap_to(self, slot_label: str) -> bool:
        """
        Click the slot produced by search_centre_slots(), then confirm the change.
        Accepts either "Centre · YYYY-MM-DD · HH:MM" or "Centre · (date unknown) · HH:MM".
        Emits 'booked:' or 'booking_failed:' status when sid is attached.
        """
        await self._captcha_guard("pre_swap_confirm")
        await self._wait_for_dvsa_ready()

        # Parse parts
        m = re.match(r"^(.*?) · (.*?) · (\d{1,2}:\d{2})$", slot_label)
        centre = date_txt = time_txt = None
        if m:
            centre, date_txt, time_txt = m.groups()
            try:
                await self._open_centre(centre)
            except Exception:
                await self._event(f"error:{centre or 'unknown'} · OpenFailed")
                return False

        # Click date if present
        if date_txt and date_txt != "(date unknown)":
            try:
                date_nodes = await self.page.query_selector_all(_sel("centre_dates"))
                for d in date_nodes:
                    dtxt = (await d.text_content() or "").strip()
                    if date_txt in dtxt:
                        await d.click()
                        await self._human_pause(CLICK_SETTLE_MS)
                        await self._rps_pause("after_swap_date_click")
                        break
            except PWTimeout:
                pass

        # Click matching time
        try:
            time_nodes = await self.page.query_selector_all(_sel("centre_times"))
            clicked = False
            for t in time_nodes:
                ttxt = (await t.text_content() or "").strip()
                if time_txt and ttxt.startswith(time_txt):
                    await t.click()
                    await self._human_pause(CLICK_SETTLE_MS)
                    await self._rps_pause("after_swap_time_click")
                    clicked = True
                    break
                if not time_txt and slot_label in ttxt:
                    await t.click()
                    await self._human_pause(CLICK_SETTLE_MS)
                    await self._rps_pause("after_swap_time_click2")
                    clicked = True
                    break
            if not clicked:
                await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
                return False
        except PWTimeout:
            await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
            return False

        # Confirm
        try:
            await self._click_css(_sel("confirm_button"))
        except PWTimeout:
            await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
            return False

        await self._expect_ok()
        await self._status("booked", f"booked:{centre} · {date_txt} · {time_txt or 'unknown'}")
        return True

    # ---------- NEW: booking reference extraction ----------
    async def _extract_booking_reference(self) -> Optional[str]:
        """
        Tries a few robust patterns/locations to extract the DVSA booking/application
        reference (typically 8 digits) from the current page.
        Returns the reference string (e.g., '12345678') or None if not found yet.
        """
        try:
            # Give the confirmation page a moment to fully render
            await self._wait_for_dvsa_ready()
            html = await self.page.content()

            # 1) Look for common labels near an 8-digit number
            patterns = [
                r"(?:booking|application)\s+reference(?:\s+number)?[^0-9]{0,40}(\d{8})",
                r"reference\s+number[^0-9]{0,40}(\d{8})",
                r"\b(\d{8})\b",  # last resort: any 8-digit number on page
            ]
            for pat in patterns:
                m = re.search(pat, html, flags=re.I | re.S)
                if m:
                    return m.group(1)

            # 2) Try reading visible text from likely containers
            candidates = [
                "text=/booking reference/i",
                "text=/application reference/i",
                "text=/reference number/i",
                ".govuk-panel__body",
                ".govuk-panel--confirmation",
                "[data-test='reference'], [data-testid='reference']",
            ]
            for sel in candidates:
                try:
                    loc = self.page.locator(sel).first
                    if await loc.count() > 0:
                        txt = (await loc.text_content()) or ""
                        m = re.search(r"\b(\d{8})\b", txt)
                        if m:
                            return m.group(1)
                except Exception:
                    pass
        except Exception:
            pass
        return None

    # ---------- NEW: book & pay for NEW bookings and return reference ----------
    async def book_and_pay(self, slot_label: str) -> Optional[str]:
        """
        Book a NEW test slot (same label format as search results) and return the
        real 8-digit booking reference from the confirmation page.
        Returns:
            str booking_reference on success, or None on failure.
        Emits:
            - booked:<centre> · <date> · <time> · ref=<REF>
            - booking_failed:<centre> · <date> · <time>
        """
        await self._captcha_guard("pre_book_and_pay")
        await self._wait_for_dvsa_ready()

        # Parse parts
        m = re.match(r"^(.*?) · (.*?) · (\d{1,2}:\d{2})$", slot_label)
        centre = date_txt = time_txt = None
        if m:
            centre, date_txt, time_txt = m.groups()
            try:
                await self._open_centre(centre)
            except Exception:
                await self._event(f"error:{centre or 'unknown'} · OpenFailed")
                return None

        # Click date if present
        if date_txt and date_txt != "(date unknown)":
            try:
                date_nodes = await self.page.query_selector_all(_sel("centre_dates"))
                for d in date_nodes:
                    dtxt = (await d.text_content() or "").strip()
                    if date_txt in dtxt:
                        await d.click()
                        await self._human_pause(CLICK_SETTLE_MS)
                        await self._rps_pause("after_bookpay_date_click")
                        break
            except PWTimeout:
                pass

        # Click matching time
        try:
            time_nodes = await self.page.query_selector_all(_sel("centre_times"))
            clicked = False
            for t in time_nodes:
                ttxt = (await t.text_content() or "").strip()
                if time_txt and ttxt.startswith(time_txt):
                    await t.click()
                    await self._human_pause(CLICK_SETTLE_MS)
                    await self._rps_pause("after_bookpay_time_click")
                    clicked = True
                    break
                if not time_txt and slot_label in ttxt:
                    await t.click()
                    await self._human_pause(CLICK_SETTLE_MS)
                    await self._rps_pause("after_bookpay_time_click2")
                    clicked = True
                    break
            if not clicked:
                await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
                return None
        except PWTimeout:
            await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
            return None

        # Confirm (this may lead to payment + confirmation, or straight to confirmation)
        try:
            await self._click_css(_sel("confirm_button"))
        except PWTimeout:
            await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
            return None

        # We’re now on either a payment or confirmation page. Wait & try to extract reference.
        await self._expect_ok()

        # Give the flow a few chances (some journeys need a couple of redirects)
        for _ in range(3):
            ref = await self._extract_booking_reference()
            if ref:
                await self._status("booked", f"booked:{centre} · {date_txt} · {time_txt or 'unknown'} · ref={ref}")
                return ref
            # small pause and rps pacing before next attempt
            await self._human_pause(900)
            await self._rps_pause("after_bookpay_wait")

        # If still no reference, mark failed (caller can decide to retry)
        await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
        return None


# ------------- Polite sleep helper for worker loops (optional import) -------------
async def polite_sleep(base_seconds: float = 75.0, jitter: float = 0.25):
    """
    Sleep for base_seconds ± (base_seconds * jitter), min 5s.
    Use between DVSA polls per job to keep requests friendly.
    """
    j = base_seconds * (1 + random.uniform(-jitter, jitter))
    await asyncio.sleep(max(5.0, j))
