# dvsa_client.py
"""
Minimal, SAFE DVSA automation for FastDTF:
- No CAPTCHA bypass: raises CaptchaDetected if a bot wall appears.
- Public API used by worker: login_swap, login_new, search_centre_slots, swap_to.
- All selectors centralized in SEL with fallbacks.
- Optional session reuse (per licence) and optional proxy support.

Instrumentation added:
- Optional event/status callbacks to your API when a job id (sid) is attached.
- Events posted:
    checked:<centre> · no_slots|<count>
    captcha_cooldown:<centre>
    slot_found:<centre> · <date> · <time>        (only when swap_to() is called)
    booked:<centre> · <date> · <time>
    booking_failed:<centre> · <date> · <time>
    error:<centre> · <ExceptionName>

Set these env vars on the worker host (optional):
    API_BASE=https://api.fastdrivingtestfinder.co.uk/api
    WORKER_TOKEN=<same secret as backend>
"""

from __future__ import annotations

import os
import re
import asyncio
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
    pass


# ---------------- Config (override via env if needed) ----------------
# Use DVSA application/manage entry points directly (skip the GOV.UK explainer).
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

# Backend instrumentation config (optional)
API_BASE = os.environ.get("API_BASE", "https://api.fastdrivingtestfinder.co.uk/api").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

# All selectors live here; include generous fallbacks.
SEL = {
    # GOV.UK landing “Start now”
    "start_now": "a.govuk-button, a.button--start, a[href*='start']",

    # NEW booking – category page (pick Car)
    "car_button": "button:has-text('Car'), a:has-text('Car (manual'), .app-button:has-text('Car'), [role='button']:has-text('Car')",

    # Existing booking (swap) login
    "swap_licence":  "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "swap_ref":      "input[name='booking-reference'], input[name*='reference'], input[id*='reference'], input[name='bookingReference']",
    "swap_email":    "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "swap_continue": "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # New booking login (licence details page)
    "new_licence":   "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "new_theory":    "input[name='theory-pass-number'], input[name*='candidate'], input[id*='theory'], input[name='theoryPassNumber']",
    "new_email":     "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "new_continue":  "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # Centre search & availability
    "centre_search_box":  "input[name*='test-centre'], input[id*='test-centre'], input[name*='centre'], input[name='testCentreSearch']",
    "centre_suggestions": "ul[role='listbox'] li, li[role='option'], ul li a",
    "centre_dates":       "[data-test='available-dates'] li, ul.available-dates li, .dates li, .available-dates li",
    "centre_times":       "[data-test='available-times'] li, ul.available-times li, .times li, .available-times li",

    # Confirm
    "confirm_button": "button[type='submit'], button.govuk-button, [role='button'][type='submit']",

    # Errors
    "error_summary": ".govuk-error-summary, [role='alert']",
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

    # ------------- Context mgr -------------
    async def __aenter__(self) -> "DVSAClient":
        self.pw = await async_playwright().start()

        launch_kwargs = dict(headless=self.headless, slow_mo=SLOWMO_MS)
        if DVSA_PROXY:
            launch_kwargs["proxy"] = {"server": DVSA_PROXY}

        self.browser = await self.pw.chromium.launch(
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-breakpad",
                "--no-first-run",
            ],
            **launch_kwargs,
        )

        storage_state = None
        if self.session_key:
            safe_key = "".join(ch for ch in self.session_key if ch.isalnum() or ch in ("-", "_"))[:40]
            self._storage_path = f"/tmp/dvsa_sess_{safe_key}.json"
            if os.path.exists(self._storage_path):
                storage_state = self._storage_path

        self.context = await self.browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 850},
            storage_state=storage_state,
        )
        # Reduce automation fingerprint
        await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")

        self.page = await self.context.new_page()
        self.page.set_default_timeout(NAV_TIMEOUT_MS)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self._storage_path and self.context:
                try:
                    await self.context.storage_state(path=self._storage_path)
                except Exception:
                    pass
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        finally:
            if self.pw:
                await self.pw.stop()
            await self._close_http()

    # ------------- Helpers -------------
    async def _goto(self, url: str, marker: Optional[str] = None):
        await self.page.goto(url, wait_until="domcontentloaded")
        await self._captcha_guard()
        if marker:
            await self.page.wait_for_selector(f"text={marker}")

    async def _fill(self, sel: str, value: str):
        await self.page.wait_for_selector(sel)
        await self.page.fill(sel, value)

    async def _click(self, sel: str):
        await self.page.wait_for_selector(sel)
        await self.page.click(sel)

    async def _captcha_guard(self):
        html = (await self.page.content()).lower()
        if any(k in html for k in ["recaptcha", "hcaptcha", "unusual traffic", "not a robot", "captcha", "additional security check is required"]):
            # Optional: screenshot for debugging
            try:
                ts = str(int(asyncio.get_event_loop().time()))
                await self.page.screenshot(path=f"/tmp/captcha_{ts}.png", full_page=True)
            except Exception:
                pass
            raise CaptchaDetected("Captcha/anti-bot detected")

    async def _expect_ok(self):
        await self._captcha_guard()
        err = await self.page.query_selector(_sel("error_summary"))
        if err:
            msg = (await err.text_content() or "").strip()
            raise DVSAError(f"DVSA error: {msg}")

    async def _maybe_click_start_now(self):
        # If we land on the GOV.UK explainer, click Start now.
        try:
            start = await self.page.query_selector(_sel("start_now"))
            if start:
                await start.click()
        except Exception:
            pass

    async def _select_car_category_if_present(self):
        # On /application the first page is the category picker (Car, Motorcycles, ...).
        # If it's there, click Car. If not, we are already on licence details.
        try:
            btn = await self.page.query_selector(_sel("car_button"))
            if btn:
                await btn.click()
                await self._captcha_guard()
        except PWTimeout:
            pass
        except Exception:
            # Non-fatal: if the page is already the licence details, proceed.
            pass

    async def _answer_no_no_if_present(self):
        """
        On the licence details page, DVSA asks:
          - Have you been ordered by a court to take an extended test?
          - Do you have any special requirements?
        We answer No/No when those controls exist.
        """
        try:
            # Use accessible labels where possible (Playwright will find the matching radio).
            no_radios = self.page.get_by_label("No", exact=True)
            # Click first two "No" radios if present.
            await no_radios.nth(0).check()
            await asyncio.sleep(0.05)
            await no_radios.nth(1).check()
        except Exception:
            # Fall back: click any radios with value/no.
            try:
                radios = await self.page.query_selector_all("input[type='radio'][value='no'], input[type='radio'][aria-label='No']")
                for r in radios[:2]:
                    try:
                        await r.check()
                    except Exception:
                        pass
            except Exception:
                pass

    # ------------- Public API used by worker -------------
    async def login_swap(self, licence_number: str, booking_reference: str, email: Optional[str] = None):
        await self._goto(URL_CHANGE_TEST)
        await self._maybe_click_start_now()

        await self._fill(_sel("swap_licence"), licence_number)
        await self._fill(_sel("swap_ref"), booking_reference)
        if email:
            await self._fill(_sel("swap_email"), email)
        await self._click(_sel("swap_continue"))
        await self._expect_ok()

    async def login_new(self, licence_number: str, theory_pass: Optional[str] = None, email: Optional[str] = None):
        await self._goto(URL_BOOK_TEST)
        await self._maybe_click_start_now()

        # If the category picker is visible, choose Car.
        await self._select_car_category_if_present()

        # Now licence details page: fill licence, optional theory/email, answer No/No, continue.
        await self._fill(_sel("new_licence"), licence_number)
        if theory_pass:
            try:
                await self._fill(_sel("new_theory"), theory_pass)
            except Exception:
                # not always present / required
                pass
        if email:
            try:
                await self._fill(_sel("new_email"), email)
            except Exception:
                pass

        await self._answer_no_no_if_present()
        await self._click(_sel("new_continue"))
        await self._expect_ok()

    async def _open_centre(self, centre_name: str) -> bool:
        await self.page.wait_for_selector(_sel("centre_search_box"))
        await self.page.fill(_sel("centre_search_box"), centre_name)

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
            else:
                await self.page.keyboard.press("Enter")
        except PWTimeout:
            await self.page.keyboard.press("Enter")

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
            captcha_cooldown:<centre>
            error:<centre> · <ExceptionName>
        """
        try:
            await self._captcha_guard()
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
                        await asyncio.sleep(0.25)  # small settle
                    except PWTimeout:
                        continue
                    time_nodes = await self.page.query_selector_all(_sel("centre_times"))
                    for t in time_nodes:
                        ttxt = (await t.text_content() or "").strip()
                        if ttxt:
                            slots.append(f"{centre_name} · {dtxt} · {ttxt}")
            else:
                # Times only
                time_nodes = await self.page.query_selector_all(_sel("centre_times"))
                for t in time_nodes:
                    ttxt = (await t.text_content() or "").strip()
                    if ttxt:
                        slots.append(f"{centre_name} · (date unknown) · {ttxt}")

            # emit a compact heartbeat with count
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
        await self._captcha_guard()

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
                    clicked = True
                    break
                if not time_txt and slot_label in ttxt:
                    await t.click()
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
            await self._click(_sel("confirm_button"))
        except PWTimeout:
            await self._event(f"booking_failed:{centre} · {date_txt} · {time_txt or 'unknown'}")
            return False

        await self._expect_ok()
        await self._status("booked", f"booked:{centre} · {date_txt} · {time_txt or 'unknown'}")
        return True


# ------------- Polite sleep helper for worker loops (optional import) -------------
async def polite_sleep(base_seconds: float = 75.0, jitter: float = 0.25):
    """
    Sleep for base_seconds ± (base_seconds * jitter), min 5s.
    Use between DVSA polls per job to keep requests friendly.
    """
    import random
    j = base_seconds * (1 + random.uniform(-jitter, jitter))
    await asyncio.sleep(max(5.0, j))
