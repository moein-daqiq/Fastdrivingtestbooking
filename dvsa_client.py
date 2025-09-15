# dvsa_client.py
"""
Minimal, SAFE DVSA automation for FastDTF:
- No CAPTCHA bypass: raises CaptchaDetected if a bot wall appears.
- Public API used by worker: login_swap, login_new, search_centre_slots, swap_to.
- All selectors centralized in SEL with fallbacks.
- Optional session reuse (per licence) and optional proxy support.

⚠️ Verify/tweak the selectors in SEL to match the current DVSA pages once.
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


# ---------------- Exceptions ----------------
class DVSAError(Exception):
    pass


class CaptchaDetected(DVSAError):
    pass


# ---------------- Config (override via env if needed) ----------------
URL_CHANGE_TEST = os.environ.get("DVSA_URL_CHANGE", "https://www.gov.uk/change-driving-test")
URL_BOOK_TEST   = os.environ.get("DVSA_URL_BOOK",   "https://www.gov.uk/book-driving-test")

HEADLESS = os.environ.get("DVSA_HEADLESS", "true").lower() == "true"
NAV_TIMEOUT_MS = int(os.environ.get("DVSA_NAV_TIMEOUT_MS", "25000"))
SLOWMO_MS = int(os.environ.get("DVSA_SLOWMO_MS", "0"))
USER_AGENT = os.environ.get(
    "DVSA_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DVSA_PROXY = os.environ.get("DVSA_PROXY")  # e.g. http://user:pass@host:port

# All selectors live here; include generous fallbacks.
SEL = {
    # GOV.UK landing “Start now”
    "start_now": "a.govuk-button, a.button--start, a[href*='start']",

    # Existing booking (swap) login
    "swap_licence":  "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "swap_ref":      "input[name='booking-reference'], input[name*='reference'], input[id*='reference'], input[name='bookingReference']",
    "swap_email":    "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "swap_continue": "button[type='submit'], button.govuk-button",

    # New booking login
    "new_licence":   "input[name='driving-licence-number'], input[name*='licence'], input[id*='licence'], input[name='driverLicenceNumber']",
    "new_theory":    "input[name='theory-pass-number'], input[name*='candidate'], input[id*='theory'], input[name='theoryPassNumber']",
    "new_email":     "input[name='email'], input[id*='email'], input[name='candidateEmail']",
    "new_continue":  "button[type='submit'], button.govuk-button",

    # Centre search & availability
    "centre_search_box":  "input[name*='test-centre'], input[id*='test-centre'], input[name*='centre'], input[name='testCentreSearch']",
    "centre_suggestions": "ul[role='listbox'] li, li[role='option'], ul li a",
    "centre_dates":       "[data-test='available-dates'] li, ul.available-dates li, .dates li, .available-dates li",
    "centre_times":       "[data-test='available-times'] li, ul.available-times li, .times li, .available-times li",

    # Confirm
    "confirm_button": "button[type='submit'], button.govuk-button",

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
        if any(k in html for k in ["recaptcha", "hcaptcha", "unusual traffic", "not a robot", "captcha"]):
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

    # ------------- Public API used by worker -------------
    async def login_swap(self, licence_number: str, booking_reference: str, email: Optional[str] = None):
        await self._goto(URL_CHANGE_TEST)
        start = await self.page.query_selector(_sel("start_now"))
        if start:
            await start.click()

        await self._fill(_sel("swap_licence"), licence_number)
        await self._fill(_sel("swap_ref"), booking_reference)
        if email:
            await self._fill(_sel("swap_email"), email)
        await self._click(_sel("swap_continue"))
        await self._expect_ok()

    async def login_new(self, licence_number: str, theory_pass: Optional[str] = None, email: Optional[str] = None):
        await self._goto(URL_BOOK_TEST)
        start = await self.page.query_selector(_sel("start_now"))
        if start:
            await start.click()

        await self._fill(_sel("new_licence"), licence_number)
        if theory_pass:
            await self._fill(_sel("new_theory"), theory_pass)
        if email:
            await self._fill(_sel("new_email"), email)
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
        """
        await self._captcha_guard()
        if not await self._open_centre(centre_name):
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

        return slots

    async def swap_to(self, slot_label: str) -> bool:
        """
        Click the slot produced by search_centre_slots(), then confirm the change.
        Accepts either "Centre · YYYY-MM-DD · HH:MM" or "Centre · (date unknown) · HH:MM".
        """
        await self._captcha_guard()

        # Parse parts
        m = re.match(r"^(.*?) · (.*?) · (\d{1,2}:\d{2})$", slot_label)
        centre = date_txt = time_txt = None
        if m:
            centre, date_txt, time_txt = m.groups()
            await self._open_centre(centre)

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
            for t in time_nodes:
                ttxt = (await t.text_content() or "").strip()
                if time_txt and ttxt.startswith(time_txt):
                    await t.click()
                    break
                if not time_txt and slot_label in ttxt:
                    await t.click()
                    break
        except PWTimeout:
            return False

        # Confirm
        try:
            await self._click(_sel("confirm_button"))
        except PWTimeout:
            return False

        await self._expect_ok()
        return True
