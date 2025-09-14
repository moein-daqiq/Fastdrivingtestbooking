# dvsa_client.py
import asyncio
from datetime import datetime
from typing import List, Optional

from playwright.async_api import async_playwright, Browser, Page

class DVSAError(Exception):
    pass

class CaptchaDetected(DVSAError):
    pass

class DVSAClient:
    """
    Minimal, SAFE DVSA automation:
      - No CAPTCHA bypass. If detected, raise CaptchaDetected.
      - Lightweight navigation with explicit selectors (update as DVSA HTML evolves).
    NOTE: You MUST update the URLS and selectors below to match DVSA pages.
    """

    # ===== TODO: Confirm these URLs in your environment (inspect in your browser) =====
    URL_CHANGE_TEST = "https://www.gov.uk/change-driving-test"           # or direct deep link to the DVSA change portal
    URL_BOOK_TEST   = "https://www.gov.uk/book-driving-test"             # entry for new bookings

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

    async def __aenter__(self):
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=self.headless,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-breakpad",
                "--no-first-run",
            ],
        )
        self.page = await self.browser.new_page(user_agent="Mozilla/5.0 (compatible; FastDTF/1.0)")
        self.page.set_default_timeout(15000)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if self.browser:
                await self.browser.close()
        finally:
            await self.pw.stop()

    # ---------------- Helpers ----------------
    async def _ensure_no_captcha(self):
        # Very conservative checks; extend if DVSA changes markup.
        sel = [
            "iframe[src*='recaptcha']",
            "div.g-recaptcha",
            "div.h-captcha",
            "iframe[src*='hcaptcha']",
            "text=are you a robot",
            "text=select all images",  # generic wording
        ]
        for s in sel:
            if await self.page.query_selector(s):
                raise CaptchaDetected("CAPTCHA detected")

    async def _goto_and_wait(self, url: str, text_marker: str):
        await self.page.goto(url, wait_until="domcontentloaded")
        await self._ensure_no_captcha()
        await self.page.wait_for_selector(f"text={text_marker}")

    # ---------------- Login flows ----------------
    async def login_swap(self, licence_number: str, booking_reference: str, email: Optional[str] = None):
        """
        Change/swap flow usually asks for: licence number + booking reference (+ sometimes email).
        Update selectors after inspecting the DVSA form fields in DevTools.
        """
        await self._goto_and_wait(self.URL_CHANGE_TEST, "Change your driving test")

        # Example: click 'Start now' on GOV.UK pattern
        start = await self.page.query_selector("a.govuk-button, a[href*='start']")
        if start:
            await start.click()

        await self._ensure_no_captcha()
        # TODO: Replace with actual selectors
        await self.page.fill("input[name='driverLicenceNumber']", licence_number)
        await self.page.fill("input[name='bookingReference']", booking_reference)
        if email:
            await self.page.fill("input[name='candidateEmail']", email)
        await self.page.click("button[type='submit']")

        await self._ensure_no_captcha()
        # Wait for dashboard/appointments page
        await self.page.wait_for_selector("text=Change appointment")

    async def login_new(self, licence_number: str, theory_pass: Optional[str] = None, email: Optional[str] = None):
        """
        New booking flow often requires: licence number (+ theory pass reference).
        Update selectors/steps to match DVSA booking portal.
        """
        await self._goto_and_wait(self.URL_BOOK_TEST, "Book your driving test")
        start = await self.page.query_selector("a.govuk-button, a[href*='start']")
        if start:
            await start.click()

        await self._ensure_no_captcha()
        # TODO: Replace with actual selectors
        await self.page.fill("input[name='driverLicenceNumber']", licence_number)
        if theory_pass:
            await self.page.fill("input[name='theoryPassNumber']", theory_pass)
        if email:
            await self.page.fill("input[name='candidateEmail']", email)
        await self.page.click("button[type='submit']")

        await self._ensure_no_captcha()
        await self.page.wait_for_selector("text=Choose a test centre")

    # ---------------- Slot search ----------------
    async def search_centre_slots(self, centre_name: str) -> List[str]:
        """
        Navigate to the centre search page, select `centre_name`, and scrape available slots.
        Return a list of strings like 'Pinner · 2025-09-14 08:10'.
        """
        await self._ensure_no_captcha()

        # TODO: navigate to centre selection (depends on current portal step).
        # Example pseudo-steps:
        #  - type centre into autocomplete
        #  - select from dropdown
        #  - submit / view availability
        #
        # Update selectors here after inspecting the page.
        await self.page.fill("input[name='testCentreSearch']", centre_name)
        # pick first suggestion
        await self.page.keyboard.press("ArrowDown")
        await self.page.keyboard.press("Enter")
        await self.page.click("button[type='submit']")

        await self._ensure_no_captcha()
        # Now parse available slots (update selector!)
        slot_cards = await self.page.query_selector_all("[data-slot], .slot, .available-slot")
        results: List[str] = []
        for el in slot_cards:
            txt = (await el.inner_text()).strip()
            if txt:
                # Normalize to a single line
                txt = " ".join(txt.split())
                results.append(f"{centre_name} · {txt}")
        return results

    # ---------------- Swap (reschedule) ----------------
    async def swap_to(self, slot_label: str) -> bool:
        """
        Click the slot row/button matching the label, confirm, and wait for success.
        """
        await self._ensure_no_captcha()
        # Find by partial text; adjust to button/link selector used on DVSA
        btn = await self.page.query_selector(f"text={slot_label}")
        if not btn:
            return False
        await btn.click()
        await self._ensure_no_captcha()

        # Confirm page
        confirm = await self.page.query_selector("button[type='submit'], .govuk-button")
        if confirm:
            await confirm.click()
        await self._ensure_no_captcha()

        # Success marker (update to the DVSA success message)
        await self.page.wait_for_selector("text=Your appointment has been changed")
        return True
