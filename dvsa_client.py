# =============================
# FILE: dvsa_client.py  (synchronous Playwright + rich live instrumentation)
# =============================
"""
Synchronous DVSA automation client for FastDTF worker.
- Robust readiness + cookie banner handling
- Label-first discovery with multiple fallbacks
- Human-like pacing with optional internal RPS throttle
- CAPTCHA / WAF / Service-closed guards with typed exceptions
- Rich, per-step instrumentation to your backend when a job SID is attached
- Exposes current_stage/current_centre so the worker can heartbeat a "Live Status" line every minute

Public API used by the worker (sync):
  - login_swap(licence_number, booking_reference, email=None)
  - login_new(licence_number, theory_pass=None, email=None)
  - search_centre_slots(centre_name) -> List[dict]
  - swap_to(slot_dict) -> bool
  - book_and_pay(slot_dict, mode="simulate"|"real") -> bool
"""
from __future__ import annotations

import os
import re
import json
import time
import random
import logging
from typing import Any, Dict, List, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout  # type: ignore

log = logging.getLogger("dvsa")

# ---------- Exceptions ----------
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

# ---------- ENV ----------
API_BASE = os.environ.get("API_BASE", "http://localhost:8000/api").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")

HEADLESS = os.environ.get("DVSA_HEADLESS", "true").lower() == "true"
NAV_TIMEOUT_MS = int(os.environ.get("DVSA_NAV_TIMEOUT_MS", "25000"))
READY_MAX_MS = int(os.environ.get("DVSA_READY_MAX_MS", "30000"))
POST_NAV_SETTLE_MS = int(os.environ.get("DVSA_POST_NAV_SETTLE_MS", "800"))
CLICK_SETTLE_MS = int(os.environ.get("DVSA_CLICK_SETTLE_MS", "700"))
USER_AGENT = os.environ.get("DVSA_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
DVSA_PROXY = os.environ.get("DVSA_PROXY", "")

DVSA_RPS = float(os.environ.get("DVSA_RPS", "0.5"))
DVSA_RPS_JITTER = float(os.environ.get("DVSA_RPS_JITTER", "0.35"))

URL_CHANGE_TEST = os.environ.get("DVSA_URL_CHANGE", "https://driverpracticaltest.dvsa.gov.uk/manage")
URL_BOOK_TEST   = os.environ.get("DVSA_URL_BOOK",   "https://driverpracticaltest.dvsa.gov.uk/application")

# ---------- Labels & selectors ----------
LICENCE_LABELS = [
    "Driving licence number",
    "Driving license number",
    "Driver number",
    "Licence number",
    "Driving licence",
]
REF_LABELS = [
    "Application reference number",
    "Booking reference",
    "Reference number",
]
THEORY_LABELS = [
    "Theory test pass number",
    "Theory pass number",
    "Theory pass certificate number",
]
SEL = {
    "start_now": "a.govuk-button--start, a.govuk-button[href*='start'], a.button--start, a[href*='start now' i]",
    "swap_licence": "input[name*='licen'], input[id*='licen'], input#driving-licence-number, input#drivingLicenceNumber",
    "swap_ref": "input[name*='reference'], input[id*='reference'], input#booking-reference, input[name='booking-reference']",
    "new_licence": "input[name*='licen'], input[id*='licen'], input#driving-licence-number, input#drivingLicenceNumber",
    "new_theory": "input[name*='theory'], input[id*='theory']",
    "centre_search_box": "input[name*='test-centre'], input[id*='test-centre'], input[name*='centre'], input[name='testCentreSearch']",
    "centre_suggestions": "ul[role='listbox'] li, li[role='option'], ul li a",
    "centre_dates": "[data-test='available-dates'] li, ul.available-dates li, .available-dates li",
    "centre_times": "[data-test='available-times'] li, ul.available-times li, .available-times li, .slot, a:has-text(':')",
    "continue": "button[type='submit'], button.govuk-button, [role='button'][type='submit']",
    "error_summary": ".govuk-error-summary, [role='alert']",
}

# ---------- Client ----------
class DVSAClient:
    def __init__(self, honour_client_rps: bool = True, dvsa_rps: float = DVSA_RPS, dvsa_rps_jitter: float = DVSA_RPS_JITTER) -> None:
        self._p = None
        self._browser = None
        self._ctx = None
        self._page = None
        self._last_ts = 0.0
        self._honour = honour_client_rps
        self._rps = max(0.05, dvsa_rps)
        self._jit = max(0.0, dvsa_rps_jitter)
        self.sid: Optional[int] = None
        # Exposed for heartbeat
        self.current_stage: str = "init"
        self.current_centre: str = ""
        self._boot()

    # ----- boot/close -----
    def _boot(self) -> None:
        self._p = sync_playwright().start()
        kwargs = {"headless": HEADLESS}
        if DVSA_PROXY:
            kwargs["proxy"] = {"server": DVSA_PROXY}
        self._browser = self._p.chromium.launch(**kwargs)
        self._ctx = self._browser.new_context(user_agent=USER_AGENT)
        self._ctx.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        self._ctx.set_default_timeout(NAV_TIMEOUT_MS)
        self._page = self._ctx.new_page()
        log.info("playwright chromium ready")

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

    # ----- instrumentation wiring -----
    def attach_job(self, sid: int) -> None:
        self.sid = int(sid)

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {WORKER_TOKEN}",
            "User-Agent": "FastDTF-DVSA/2025-10-07",
        }

    def _post_json(self, path: str, payload: Dict[str, Any]) -> None:
        if not (self.sid and API_BASE and WORKER_TOKEN):
            return
        try:
            url = f"{API_BASE}{path}"
            requests.post(url, headers=self._headers(), data=json.dumps(payload), timeout=15)
        except Exception:
            pass

    def _event(self, text: str) -> None:
        if not self.sid:
            return
        self._post_json(f"/worker/searches/{self.sid}/event", {"event": text})

    def _status(self, status: Optional[str] = None, **fields: Any) -> None:
        if not self.sid:
            return
        payload: Dict[str, Any] = {"event": fields.pop("event", "")}
        if status:
            payload["status"] = status
        payload.update(fields)
        self._post_json(f"/worker/searches/{self.sid}/status", payload)

    # Convenience: set stage/centre and emit a status line
    def _live(self, text: str, *, centre: Optional[str] = None) -> None:
        if centre is not None:
            self.current_centre = centre
        self.current_stage = text
        self._status(event=text, last_centre=self.current_centre)
        self._event(text)  # mirror for current UI

    # ----- pacing/guards -----
    def _sleep_rps(self) -> None:
        if not self._honour:
            return
        now = time.time()
        min_gap = 1.0 / self._rps
        jitter = random.uniform(0, min_gap * self._jit)
        wait = max(0.0, (self._last_ts + min_gap + jitter) - now)
        if wait > 0:
            time.sleep(wait)
        self._last_ts = time.time()

    def _ready_guard(self) -> None:
        self._sleep_rps()
        time.sleep(POST_NAV_SETTLE_MS / 1000.0)

    def _click(self, sel_or_loc) -> None:
        self._sleep_rps()
        if isinstance(sel_or_loc, str):
            self._page.click(sel_or_loc)
        else:
            sel_or_loc.click()
        time.sleep(CLICK_SETTLE_MS / 1000.0)

    def _fill(self, sel_or_loc, text: str) -> None:
        self._sleep_rps()
        if isinstance(sel_or_loc, str):
            self._page.fill(sel_or_loc, text)
        else:
            sel_or_loc.fill(text)

    def _dismiss_cookies(self) -> None:
        for s in [
            "button:has-text('Accept additional cookies')",
            "button:has-text('Accept all cookies')",
            "#accept-additional-cookies",
            "#accept-all-cookies",
            "text=Accept cookies",
        ]:
            try:
                if self._page.locator(s).is_visible():
                    self._click(s)
                    break
            except Exception:
                pass

    def _is_captcha(self) -> bool:
        html = self._page.content().lower()
        return any(k in html for k in ["cf-challenge", "hcaptcha", "g-recaptcha", "are you human", "unusual traffic"]) or "hcaptcha.com" in (self._page.url.lower())

    def _is_waf_block(self) -> bool:
        html = self._page.content().lower()
        return any(k in html for k in ["request blocked", "access denied", "http 403", "forbidden", "error code 1020"])  # heuristic

    def _goto(self, url: str, stage: str) -> None:
        try:
            self._page.goto(url)
        except PWTimeout:
            self._live(f"nav_timeout:{stage}")
            raise ServiceClosed(stage)
        self._ready_guard()
        self._dismiss_cookies()
        if self._is_captcha():
            self._live(f"captcha_cooldown:{stage}")
            raise CaptchaDetected(stage)
        if self._is_waf_block():
            self._live(f"ip_blocked:{stage}")
            raise WAFBlocked(stage)

    # ----- helpers -----
    def _get_by_label_any(self, labels: List[str]):
        for t in labels:
            try:
                loc = self._page.get_by_label(t, exact=False)
                if loc and loc.is_visible():
                    return loc
            except Exception:
                pass
        return None

    def _find_any(self, selectors: List[str]):
        for s in selectors:
            try:
                loc = self._page.locator(s)
                if loc and loc.is_visible():
                    return loc
            except Exception:
                pass
        return None

    # ----- Public API -----
    def login_swap(self, licence: str, booking_ref: str, email: Optional[str] = None) -> None:
        if not (licence and booking_ref and len(booking_ref) == 8 and booking_ref.isdigit()):
            raise LayoutIssue("invalid swap credentials")
        stage = "login_swap"
        self._live("form_nav", centre=None)
        self._goto(URL_CHANGE_TEST, stage)
        self._live("form_nav_ok")

        lic = self._get_by_label_any(LICENCE_LABELS) or self._find_any([SEL["swap_licence"]])
        if not lic:
            self._live("licence_field_missing")
            raise LayoutIssue("layout_change: licence field not found")
        self._live("licence_field_found")
        self._fill(lic, licence)
        self._live("licence_number_entered")

        ref = self._get_by_label_any(REF_LABELS) or self._find_any([SEL["swap_ref"]])
        if not ref:
            self._live("booking_reference_field_missing")
            raise LayoutIssue("layout_change: booking reference field not found")
        self._live("booking_reference_field_found")
        self._fill(ref, booking_ref)
        self._live("booking_reference_entered")

        if email:
            try:
                em = self._page.get_by_label("Email", exact=False)
                if em and em.is_visible():
                    self._live("email_field_found")
                    self._fill(em, email)
                    self._live("email_entered")
            except Exception:
                pass

        cont = self._find_any([SEL["continue"], "button:has-text('Continue')"]) or self._page.locator("button.govuk-button").first
        if cont:
            self._click(cont)
            self._live("continue_clicked")
        self._ready_guard()

        if self._is_captcha():
            self._live("captcha_cooldown:login_after_continue")
            raise CaptchaDetected(stage)
        if self._is_waf_block():
            self._live("ip_blocked:login_after_continue")
            raise WAFBlocked(stage)

    def login_new(self, licence: str, theory_pass: Optional[str] = None, email: Optional[str] = None) -> None:
        if not licence:
            raise LayoutIssue("invalid new credentials")
        stage = "login_new"
        self._live("form_nav", centre=None)
        self._goto(URL_BOOK_TEST, stage)
        self._live("form_nav_ok")

        lic = self._get_by_label_any(LICENCE_LABELS) or self._find_any([SEL["new_licence"]])
        if not lic:
            self._live("licence_field_missing")
            raise LayoutIssue("layout_change: licence field not found")
        self._live("licence_field_found")
        self._fill(lic, licence)
        self._live("licence_number_entered")

        if theory_pass:
            th = self._get_by_label_any(THEORY_LABELS) or self._find_any([SEL["new_theory"]])
            if th:
                self._live("theory_field_found")
                self._fill(th, theory_pass)
                self._live("theory_number_entered")

        if email:
            try:
                em = self._page.get_by_label("Email", exact=False)
                if em and em.is_visible():
                    self._live("email_field_found")
                    self._fill(em, email)
                    self._live("email_entered")
            except Exception:
                pass

        cont = self._find_any([SEL["continue"], "button:has-text('Continue')"]) or self._page.locator("button.govuk-button").first
        if cont:
            self._click(cont)
            self._live("continue_clicked")
        self._ready_guard()

        if self._is_captcha():
            self._live("captcha_cooldown:login_after_continue")
            raise CaptchaDetected(stage)
        if self._is_waf_block():
            self._live("ip_blocked:login_after_continue")
            raise WAFBlocked(stage)

    def _open_centre(self, centre_name: str) -> None:
        self.current_centre = centre_name
        self._live(f"centre_open_start:{centre_name}", centre=centre_name)
        box = self._find_any([SEL["centre_search_box"]])
        if not box:
            raise LayoutIssue("centre search box not found")
        self._fill(box, centre_name)
        self._live(f"centre_query_entered:{centre_name}")
        try:
            sugg = self._page.locator(SEL["centre_suggestions"]).first
            if sugg and sugg.is_visible():
                self._click(sugg)
            else:
                self._page.keyboard.press("Enter")
        except Exception:
            self._page.keyboard.press("Enter")
        self._ready_guard()
        self._live(f"centre_selected:{centre_name}")

    def search_centre_slots(self, centre_name: str) -> List[Dict[str, str]]:
        self._open_centre(centre_name)
        slots: List[Dict[str, str]] = []
        try:
            dates = self._page.locator(SEL["centre_dates"])  # may be empty
            n_dates = dates.count() if hasattr(dates, "count") else 0
            if n_dates > 0:
                for i in range(min(5, n_dates)):
                    try:
                        dates.nth(i).click()
                        self._ready_guard()
                        times = self._page.locator(SEL["centre_times"]) or []
                        n = times.count() if hasattr(times, "count") else 0
                        for j in range(n):
                            ttxt = times.nth(j).inner_text().strip()
                            if not ttxt:
                                continue
                            slots.append({"centre": centre_name, "date": f"date#{i+1}", "time": ttxt})
                    except Exception:
                        continue
            else:
                self._live(f"centre_no_dates:{centre_name}")
        except Exception:
            pass
        if slots:
            self._live(f"centre_times_listed:{centre_name}")
        self._live(f"checked:{centre_name} · {('no_slots' if not slots else str(len(slots)))}")
        return slots

    def swap_to(self, slot: Dict[str, str]) -> bool:
        self._live(f"swap_attempt:{slot.get('centre','?')} · {slot.get('date','?')} · {slot.get('time','?')}")
        btn = self._find_any([SEL["continue"], "button:has-text('Confirm')", "button:has-text('Book')"])  # heuristic
        if btn:
            self._click(btn)
            self._ready_guard()
            self._status(status="booked", event="swap_confirm_clicked", last_centre=self.current_centre)
            self._event("booked")
            return True
        self._live("swap_fail:no_button")
        return False

    def book_and_pay(self, slot: Dict[str, str], mode: str = "simulate") -> bool:
        self._live(f"book_attempt:{slot.get('centre','?')} · {slot.get('date','?')} · {slot.get('time','?')}")
        if mode == "simulate":
            self._status(status="booked", event="book_simulated", last_centre=self.current_centre)
            self._event("booked")
            return True
        btn = self._find_any([SEL["continue"], "button:has-text('Confirm')", "button:has-text('Book and pay')"])  # heuristic
        if btn:
            self._click(btn)
            self._ready_guard()
            self._status(status="booked", event="bookpay_clicked", last_centre=self.current_centre)
            self._event("booked")
            return True
        self._live("bookpay_fail:no_button")
        return False
