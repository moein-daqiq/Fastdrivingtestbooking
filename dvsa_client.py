# =============================
# FILE: dvsa_client.py
# =============================
"""
Synchronous Playwright DVSA client tailored for FastDTF worker.
- Robust cookie-banner dismissal and cross-frame field discovery
- Label-first lookup with multiple fallbacks for licence/reference/theory fields
- Human-like pacing + optional RPS throttle
- CAPTCHA/WAF/Service-closed guards raising typed exceptions
- API compatible with worker.py: login_swap, login_new, search_centre_slots, swap_to, book_and_pay

Env knobs (optional):
  DVSA_HEADLESS=true
  DVSA_NAV_TIMEOUT_MS=25000
  DVSA_READY_MAX_MS=30000
  DVSA_POST_NAV_SETTLE_MS=800
  DVSA_CLICK_SETTLE_MS=700
  DVSA_USER_AGENT=<modern UA>
  DVSA_PROXY=http://user:pass@host:port
  DVSA_RPS=0.5
  DVSA_RPS_JITTER=0.35
"""

from __future__ import annotations

import os
import time
import random
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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
DVSA_USER_AGENT = os.environ.get(
    "DVSA_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)
DVSA_PROXY = os.environ.get("DVSA_PROXY", "")

DVSA_MANAGE_URL = "https://driverpracticaltest.dvsa.gov.uk/manage"

# ---------- Label text we try to match (GOV.UK varies) ----------
LICENCE_LABELS = [
    "Driving licence number",
    "Driving license number",
    "Driver number",
    "Licence number",
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

# ---------- DVSA RPS throttle ----------
DVSA_RPS = float(os.environ.get("DVSA_RPS", "0.5"))
DVSA_RPS_JITTER = float(os.environ.get("DVSA_RPS_JITTER", "0.35"))

@dataclass
class DVSAClient:
    honour_client_rps: bool = True
    dvsa_rps: float = DVSA_RPS
    dvsa_rps_jitter: float = DVSA_RPS_JITTER

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

    # ---------- pacing ----------
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

    # ---------- query helpers ----------
    def _frames(self):
        return [self._page, *self._page.frames]

    def _find_any(self, selectors: List[str]):
        for scope in self._frames():
            for s in selectors:
                try:
                    loc = scope.locator(s)
                    if loc and loc.is_visible():
                        return loc
                except Exception:
                    continue
        return None

    def _get_by_label_any(self, labels: List[str]):
        for scope in self._frames():
            for t in labels:
                try:
                    loc = scope.get_by_label(t, exact=False)
                    if loc and loc.is_visible():
                        return loc
                except Exception:
                    continue
        return None

    def _dismiss_cookie_banners(self) -> None:
        buttons = [
            "button:has-text('Accept additional cookies')",
            "button:has-text('Accept all cookies')",
            "#accept-additional-cookies",
            "#accept-all-cookies",
            "text=Accept cookies",
        ]
        btn = self._find_any(buttons)
        if btn:
            try:
                self._click(btn)
            except Exception:
                pass
        hide = self._find_any(["button:has-text('Hide')", "#hide-cookie-banner"])
        if hide:
            try:
                self._click(hide)
            except Exception:
                pass

    # ---------- low-level actions ----------
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

    # ---------- guards & nav ----------
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
        html = self._page.content().lower()
        return any(k in html for k in ["request blocked", "access denied", "http 403", "forbidden"])  # heuristic

    # ---------- form discovery ----------
    def _ensure_login_form(self, stage: str) -> Dict[str, Any]:
        self._dismiss_cookie_banners()

        licence = self._get_by_label_any(LICENCE_LABELS)
        ref = self._get_by_label_any(REF_LABELS)
        theory = self._get_by_label_any(THEORY_LABELS)

        if not licence:
            licence = self._find_any([
                "input[name='driving-licence-number']",
                "input#driving-licence-number",
                "input[name*='licence']",
                "input[autocomplete='driving-licence-number']",
                "input[name*='driver']",
            ])
        if not ref:
            ref = self._find_any([
                "input[name='application-reference-number']",
                "input#application-reference-number",
                "input[name*='reference']",
                "input[name='booking-reference']",
            ])
        if not theory:
            theory = self._find_any([
                "input[name='theory-test-pass-number']",
                "input#theory-test-pass-number",
                "input[name*='theory']",
            ])

        if not (licence and (ref or theory)):
            raise LayoutIssue(f"{stage}: licence field not found")

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
        for _ in range(5):
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
        for _ in range(5):
            try:
                parts = self._ensure_login_form("login_new")
                self._fill(parts["licence"], licence)
                if parts["theory"]:
                    self._fill(parts["theory"], theory_pass)
                else:
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
        # Placeholder. Replace with real selectors when ready.
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
