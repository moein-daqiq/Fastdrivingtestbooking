# dvsa_client.py  (only relevant changes shown below; rest of file unchanged)

# ... existing imports ...
from typing import List, Optional, Tuple

# (rest of the file unchanged up to class DVSAClient)

class DVSAClient:
    # ... existing __init__ and helpers ...

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
            #    (Booking reference, Application reference, Reference number)
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

    # ------------- Public API used by worker (existing) -------------
    # ... login_swap, login_new, search_centre_slots, swap_to (unchanged) ...

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
        Notes:
            This assumes the DVSA journey leads directly to a confirmation page
            after selecting a time and confirming. If an intermediate payment page
            appears that requires manual card entry, this function will wait for a
            confirmation state and attempt to scrape the reference; otherwise it
            returns None.
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
