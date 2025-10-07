# =============================
# FILE: worker.py  (FastDTF worker – minute-by-minute Live Status)
# =============================
import os
import time
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# Optional Twilio (WhatsApp alerts)
try:
    from twilio.rest import Client as TwilioClient  # type: ignore
except Exception:  # pragma: no cover
    TwilioClient = None

# --- Import DVSA client + typed errors ---
try:
    from dvsa_client import (
        DVSAClient,
        DVSAError,
        CaptchaDetected,
        ServiceClosed,
        LayoutIssue,
        WAFBlocked,
    )
except ImportError:  # pragma: no cover
    from dvsa_client import DVSAClient, DVSAError, CaptchaDetected, ServiceClosed, WAFBlocked  # type: ignore

    class LayoutIssue(DVSAError):
        pass

# ---------- ENV ----------
API_BASE = os.environ.get("API_BASE", "http://localhost:8000/api").rstrip("/")
WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
STATE_DB = os.environ.get("SEARCHES_DB", "searches.db")

AUTOBOOK_ENABLED = os.environ.get("AUTOBOOK_ENABLED", "true").lower() == "true"
AUTOBOOK_MODE = os.environ.get("AUTOBOOK_MODE", "simulate")  # simulate | real
HONOUR_CLIENT_RPS = os.environ.get("HONOUR_CLIENT_RPS", "true").lower() == "true"
DVSA_RPS = float(os.environ.get("DVSA_RPS", "0.5"))
DVSA_RPS_JITTER = float(os.environ.get("DVSA_RPS_JITTER", "0.35"))

# Optional WhatsApp alerts
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.environ.get("TWILIO_WHATSAPP_FROM", "")
WHATSAPP_OWNER_TO = os.environ.get("WHATSAPP_OWNER_TO", "")

RESUME_URL = os.environ.get("RESUME_URL", f"{API_BASE}/worker/resume")
QUIET_HOURS = os.environ.get("QUIET_HOURS", "")  # e.g., "00-06"

# Anywhere scanning (NEW jobs only)
ALL_CENTRES = [s.strip() for s in os.environ.get("ALL_CENTRES", "").split(",") if s.strip()]
SCAN_ANYWHERE_FIRST_DEFAULT = os.environ.get("SCAN_ANYWHERE_FIRST_DEFAULT", "true").lower() == "true"

# Operational
WORKER_POLL_SEC = int(os.environ.get("WORKER_POLL_SEC", "30"))
WORKER_CONCURRENCY = int(os.environ.get("WORKER_CONCURRENCY", "8"))
JOB_MAX_PARALLEL_CHECKS = int(os.environ.get("JOB_MAX_PARALLEL_CHECKS", "4"))
HEARTBEAT_SEC = int(os.environ.get("LIVE_HEARTBEAT_SEC", "60"))  # 1‑minute live status updates

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")
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
        return h >= s or h < e
    except Exception:
        return False


def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "User-Agent": "FastDTF-Worker/2025-10-07",
    }


def _post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=60)


def _get(path: str) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.get(url, headers=_headers(), timeout=60)


# ---- Backend wiring ----

def post_event(sid: int, text: str) -> None:
    try:
        _post(f"/worker/searches/{sid}/event", {"event": text})
    except Exception:
        pass


def post_status(sid: int, status: Optional[str] = None, **fields: Any) -> None:
    payload: Dict[str, Any] = {"event": fields.pop("event", "")}
    if status is not None:
        payload["status"] = status
    payload.update(fields)
    try:
        _post(f"/worker/searches/{sid}/status", payload)
    except Exception:
        pass


def live(sid: int, text: str, **fields: Any) -> None:
    """Mirror a live line to both status (for the new Live Status column) and events (existing Last event)."""
    post_status(sid, event=text, **fields)
    post_event(sid, text)


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
    def __init__(self, job: Dict[str, Any]):
        super().__init__(daemon=True)
        self.job = job
        self._stop_flag = threading.Event()
        self._hb_thread: Optional[threading.Thread] = None
        self.dvsa: Optional[DVSAClient] = None

    def stop(self) -> None:
        self._stop_flag.set()

    # --- heartbeat: every minute push a live line ---
    def _heartbeat(self, sid: int) -> None:
        while not self._stop_flag.wait(HEARTBEAT_SEC):
            stage = getattr(self.dvsa, "current_stage", "idle") if self.dvsa else "idle"
            centre = getattr(self.dvsa, "current_centre", "") if self.dvsa else ""
            live(sid, f"live: {stage}", last_centre=centre)

    def run(self) -> None:  # noqa: C901
        sid = int(self.job.get("id"))
        booking_type = self.job.get("booking_type")  # "swap" | "new"
        user_centres: List[str] = self.job.get("centres", []) or []
        licence = self.job.get("licence_number")
        ref = self.job.get("booking_reference")
        theory_pass = self.job.get("theory_pass")

        # Scan set: NEW may use ALL_CENTRES; SWAP strictly user centres only.
        centres = user_centres[:]
        if booking_type == "new" and ALL_CENTRES and SCAN_ANYWHERE_FIRST_DEFAULT:
            centres = ALL_CENTRES + [c for c in user_centres if c not in ALL_CENTRES]

        log.info("job %s start booking_type=%s centres(first5)=%s", sid, booking_type, centres[:5])
        # Show the applicant's centres in Admin (Centres column comes from DB; we do not modify it).
        live(sid, f"worker_claimed:{len(centres)}", last_centre=(centres[0] if centres else ""))

        # Boot DVSA client and attach SID so it can emit rich status
        self.dvsa = DVSAClient(honour_client_rps=HONOUR_CLIENT_RPS, dvsa_rps=DVSA_RPS, dvsa_rps_jitter=DVSA_RPS_JITTER)
        try:
            self.dvsa.attach_job(sid)
        except Exception:
            pass

        # Start heartbeat
        self._hb_thread = threading.Thread(target=self._heartbeat, args=(sid,), daemon=True, name=f"hb-{sid}")
        self._hb_thread.start()

        try:
            # Iterate centres (optionally limited per tick)
            scan_iter = centres[:JOB_MAX_PARALLEL_CHECKS] if JOB_MAX_PARALLEL_CHECKS > 0 else centres
            for centre in scan_iter:
                live(sid, f"step:login_start | centre:{centre}", last_centre=centre)
                try:
                    if booking_type == "swap":
                        self.dvsa.login_swap(licence, ref)
                    else:
                        self.dvsa.login_new(licence, theory_pass)
                    live(sid, f"step:login_ok | centre:{centre}", last_centre=centre)
                except LayoutIssue:
                    live(sid, f"layout_issue:{centre}", last_centre=centre)
                    continue

                # --- Search slots ---
                live(sid, f"step:open_centre | centre:{centre}", last_centre=centre)
                slots = self.dvsa.search_centre_slots(centre)
                if not slots:
                    live(sid, f"checked:{centre} · no_slots", last_centre=centre)
                    continue

                # Earliest slot heuristic
                try:
                    slots.sort(key=lambda s: (s.get("date","zz"), s.get("time","zz")))
                except Exception:
                    pass
                slot = slots[0]
                live(sid, f"slot_found:{centre} · {slot.get('date','?')} · {slot.get('time','?')}", last_centre=centre)

                if not AUTOBOOK_ENABLED:
                    live(sid, f"found_only:{centre} · {slot.get('date','?')} {slot.get('time','?')}", last_centre=centre)
                    break

                ok = self.dvsa.swap_to(slot) if booking_type == "swap" else self.dvsa.book_and_pay(slot, mode=AUTOBOOK_MODE)
                if ok:
                    post_status(sid, status="booked", event=f"booked:{centre} · {slot.get('date','?')} {slot.get('time','?')}", last_centre=centre)
                    post_event(sid, f"booked:{centre} · {slot.get('date','?')} {slot.get('time','?')}")
                    if booking_type == "new":
                        try:
                            _post("/upgrade-to-swap", {"sid": sid})
                        except Exception:
                            pass
                    return
                else:
                    live(sid, f"booking_failed:{centre} · {slot.get('date','?')} {slot.get('time','?')}", last_centre=centre)

        except CaptchaDetected as e:
            live(sid, f"captcha_cooldown:{getattr(e, 'stage', 'unknown')}", last_centre=getattr(self.dvsa, 'current_centre', ''))
            _notify_whatsapp(f"CAPTCHA hit. Tap to resume: {RESUME_URL}")
        except WAFBlocked as e:
            live(sid, f"ip_blocked:{getattr(e, 'stage', 'unknown')}")
        except ServiceClosed as e:
            live(sid, f"service_closed:{getattr(e, 'stage', 'unknown')}")
        except DVSAError as e:
            live(sid, f"error:{type(e).__name__}:{str(e)[:140]}")
        except Exception as e:  # pragma: no cover
            live(sid, f"unexpected:{type(e).__name__}:{str(e)[:140]}")
        finally:
            try:
                self.stop()
                if self._hb_thread:
                    self._hb_thread.join(timeout=1)
            except Exception:
                pass
            try:
                if self.dvsa:
                    self.dvsa.close()
            except Exception:
                pass


class ClaimLoop:
    def __init__(self) -> None:
        self.stop_flag = False
        self.sem = threading.Semaphore(WORKER_CONCURRENCY)

    def _claim(self) -> List[Dict[str, Any]]:
        try:
            r = _post("/worker/claim", {"limit": WORKER_CONCURRENCY})
            if r.status_code == 401:
                log.error("Unauthorized (bad WORKER_TOKEN)")
                return []
            r.raise_for_status()
            data = r.json()
            return data.get("jobs") or data.get("items", [])
        except Exception as e:
            log.warning("claim error: %s", e)
            return []

    def loop(self) -> None:
        client_rps = "ON" if HONOUR_CLIENT_RPS else f"OFF({DVSA_RPS}/s±{DVSA_RPS_JITTER})"
        log.info(
            "[worker] up state_db=%s poll=%ss client_rps=%s autobook=%s mode=%s",
            STATE_DB,
            WORKER_POLL_SEC,
            client_rps,
            AUTOBOOK_ENABLED,
            AUTOBOOK_MODE,
        )

        while not self.stop_flag:
            try:
                cr = _get("/worker/controls")
                pause = False
                if cr.ok:
                    c = cr.json()
                    pause = bool(c.get("pause") or c.get("pause_all"))
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
                    t = JobRunner(j)
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join()
                    self.sem.release()

            except KeyboardInterrupt:
                self.stop_flag = True
            except Exception as e:  # pragma: no cover
                log.warning("loop error: %s", e)
                time.sleep(WORKER_POLL_SEC)


if __name__ == "__main__":
    ClaimLoop().loop()
