# =============================
# FILE: worker.py  (FastDTF worker – live step-by-step telemetry)
# =============================
import os
import time
import json
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

# Optional Twilio (WhatsApp alerts)
try:
    from twilio.rest import Client as TwilioClient  # type: ignore
except Exception:  # pragma: no cover
    TwilioClient = None

# --- Import DVSA client + typed errors (with safe fallback for LayoutIssue) ---
try:
    from dvsa_client import (
        DVSAClient,
        DVSAError,
        CaptchaDetected,
        ServiceClosed,
        LayoutIssue,
        WAFBlocked,
    )
except ImportError:
    from dvsa_client import DVSAClient, DVSAError, CaptchaDetected, ServiceClosed, WAFBlocked  # type: ignore

    class LayoutIssue(DVSAError):
        pass

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
        return h >= s or h < e
    except Exception:
        return False


def _headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {WORKER_TOKEN}",
        "User-Agent": "FastDTF-Worker/2025-10-06",
    }


def _post(path: str, payload: Dict[str, Any]) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.post(url, headers=_headers(), data=json.dumps(payload), timeout=60)


def _get(path: str) -> requests.Response:
    url = f"{API_BASE}{path}"
    return requests.get(url, headers=_headers(), timeout=60)


# ---- Backend wiring (correct endpoints) ----

def post_event(sid: int, text: str) -> None:
    try:
        _post(f"/worker/searches/{sid}/event", {"event": text})
    except Exception as e:  # pragma: no cover
        log.debug("post_event failed: %s", e)


def post_status(sid: int, status: str | None = None, **fields: Any) -> None:
    payload: Dict[str, Any] = {"event": fields.pop("event", "")}
    if status is not None:
        payload["status"] = status
    payload.update(fields)  # allow last_centre, reached, captcha, etc.
    try:
        _post(f"/worker/searches/{sid}/status", payload)
    except Exception as e:  # pragma: no cover
        log.debug("post_status failed: %s", e)


def _notify_whatsapp(text: str) -> None:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM and WHATSAPP_OWNER_TO and TwilioClient):
        return
    try:
        tc = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        tc.messages.create(
            from_=f"whatsapp:{TWILIO_WHATSAPP_FROM}",
            to=f"whatsapp:{WHATSAPP_OWNER_TO}",
            body=text,
        )
    except Exception as e:  # pragma: no cover
        log.warning("whatsapp send failed: %s", e)


def _centres_summary(centres: List[str], max_names: int = 12) -> str:
    names = [str(c) for c in centres]
    head = ", ".join(names[:max_names])
    tail = f" (+{len(names) - max_names} more)" if len(names) > max_names else ""
    return head + tail


# ---------- Worker Threads ----------
class JobRunner(threading.Thread):
    def __init__(self, job: Dict[str, Any]):
        super().__init__(daemon=True)
        self.job = job

    def run(self) -> None:  # noqa: C901
        sid = int(self.job.get("id"))

        # Fresh DVSA client per thread + attach SID for DVSA-side events
        dvsa = DVSAClient(
            honour_client_rps=HONOUR_CLIENT_RPS,
            dvsa_rps=DVSA_RPS,
            dvsa_rps_jitter=DVSA_RPS_JITTER,
        )
        try:
            dvsa.attach_job(sid)
        except Exception:
            pass

        try:
            centre_list = self.job.get("centres", [])
            booking_type = self.job.get("booking_type")  # "swap" | "new"
            licence = self.job.get("licence_number")
            ref = self.job.get("booking_reference")
            theory_pass = self.job.get("theory_pass")

            centres = centre_list[:]
            if ALL_CENTRES and SCAN_ANYWHERE_FIRST_DEFAULT:
                centres = ALL_CENTRES + [c for c in centre_list if c not in ALL_CENTRES]

            log.info("job %s start booking_type=%s centres=%s", sid, booking_type, centres[:5])
            post_event(sid, f"claimed:{len(centres)} | centres:{_centres_summary(centres)}")
            post_status(sid, event="worker_start", reached=False, last_centre=(centres[0] if centres else ""))

            # Iterate centres (optionally limited per tick)
            for centre in centres[:JOB_MAX_PARALLEL_CHECKS] if JOB_MAX_PARALLEL_CHECKS > 0 else centres:
                post_status(sid, event=f"step:login_start | centre:{centre}", last_centre=centre)
                try:
                    if booking_type == "swap":
                        dvsa.login_swap(licence, ref)
                    else:
                        dvsa.login_new(licence, theory_pass)
                    post_status(sid, event=f"step:login_ok | centre:{centre}", reached=True, last_centre=centre)
                except LayoutIssue:
                    post_status(sid, event=f"layout_issue:{centre}", last_centre=centre)
                    continue

                # --- Search slots ---
                post_status(sid, event=f"step:open_centre | centre:{centre}", last_centre=centre)
                slots = dvsa.search_centre_slots(centre)
                if not slots:
                    post_event(sid, f"checked:{centre} · no_slots")
                    continue

                slots.sort(key=lambda s: (s["date"], s["time"]))
                slot = slots[0]
                post_event(sid, f"slot_found:{centre} · {slot['date']} · {slot['time']}")

                if not AUTOBOOK_ENABLED:
                    post_event(sid, f"found_only:{centre} · {slot['date']} {slot['time']}")
                    break

                ok = dvsa.swap_to(slot) if booking_type == "swap" else dvsa.book_and_pay(slot, mode=AUTOBOOK_MODE)

                if ok:
                    post_status(
                        sid,
                        status="booked",
                        event=f"booked:{centre} · {slot['date']} {slot['time']}",
                        last_centre=centre,
                    )
                    if booking_type == "new":
                        try:
                            _post("/upgrade-to-swap", {"sid": sid})
                        except Exception:
                            pass
                    return
                else:
                    post_event(sid, f"booking_failed:{centre} · {slot['date']} {slot['time']}")
        except CaptchaDetected as e:
            post_status(sid, event=f"captcha_cooldown:{getattr(e, 'stage', 'unknown')}", captcha=True)
            _notify_whatsapp(f"CAPTCHA hit. Tap to resume: {RESUME_URL}")
        except WAFBlocked as e:
            post_status(sid, event=f"ip_blocked:{getattr(e, 'stage', 'unknown')}")
        except ServiceClosed as e:
            post_status(sid, event=f"service_closed:{getattr(e, 'stage', 'unknown')}")
        except DVSAError as e:
            post_event(sid, f"error:{type(e).__name__}:{str(e)[:140]}")
        except Exception as e:  # pragma: no cover
            post_event(sid, f"unexpected:{type(e).__name__}:{str(e)[:140]}")
        finally:
            try:
                dvsa.close()
            except Exception:
                pass


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
            # Support both {jobs:[...]} and {items:[...]}
            jobs = data.get("jobs")
            if jobs is None:
                jobs = data.get("items", [])
            return jobs
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

        while not self.stop:
            try:
                cr = _get("/worker/controls")
                pause = False
                if cr.ok:
                    c = cr.json()
                    pause = bool(c.get("pause", False) or c.get("pause_all", False))
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
                self.stop = True
            except Exception as e:  # pragma: no cover
                log.warning("loop error: %s", e)
                time.sleep(WORKER_POLL_SEC)


if __name__ == "__main__":
    ClaimLoop().loop()


# =============================
