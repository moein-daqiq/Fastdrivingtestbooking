import os
import time
import json
import random
import sqlite3
import requests
from datetime import datetime

DB_FILE = os.environ.get("SEARCHES_DB", "searches.db")
POLL_SEC = int(os.environ.get("WORKER_POLL_SEC", "45"))
NOTIFY_WEBHOOK_URL = os.environ.get("NOTIFY_WEBHOOK_URL", "")  # optional

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def get_conn():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def notify(event: str, payload: dict):
    if NOTIFY_WEBHOOK_URL:
        try:
            requests.post(NOTIFY_WEBHOOK_URL, json={"event": event, "data": payload}, timeout=8)
        except Exception as e:
            print(f"[notify] failed: {e}")
    else:
        print(f"[notify] {event}: {json.dumps(payload, ensure_ascii=False)}")

def claim_one():
    """
    Atomically claim one paid job in 'queued' or 'new' state by flipping to 'searching'.
    Returns the row or None.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Pick a candidate
    cur.execute("""
        SELECT * FROM searches
         WHERE paid = 1 AND status IN ('queued','new')
         ORDER BY updated_at ASC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    # Try to claim it
    cur.execute("""
        UPDATE searches
           SET status='searching', last_event='worker_claimed', updated_at=?
         WHERE id=? AND status IN ('queued','new') AND paid=1
    """, (now_iso(), row["id"]))
    conn.commit()

    # Verify claim succeeded
    cur.execute("SELECT * FROM searches WHERE id=?", (row["id"],))
    claimed = cur.fetchone()
    conn.close()
    if claimed and claimed["status"] == "searching":
        return claimed
    return None

def simulate_search(row):
    """
    Simulate the search: iterate centres and decide outcome.
    Deterministic outcome by id (demo-friendly).
    """
    sid = row["id"]
    centres = json.loads(row["centres_json"] or "[]")
    if not centres:
        centres = ["(no centre provided)"]

    # Fake work
    for idx, c in enumerate(centres, start=1):
        time.sleep(1)  # very short per-centre delay to show progress
        touch_last_event(sid, f"checked_{idx}:{c}")

    # Deterministic outcome (id % 3): 0=failed, 1=found, 2=booked
    mode = sid % 3
    if mode == 0:
        set_status(sid, "failed", "no_slots_found")
    elif mode == 1:
        set_status(sid, "found", f"slot_found_at:{centres[0]}")
    else:
        set_status(sid, "booked", f"booked_at:{centres[0]}")

def touch_last_event(sid: int, event: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches SET last_event=?, updated_at=? WHERE id=?
    """, (event, now_iso(), sid))
    conn.commit()
    conn.close()

def set_status(sid: int, status: str, event: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE searches SET status=?, last_event=?, updated_at=? WHERE id=?
    """, (status, event, now_iso(), sid))
    conn.commit()
    cur.execute("SELECT * FROM searches WHERE id=?", (sid,))
    row = cur.fetchone()
    conn.close()

    notify("search.status", {
        "id": sid,
        "status": status,
        "last_event": event,
        "booking_type": row["booking_type"],
        "phone": row["phone"],
        "email": row["email"],
        "centres": json.loads(row["centres_json"] or "[]")
    })

def main():
    print(f"[worker] starting. DB={DB_FILE} poll={POLL_SEC}s webhook={'on' if NOTIFY_WEBHOOK_URL else 'off'}")
    # ensure requests exists even if no webhook
    while True:
        try:
            job = claim_one()
            if job:
                print(f"[worker] claimed search #{job['id']}")
                simulate_search(job)
            else:
                # no work this tick
                pass
        except Exception as e:
            print(f"[worker] error: {e}")
        time.sleep(POLL_SEC)

if __name__ == "__main__":
    # Ensure 'requests' import doesn't crash if not installed (optional webhook).
    try:
        import requests as _r  # noqa
    except Exception:
        print("[worker] 'requests' not installed; NOTIFY_WEBHOOK_URL will be ignored.")
    main()
