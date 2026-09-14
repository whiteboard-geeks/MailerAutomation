#!/usr/bin/env python
"""
Create missing EasyPost trackers for re-mailed ("recycled") Close leads.

WHY THIS EXISTS
---------------
The Close webhook subscription that drives /easypost/create_tracker
(whsub_0vyDZbaKl3PbrYl9PCWHHI) only fires when the lead's
"EasyPost Tracker ID" field is NULL:

    changed_fields contains <Tracking Number> or <Carrier>
    AND <Tracking Number> is non-null
    AND <Carrier> is non-null
    AND <EasyPost Tracker ID> IS NULL      <-- the bug

A recycled lead still holds the tracker ID from its PREVIOUS mailing, so when a
new tracking number is written the filter evaluates false, no webhook fires, no
tracker is created, and no delivery status will ever update for that piece.

This script repairs the data. The permanent fix is to drop the "is null" gate
from the webhook subscription so a *changed* tracking number always re-tracks.

WHAT IT DOES (per lead)
-----------------------
  * no tracking number or no carrier      -> skipped, reported
  * stored tracker already covers the
    lead's CURRENT tracking code          -> nothing to do
  * current code already has some tracker -> no EasyPost write; Close pointer
                                             repaired only if it is stale
  * current code has no tracker           -> create tracker, write its ID back

IMPORTANT -- why the stored-ID check comes first
------------------------------------------------
EasyPost's GET /v2/trackers?tracking_code=... index does NOT return older
trackers (observed: nothing from 2025 is indexed, though GET by tracker ID
still resolves them fine). Trusting that index alone yields a FALSE NEGATIVE on
any previously-mailed piece, which would create a duplicate tracker on an
already-delivered package and re-fire the delivery webhook -- stamping bogus
fresh "Mailer Delivered" data onto the lead.

So we resolve the lead's stored tracker BY ID first and compare its
tracking_code to the lead's current code. Only when they differ (a genuine
re-mail) do we consult the index and consider creating anything.

Idempotent: re-running never produces duplicate trackers.

Defaults to dry-run. Pass --apply to write.

Usage:
  python -m scripts.backfill_recycle_trackers                          # dry-run, all recycle waves
  python -m scripts.backfill_recycle_trackers --wave "1 (MTP_Big)"     # one wave
  python -m scripts.backfill_recycle_trackers --limit 5                # first 5 only
  python -m scripts.backfill_recycle_trackers --apply                  # write
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from close_utils import make_close_request  # noqa: E402
from utils.easypost import get_easypost_client  # noqa: E402

# --- Close custom field ids -------------------------------------------------
CF_TRACKING_NUMBER = "custom.cf_iSOPYKzS9IPK20gJ8eH9Q74NT7grCQW9psqo4lZR3Ii"
CF_CARRIER = "custom.cf_2QQR5e6vJUyGzlYBtHddFpdqNp5393nEnUiZk1Ukl9l"
CF_TRACKER_ID = "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
CF_RECYCLE_WAVE = "custom.cf_qaz123fktKLBppHO63uvG4vJP34go62fqxxLyZHJuf3"

RECYCLE_WAVES = ["1 (MTP_Big)", "1 (MTP_BigVid)", "1 (MTP_Letters)"]

FIELDS = ",".join(
    ["id", "display_name", CF_TRACKING_NUMBER, CF_CARRIER, CF_TRACKER_ID, CF_RECYCLE_WAVE]
)


def fetch_leads(wave: str) -> list[dict]:
    """Page through every Close lead in the given Recycle Wave."""
    leads: list[dict] = []
    skip = 0
    while True:
        response = make_close_request(
            "get",
            "https://api.close.com/api/v1/lead/",
            params={
                "query": f'{CF_RECYCLE_WAVE}:"{wave}"',
                "_fields": FIELDS,
                "_limit": 200,
                "_skip": skip,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"Close lead search failed: {response.status_code} {response.text[:300]}")
        payload = response.json()
        leads.extend(payload.get("data", []))
        if not payload.get("has_more"):
            return leads
        skip += 200
        time.sleep(0.2)


def get_tracker_by_id(client, tracker_id: str):
    """Resolve a tracker by its ID. Reliable for trackers of any age."""
    try:
        return client.tracker.retrieve(tracker_id)
    except Exception:
        return None


def find_existing_tracker(client, tracking_code: str):
    """Return an indexed EasyPost tracker for this code, or None.

    NOTE: the index omits older trackers, so None here means only "no RECENT
    tracker". Never call this without first ruling out the lead's stored
    tracker via get_tracker_by_id().
    """
    page = client.tracker.all(tracking_code=tracking_code, page_size=5)
    trackers = getattr(page, "trackers", None) or []
    for tracker in trackers:
        if str(tracker.tracking_code) == str(tracking_code):
            return tracker
    return None


def write_tracker_id(lead_id: str, tracker_id: str) -> None:
    response = make_close_request(
        "put",
        f"https://api.close.com/api/v1/lead/{lead_id}",
        json={CF_TRACKER_ID: tracker_id},
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Close update failed for {lead_id}: {response.status_code} {response.text[:300]}"
        )
    body = response.json()
    if body.get(CF_TRACKER_ID) != tracker_id:
        raise RuntimeError(f"Close did not persist tracker id for lead {lead_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--wave", action="append", help="Recycle Wave value (repeatable). Default: all.")
    parser.add_argument("--limit", type=int, help="Only process the first N actionable leads.")
    parser.add_argument("--apply", action="store_true", help="Actually write to EasyPost and Close.")
    args = parser.parse_args()

    waves = args.wave or RECYCLE_WAVES
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] recycle waves: {waves}\n")

    client = get_easypost_client()
    tally: Counter[str] = Counter()
    actioned = 0

    for wave in waves:
        leads = fetch_leads(wave)
        print(f"=== {wave}: {len(leads)} leads ===")

        for lead in leads:
            lead_id = lead["id"]
            tracking_code = str(lead.get(CF_TRACKING_NUMBER) or "").strip()
            carrier_raw = lead.get(CF_CARRIER)
            carrier = carrier_raw[0] if isinstance(carrier_raw, list) and carrier_raw else carrier_raw
            stored_tracker_id = lead.get(CF_TRACKER_ID)

            if not tracking_code or not carrier:
                tally["skipped_missing_tracking_or_carrier"] += 1
                continue

            if args.limit is not None and actioned >= args.limit:
                tally["not_reached_limit"] += 1
                continue

            # 1. Does the lead's STORED tracker already cover the current code?
            #    Authoritative at any age; guards against the index false negative.
            if stored_tracker_id:
                stored = get_tracker_by_id(client, stored_tracker_id)
                if stored is not None and str(stored.tracking_code) == tracking_code:
                    tally["already_correct"] += 1
                    continue

            # 2. Genuine re-mail (or never tracked): consult the index.
            try:
                existing = find_existing_tracker(client, tracking_code)
            except Exception as exc:
                tally["easypost_lookup_failed"] += 1
                print(f"  !! {lead_id} {tracking_code}: lookup failed: {exc}")
                continue

            if existing is not None:
                if stored_tracker_id == existing.id:
                    tally["already_correct"] += 1
                    continue
                tally["pointer_repaired"] += 1
                actioned += 1
                print(f"  ~  {lead_id} {tracking_code}: tracker {existing.id} exists; Close pointer stale ({stored_tracker_id})")
                if args.apply:
                    write_tracker_id(lead_id, existing.id)
                continue

            tally["tracker_created"] += 1
            actioned += 1
            print(f"  +  {lead_id} {tracking_code} ({carrier}): no tracker -> create  [stale id: {stored_tracker_id}]")
            if args.apply:
                try:
                    tracker = client.tracker.create(tracking_code=tracking_code, carrier=carrier)
                except Exception as exc:
                    tally["tracker_create_failed"] += 1
                    tally["tracker_created"] -= 1
                    print(f"  !! {lead_id} {tracking_code}: create failed: {exc}")
                    continue
                write_tracker_id(lead_id, tracker.id)
                time.sleep(0.2)

    print("\n=== summary ===")
    for key, value in sorted(tally.items()):
        print(f"  {key:38s} {value}")
    if not args.apply:
        print("\nDry run only. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
