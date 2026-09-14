#!/usr/bin/env python
"""
Create the missing 'Mailer Delivered' custom activities for re-mailed leads.

WHY THIS EXISTS
---------------
utils.easypost.create_package_delivered_custom_activity_in_close used to skip
creation whenever the lead had ANY 'Mailer Delivered' activity, at any date. A
recycled lead still carries the activity from its previous mailing, so the new
delivery updated the lead's custom fields but never produced an activity -- and
Close auto-enrollment keys off the activity, not the fields.

The dedupe is now scoped to the delivery date. This script repairs the leads
that were delivered while the old behaviour was live.

WHAT IT DOES (per lead)
-----------------------
  * lead has no Date Delivered on/after --since  -> skipped
  * activity already exists for that date        -> skipped (handled by the
                                                    same dedupe the webhook uses)
  * otherwise                                    -> create the activity from the
                                                    lead's own delivery fields

Idempotent: creation goes through the same date-scoped duplicate check as the
live webhook path, so re-running cannot double-post.

TIMELINE PLACEMENT
------------------
Close stamps activity_at at creation time, which for a backfill is "now" -- so
every repaired activity piles up under today instead of sitting on the delivery
date, which is where anyone looking at the lead expects it. activity_at is
writable, so after creating we set it to the real delivery moment (the EasyPost
delivered scan when resolvable, otherwise midday on the delivery date).

Defaults to dry-run. Pass --apply to write.

Usage:
  python -m scripts.backfill_recycle_delivered_activities
  python -m scripts.backfill_recycle_delivered_activities --wave "1 (MTP_Big)"
  python -m scripts.backfill_recycle_delivered_activities --limit 5
  python -m scripts.backfill_recycle_delivered_activities --apply
  python -m scripts.backfill_recycle_delivered_activities --fix-timestamps --apply
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from close_utils import make_close_request  # noqa: E402
from utils.easypost import (  # noqa: E402
    ACTIVITY_DATE_DELIVERED_FIELD,
    MAILER_DELIVERED_ACTIVITY_TYPE_ID,
    create_package_delivered_custom_activity_in_close,
    get_easypost_client,
)

CF_TRACKER_ID = "custom.cf_JsirGUJdp8RrCI6XwW48xFKEccSwulSCwZ7pAZL84vh"
CF_RECYCLE_WAVE = "custom.cf_qaz123fktKLBppHO63uvG4vJP34go62fqxxLyZHJuf3"
CF_DATE_DELIVERED = "custom.cf_jVU4YFLX5bDq2dRjvBapPYAJxGP0iQWid9QC7cQjSCR"
CF_DATE_AND_LOCATION = "custom.cf_DTgmXXPozUH3707H1MYu2PhhDznJjWbtmDcb7zme5a9"
CF_CITY = "custom.cf_1hWUFxiA6QhUXrYT3lDh96JSWKxVBBAKCB3XO8EXGUW"
CF_STATE = "custom.cf_vxfsYfTrFk6oYrnSx0ViYrUMpE7y5sxi0NnRgTyOf30"
CF_DATE_READABLE = "custom.cf_jGC3O9doWfvwFV49NBIRGwA0PFIcKMzE0h8Zf65XLCQ"
CF_LOCATION = "custom.cf_hPAtbaFuztYBQcYVqsk4pIFV0hKvnlb696TknlzEERS"

RECYCLE_WAVES = ["1 (MTP_Big)", "1 (MTP_BigVid)", "1 (MTP_Letters)"]
DEFAULT_SINCE = "2026-09-08"

FIELDS = ",".join(
    [
        "id",
        "display_name",
        CF_TRACKER_ID,
        CF_DATE_DELIVERED,
        CF_DATE_AND_LOCATION,
        CF_CITY,
        CF_STATE,
        CF_DATE_READABLE,
        CF_LOCATION,
    ]
)


def fetch_delivered_leads(wave: str, since: str) -> list[dict]:
    leads: list[dict] = []
    skip = 0
    while True:
        response = make_close_request(
            "get",
            "https://api.close.com/api/v1/lead/",
            params={
                "query": f'{CF_RECYCLE_WAVE}:"{wave}" {CF_DATE_DELIVERED}>="{since}"',
                "_fields": FIELDS,
                "_limit": 200,
                "_skip": skip,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"Close search failed: {response.status_code} {response.text[:300]}")
        payload = response.json()
        leads.extend(payload.get("data", []))
        if not payload.get("has_more"):
            return leads
        skip += 200
        time.sleep(0.3)


def parse_date(value) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def build_delivery_information(lead: dict) -> dict | None:
    """Rebuild the activity payload from the lead's own delivery fields."""
    delivered_on = parse_date(lead.get(CF_DATE_DELIVERED))
    if delivered_on is None:
        return None

    city = (lead.get(CF_CITY) or "N/A").strip()
    state = (lead.get(CF_STATE) or "N/A").strip()
    readable = lead.get(CF_DATE_READABLE) or delivered_on.strftime("%a %-m/%-d")
    location = lead.get(CF_LOCATION) or f"{city}, {state}"
    date_and_location = lead.get(CF_DATE_AND_LOCATION) or f"{readable} to {location}"

    return {
        "delivery_city": city,
        "delivery_state": state,
        "delivery_date": delivered_on,
        "delivery_date_readable": readable,
        "location_delivered": location,
        "date_and_location_of_mailer_delivered": date_and_location,
    }


def delivered_at(lead: dict, delivered_on: date, client) -> str:
    """Best available timestamp for the delivery, as an ISO string."""
    tracker_id = lead.get(CF_TRACKER_ID)
    if tracker_id and client is not None:
        try:
            tracker = client.tracker.retrieve(tracker_id)
            scans = [
                d
                for d in (getattr(tracker, "tracking_details", None) or [])
                if getattr(d, "status", None) == "delivered"
            ]
            if scans:
                stamp = str(getattr(scans[-1], "datetime", "") or "")
                if stamp:
                    return stamp.replace("Z", "+00:00")
        except Exception:
            pass
    # Fall back to midday UTC on the delivery date: keeps it on the right day
    # in every US timezone.
    return f"{delivered_on.isoformat()}T17:00:00+00:00"


def set_activity_at(activity_id: str, when: str) -> None:
    response = make_close_request(
        "put",
        f"https://api.close.com/api/v1/activity/custom/{activity_id}/",
        json={"activity_at": when},
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"activity_at update failed for {activity_id}: "
            f"{response.status_code} {response.text[:200]}"
        )


def find_activity_for_date(lead_id: str, delivered_on: date) -> dict | None:
    response = make_close_request(
        "get",
        "https://api.close.com/api/v1/activity/custom/",
        params={
            "lead_id": lead_id,
            "custom_activity_type_id": MAILER_DELIVERED_ACTIVITY_TYPE_ID,
            "_limit": 20,
        },
    )
    if response.status_code != 200:
        return None
    wanted = delivered_on.isoformat()
    for activity in response.json().get("data", []):
        if str(activity.get(ACTIVITY_DATE_DELIVERED_FIELD) or "")[:10] == wanted:
            return activity
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--wave", action="append", help="Recycle Wave value (repeatable).")
    parser.add_argument("--since", default=DEFAULT_SINCE, help=f"Delivered on/after (default {DEFAULT_SINCE}).")
    parser.add_argument("--limit", type=int, help="Only process the first N actionable leads.")
    parser.add_argument("--apply", action="store_true", help="Actually create activities.")
    parser.add_argument(
        "--fix-timestamps",
        action="store_true",
        help="Repair activity_at on activities that already exist (no creation).",
    )
    args = parser.parse_args()

    waves = args.wave or RECYCLE_WAVES
    print(f"[{'APPLY' if args.apply else 'DRY-RUN'}] waves={waves} delivered since {args.since}\n")

    tally: Counter[str] = Counter()
    actioned = 0
    client = get_easypost_client()

    for wave in waves:
        leads = fetch_delivered_leads(wave, args.since)
        print(f"=== {wave}: {len(leads)} delivered leads ===")

        for lead in leads:
            info = build_delivery_information(lead)
            if info is None:
                tally["skipped_no_delivery_date"] += 1
                continue

            if args.limit is not None and actioned >= args.limit:
                tally["not_reached_limit"] += 1
                continue

            actioned += 1
            delivered_on = info["delivery_date"]
            label = f"{str(lead.get('display_name'))[:44]:44s} {delivered_on}"

            # --- repair-only mode: fix activity_at on what already exists
            if args.fix_timestamps:
                existing = find_activity_for_date(lead["id"], delivered_on)
                if existing is None:
                    tally["no_activity_to_fix"] += 1
                    continue
                when = delivered_at(lead, delivered_on, client)
                if str(existing.get("activity_at"))[:10] == when[:10]:
                    tally["timestamp_already_correct"] += 1
                    continue
                if not args.apply:
                    tally["would_fix_timestamp"] += 1
                    print(f"  ~  {label}  {existing.get('activity_at')} -> {when}")
                    continue
                try:
                    set_activity_at(existing["id"], when)
                    tally["timestamp_fixed"] += 1
                except Exception as exc:
                    tally["failed"] += 1
                    print(f"  !! {label}: {exc}")
                time.sleep(0.2)
                continue

            if not args.apply:
                tally["would_create_or_skip"] += 1
                print(f"  +  {label}  {info['location_delivered']}")
                continue

            try:
                result = create_package_delivered_custom_activity_in_close(lead["id"], info)
            except Exception as exc:
                tally["failed"] += 1
                print(f"  !! {label}: {exc}")
                continue

            if isinstance(result, dict) and result.get("status") == "skipped":
                tally["already_had_activity"] += 1
            else:
                tally["created"] += 1
                print(f"  +  {label}  {info['location_delivered']}")
                activity_id = result.get("id") if isinstance(result, dict) else None
                if activity_id:
                    try:
                        set_activity_at(activity_id, delivered_at(lead, delivered_on, client))
                    except Exception as exc:
                        tally["created_but_timestamp_failed"] += 1
                        print(f"  !! {label}: activity_at: {exc}")
            time.sleep(0.25)

    print("\n=== summary ===")
    for key, value in sorted(tally.items()):
        print(f"  {key:30s} {value}")
    if not args.apply:
        print("\nDry run only. Re-run with --apply to write.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
