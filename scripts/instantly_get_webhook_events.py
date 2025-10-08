"""Download webhook events from Instantly and save to JSONL file.

Usage:

    set -a; source .env-instantly; set +a;python -m scripts.instantly_get_webhook_events

The environment variable `INSTANTLY_API_KEY` must be set.

The script fetches the webhook events of the previous 7 days from the endpoint
https://api.instantly.ai/api/v2/webhook-events for the webhook
https://mailer-automation-c26722b7119c.herokuapp.com/instantly/email_sent and
stores them in instantly_weebhook_events/<timestamp>.jsonl where <timestamp> is
the current timestamp.

Documentation of Instantly webhook events API:
https://developer.instantly.ai/api/v2/webhookevent/listwebhookevent
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List

import requests


INSTANTLY_API_ENDPOINT = "https://api.instantly.ai/api/v2/webhook-events"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "instantly_weebhook_events"
REQUEST_TIMEOUT_SECONDS = 30


def fetch_webhook_events(
    api_key: str,
    start_date: date,
    end_date: date,
    limit: int = 100,
) -> List[dict]:
    """Fetch Instantly webhook events within the given date window."""

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    params = {
        "limit": limit,
        "from": start_date.strftime("%Y-%m-%d"),
        "to": end_date.strftime("%Y-%m-%d"),
    }

    events: List[dict] = []
    starting_after: str | None = None

    with requests.Session() as session:
        while True:
            if starting_after:
                params["starting_after"] = starting_after
            else:
                params.pop("starting_after", None)

            try:
                response = session.get(
                    INSTANTLY_API_ENDPOINT,
                    headers=headers,
                    params=params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as exc:
                raise RuntimeError(f"Failed to fetch webhook events: {exc}") from exc

            payload = response.json()
            items = payload.get("items", [])
            if not isinstance(items, list):
                raise RuntimeError(
                    "Unexpected response structure: 'items' is not a list"
                )

            events.extend(items)
            starting_after = payload.get("next_starting_after")

            if not starting_after:
                break

    return events


def write_events_to_jsonl(events: Iterable[dict], destination: Path) -> None:
    """Persist webhook events to a JSONL file."""

    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False))
            handle.write("\n")


def main() -> None:
    """Entry point for downloading Instantly webhook events."""

    api_key = os.environ.get("INSTANTLY_API_KEY")
    if not api_key:
        print("INSTANTLY_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    now = datetime.now(timezone.utc)
    end_date = now.date()
    start_date = end_date - timedelta(days=7)

    try:
        events = fetch_webhook_events(api_key, start_date, end_date)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    output_path = OUTPUT_DIR / f"{timestamp}.jsonl"

    write_events_to_jsonl(events, output_path)

    print(
        "Fetched {} events between {} and {}.".format(
            len(events),
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
    )
    print(f"Saved events to {output_path}")


if __name__ == "__main__":  # pragma: no cover - script entry point
    main()
