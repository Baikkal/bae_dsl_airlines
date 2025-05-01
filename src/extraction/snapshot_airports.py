# snapshot_airports.py
"""Collects full-day snapshots of FlightRadar24 schedules (arrivals & departures)
for the three Paris hubs: CDG, ORY and BVA.

Features
--------
* Iterates over *every* pagination page (100 records/page) without missing flights.
* Saves each airport snapshot as **raw JSON** with a UTC timestamp for replay.
* Designed for single daily execution (e.g., via cron or GitHub Actions).

Usage
-----
$ pip install FlightRadarAPI brotli requests  # Dependencies
$ python src/extraction/snapshot_airports.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List

# Import wrapper from PyPI
from FlightRadar24 import FlightRadar24API

# ----------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------
AIRPORTS: List[str] = ["CDG", "ORY", "BVA"]
OUTPUT_DIR: Path    = Path("snapshots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------
# Client instantiation
# ----------------------------------------------------
api = FlightRadar24API()


def _collect_schedule_pages(iata: str) -> List[dict]:
    """Return all pagination pages for an airport (arrivals & departures combined)."""
    pages: List[dict] = []
    page_no = 1
    while True:
        # Only 'iata' and 'page' parameters supported
        details = api.get_airport_details(iata, page=page_no)
        pages.append(details)

        # Read pagination info from arrivals (both streams present)
        page_info = details["airport"]["pluginData"]["schedule"]["arrivals"]["page"]
        current, total = page_info["current"], page_info["total"]
        if current >= total:
            break
        page_no += 1
    return pages


def snapshot_airport(iata: str) -> None:
    """Dump one full snapshot (all pages) for the given airport."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest_dir = OUTPUT_DIR / iata
    dest_dir.mkdir(exist_ok=True)

    pages = _collect_schedule_pages(iata)
    for idx, payload in enumerate(pages, start=1):
        fname = dest_dir / f"{iata.lower()}_snapshot_p{idx}_{ts}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[{iata}] captured {len(pages)} pages at {ts}Z")


def main() -> None:
    """Execute snapshots for all configured airports and exit."""
    for airport in AIRPORTS:
        try:
            snapshot_airport(airport)
        except Exception as exc:
            print(f"[WARN] {airport}: {exc}")
    print("Snapshot quotidien terminé.")
    sys.exit(0)


if __name__ == "__main__":
    main()

