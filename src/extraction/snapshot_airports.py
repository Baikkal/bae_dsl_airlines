# snapshot_airports.py
"""Collects full-day snapshots of FlightRadar24 schedules (arrivals & departures)
for the three Paris hubs: CDG, ORY and BVA.

Features
--------
* Iterates over *every* pagination page (100 records/page) without missing flights.
* Saves each airport snapshot as **raw JSON** with a UTC timestamp for replay.
* Designed for long‑lived loops (default 15 min) or cron/Airflow.

Usage
-----
$ pip install brotli requests  # dépendances du wrapper
$ python src/extraction/snapshot_airports.py
"""
from __future__ import annotations

import sys
from pathlib import Path
import json
from datetime import datetime, timezone
from typing import List

# ----------------------------------------------------
# 1) Vendor path setup (wrapper local dans libs)
# ----------------------------------------------------
BASE_DIR       = Path(__file__).resolve().parents[2]  # remonte jusqu'à bae_dsl_airlines
API_PYTHON_DIR = BASE_DIR / "libs" / "FlightRadarAPI" / "python"
sys.path.insert(0, str(API_PYTHON_DIR))

# 2) Import local wrapper
from FlightRadar24.api import FlightRadar24API

# ----------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------
AIRPORTS: List[str]          = ["CDG", "ORY", "BVA"]
OUTPUT_DIR: Path             = Path("snapshots")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------
# Client instantiation
# ----------------------------------------------------
api = FlightRadar24API()


def _collect_schedule_pages(iata: str) -> List[dict]:
    """Return all pagination pages for an airport (both arrivals & departures)."""
    pages: List[dict] = []
    page_no = 1

    while True:
        # Seul 'iata' et 'page' sont acceptés par get_airport_details
        details = api.get_airport_details(iata, page=page_no)
        pages.append(details)

        # On lit la pagination depuis l'une des directions (arrivals)
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
    # Exécution unique : capture de tous les hubs, puis sortie
    for airport in AIRPORTS:
        try:
            snapshot_airport(airport)
        except Exception as exc:
            print(f"[WARN] {airport}: {exc}")
    print("Snapshot quotidien terminé.")
    # Fin du programme
    sys.exit(0)

