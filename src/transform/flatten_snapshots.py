# src/transform/flatten_snapshots.py
"""
Flatten all JSON snapshots into a single CSV for easy ingestion.

Usage:
  1. Place this script in src/transform/.
  2. Activate your virtual environment.
  3. Run: python src/transform/flatten_snapshots.py
"""
import json
from pathlib import Path
import pandas as pd

# ----------------------------------------------------
# Path setup: locate project root, snapshots folder, and data output folder
# ----------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]  # remonte à bae_dsl_airlines
SNAPSHOT_DIR = PROJECT_ROOT / "snapshots"
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

if not SNAPSHOT_DIR.exists():
    raise FileNotFoundError(f"Snapshots directory not found: {SNAPSHOT_DIR}")

all_records = []

# Iterate each airport subfolder
for airport_dir in SNAPSHOT_DIR.iterdir():
    if not airport_dir.is_dir():
        continue
    airport = airport_dir.name
    # Iterate each JSON page file
    for json_file in airport_dir.glob(f"{airport.lower()}_snapshot_p*_*.json"):
        # Example filename: cdg_snapshot_p1_20240501_002749.json
        # We want the full YYYYMMDD_HHMMSS timestamp from the stem
        parts = json_file.stem.split("_")
        ts = "_".join(parts[-2:])
        data = json.loads(json_file.read_text(encoding="utf-8"))
        arrivals = data["airport"]["pluginData"]["schedule"]["arrivals"]["data"]
        departures = data["airport"]["pluginData"]["schedule"]["departures"]["data"]
        for flight in arrivals + departures:
            record = {"snapshot_ts": ts, "airport": airport}
            record.update(flight)
            all_records.append(record)

# Build DataFrame
df = pd.DataFrame(all_records)

# Save to CSV in the data directory
output_csv = DATA_DIR / "combined_snapshots.csv"
df.to_csv(output_csv, index=False)
print(f"Combined snapshots CSV saved to: {output_csv}")

# Preview first 10 rows
print(df.head(10))
