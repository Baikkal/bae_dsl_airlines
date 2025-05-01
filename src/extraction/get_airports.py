# src/extraction/get_airports.py
"""
Extracts the global list of airports via the local FlightRadarAPI vendor,
et écrit le résultat dans data/airports.csv
Usage:
  python src/extraction/get_airports.py
"""

import sys
from pathlib import Path
import pandas as pd

# 1) On ajoute le dossier vendor au PYTHONPATH
BASE_DIR     = Path(__file__).resolve().parents[2]              # …/bae_dsl_airlines
VENDOR_PYDIR = BASE_DIR / "libs" / "FlightRadarAPI" / "python"  # …/libs/FlightRadarAPI/python
sys.path.insert(0, str(VENDOR_PYDIR))

# 2) Import du bon module et de la classe
from FlightRadar24.api import FlightRadar24API

def main():
    # 3) Instanciation du client
    fr_api = FlightRadar24API()

    # 4) Récupération de la liste des aéroports
    print("Fetching list of airports…")
    airports = fr_api.get_airports() or []

    # 5) Construction du DataFrame
    records = []
    for a in airports:
        # chaque objet a des attributs, jamais de .get() sur None
        icao      = getattr(a, "icao", None)
        if not icao:
            continue
        records.append({
            "icao":     icao,
            "iata":     getattr(a, "iata", None),
            "name":     getattr(a, "name", None),
            "city":     getattr(a, "city", None),
            "country":  getattr(a, "country", None),
            "latitude": getattr(a, "latitude", None),
            "longitude": getattr(a, "longitude", None),
            "timezone": getattr(a, "timezone_name", None),
        })

    df = pd.DataFrame(records)

    # 6) Écriture du CSV dans data/airports.csv
    out_dir = BASE_DIR / "data"
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / "airports.csv"
    df.to_csv(out_file, index=False, encoding="utf-8")

    print(f"✅ Airport list saved to {out_file}")

if __name__ == "__main__":
    main()
