# src/extraction/get_airlines.py
from pathlib import Path
import pandas as pd
from FlightRadar24.api import FlightRadar24API

def main():
    # 1) Instanciation du client
    api = FlightRadar24API()

    # 2) Récupération de la liste des airlines
    print("Fetching list of airlines…")
    airlines = api.get_airlines() or []

    # 3) Construction de la liste de dicts
    records = []
    for a in airlines:
        records.append({
            "iata": getattr(a, "Code", None),
            "icao": getattr(a, "ICAO", None),
            "name": getattr(a, "Name", None),
        })

    # 4) DataFrame + renommage
    df = pd.DataFrame.from_records(records)
    df = df.rename(columns={"Code": "iata", "ICAO": "icao", "Name": "name"})

    # 5) Écriture dans data/airlines.csv
    output_dir = Path(__file__).resolve().parents[2] / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_file = output_dir / "airlines.csv"

    df.to_csv(out_file, index=False, encoding="utf-8")
    print(f"✅ {len(df)} airlines exported to {out_file}")

if __name__ == "__main__":
    main()
