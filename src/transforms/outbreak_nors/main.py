"""Transform NORS (National Outbreak Reporting System) dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int
from .test import test

DATASET_ID = "cdc_outbreak_nors"
SOURCE_ID = "5xkq-dg7x"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NORS - National Outbreak Reporting System",
    "description": (
        "National Outbreak Reporting System (NORS) data on foodborne, waterborne, and "
        "other disease outbreaks in the United States since 1971. Includes etiology, "
        "transmission mode, setting, illness counts, and deaths. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year of outbreak",
        "month": "Month of outbreak",
        "state": "State where outbreak occurred",
        "primary_mode": "Primary transmission mode (Food, Water, Person-to-person, etc.)",
        "etiology": "Causative agent (pathogen, chemical, or toxin)",
        "etiology_status": "Confirmation status of etiology",
        "setting": "Setting where outbreak occurred",
        "illnesses": "Number of illnesses reported",
        "deaths": "Number of deaths reported",
        "water_exposure": "Type of water exposure (if applicable)",
        "water_type": "Type of water system (if applicable)",
    },
}


def run():
    """Transform, validate, and upload NORS outbreak data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": parse_int(row.get("year")),
            "month": parse_int(row.get("month")),
            "state": row.get("state"),
            "primary_mode": row.get("primary_mode"),
            "etiology": row.get("etiology"),
            "etiology_status": row.get("etiology_status"),
            "setting": row.get("setting"),
            "illnesses": parse_int(row.get("illnesses")),
            "deaths": parse_int(row.get("deaths")),
            "water_exposure": row.get("water_exposure"),
            "water_type": row.get("water_type"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
