"""Transform NCHS Leading Causes of Death dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_leading_causes_death"
SOURCE_ID = "bi63-dtpu"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Leading Causes of Death: United States",
    "description": (
        "Age-adjusted death rates for the 10 leading causes of death in the United States "
        "by state, from 1999-2017. Includes heart disease, cancer, stroke, and other "
        "major causes of mortality. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year (YYYY)",
        "icd_cause_name": "ICD-10 cause name with codes",
        "cause_name": "Simplified cause of death name",
        "state": "State name",
        "deaths": "Number of deaths",
        "age_adjusted_death_rate": "Age-adjusted death rate per 100,000 population",
    },
}


def run():
    """Transform, validate, and upload leading causes of death data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "icd_cause_name": row.get("_113_cause_name"),
            "cause_name": row.get("cause_name"),
            "state": row.get("state"),
            "deaths": parse_int(row.get("deaths")),
            "age_adjusted_death_rate": parse_float(row.get("aadr")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
