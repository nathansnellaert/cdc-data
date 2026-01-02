"""Transform Childhood Mortality Rates dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_childhood_mortality"
SOURCE_ID = "v6ab-adf5"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Childhood Mortality Rates",
    "description": (
        "Historical childhood mortality rates in the United States from 1900 onwards. "
        "Tracks dramatic improvements in child survival over more than a century. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "age_at_death": "Age group at death (Under 1 Year, 1-4 Years, 5-14 Years)",
        "mortality_rate": "Mortality rate per 100,000 population",
    },
}


def run():
    """Transform, validate, and upload childhood mortality rates data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "age_at_death": row.get("age_at_death"),
            "mortality_rate": parse_float(row.get("mortality_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
