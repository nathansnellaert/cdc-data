"""Transform Age-Adjusted Death Rates for Major Causes dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_age_adjusted_death_rates"
SOURCE_ID = "6rkc-nb2q"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Age-Adjusted Death Rates for Selected Major Causes of Death",
    "description": (
        "Historical age-adjusted death rates for major causes of death in the United States. "
        "Tracks long-term mortality trends for heart disease, cancer, and other leading causes. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "leading_causes": "Cause of death category",
        "age_adjusted_death_rate": "Age-adjusted death rate per 100,000 population",
    },
}


def run():
    """Transform, validate, and upload age-adjusted death rates data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "leading_causes": row.get("leading_causes"),
            "age_adjusted_death_rate": parse_float(row.get("age_adjusted_death_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
