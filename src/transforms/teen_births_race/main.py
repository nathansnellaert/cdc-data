"""Transform Teen Birth Rates by Race and Age dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_teen_births_race"
SOURCE_ID = "e8kx-wbww"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Teen Birth Rates by Age Group, Race, and Hispanic Origin",
    "description": (
        "Historical teen birth rates for females by age group, race, and Hispanic origin "
        "in the United States from 1960 onwards. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "race": "Race/Hispanic origin category",
        "age": "Age group (10-14, 15-17, 18-19 years)",
        "birth_rate": "Birth rate per 1,000 females in age group",
    },
}


def run():
    """Transform, validate, and upload teen birth rates by race data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "race": row.get("race"),
            "age": row.get("age"),
            "birth_rate": parse_float(row.get("birth_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
