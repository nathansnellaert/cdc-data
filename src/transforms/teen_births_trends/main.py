"""Transform Teen Birth Trends by State dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_teen_births_trends"
SOURCE_ID = "y268-sna3"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS U.S. and State Trends on Teen Births",
    "description": (
        "Teen birth rates and counts by state and age group for tracking trends "
        "over time. Includes comparison to national rates. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "state": "State name",
        "age_years": "Age group (15-17, 18-19, 15-19)",
        "state_rate": "State teen birth rate per 1,000 females",
        "state_births": "Number of births in state",
        "us_births": "Number of births nationally",
        "us_birth_rate": "National teen birth rate per 1,000 females",
        "unit": "Unit of measurement",
    },
}


def run():
    """Transform, validate, and upload teen birth trends data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "state": row.get("state"),
            "age_years": row.get("age_years"),
            "state_rate": parse_float(row.get("state_rate")),
            "state_births": parse_int(row.get("state_births")),
            "us_births": parse_int(row.get("u_s_births")),
            "us_birth_rate": parse_float(row.get("u_s_birth_rate")),
            "unit": row.get("unit"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
