"""Transform Natality Measures by Race and Hispanic Origin dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_natality_measures"
SOURCE_ID = "89yk-m38d"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Natality Measures for Females by Race and Hispanic Origin",
    "description": (
        "Birth and fertility rates for females in the United States by race and "
        "Hispanic origin. Includes live births, birth rates, and fertility rates. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "race": "Race/ethnicity category",
        "live_births": "Number of live births",
        "birth_rate": "Birth rate per 1,000 population",
        "fertility_rate": "Fertility rate per 1,000 women aged 15-44",
    },
}


def run():
    """Transform, validate, and upload natality measures data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "race": row.get("race"),
            "live_births": parse_int(row.get("live_births")),
            "birth_rate": parse_float(row.get("birth_rate")),
            "fertility_rate": parse_float(row.get("fertility_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
