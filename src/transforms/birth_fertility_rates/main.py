"""Transform NCHS Births and General Fertility Rates dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_birth_fertility_rates"
SOURCE_ID = "e6fc-ccez"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Births and General Fertility Rates: United States",
    "description": (
        "Historical birth numbers, general fertility rates, and crude birth rates "
        "in the United States from 1909 onwards. Tracks long-term demographic trends. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "birth_number": "Total number of births",
        "general_fertility_rate": "Births per 1,000 women aged 15-44",
        "crude_birth_rate": "Births per 1,000 total population",
    },
}


def run():
    """Transform, validate, and upload birth and fertility rates data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "birth_number": parse_int(row.get("birth_number")),
            "general_fertility_rate": parse_float(row.get("general_fertility_rate")),
            "crude_birth_rate": parse_float(row.get("crude_birth_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
