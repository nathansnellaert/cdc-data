"""Transform CDC birth rates for unmarried women dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_birth_rates_unmarried"
SOURCE_ID = "6tkz-y37d"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Birth Rates for Unmarried Women",
    "description": (
        "Birth rates for unmarried women by age group, race, and Hispanic origin in the United States. "
        "Rates are per 1,000 unmarried women in the specified group. "
        "Data from the National Center for Health Statistics (NCHS). "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year of observation (YYYY)",
        "age_group": "Age group of mothers (e.g., '15-19 years', '20-24 years')",
        "race_ethnicity": "Race and/or Hispanic origin category",
        "birth_rate": "Birth rate per 1,000 unmarried women in the group",
    },
}


def run():
    """Transform, validate, and upload birth rates dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "age_group": row.get("age"),
            "race_ethnicity": row.get("race"),
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
