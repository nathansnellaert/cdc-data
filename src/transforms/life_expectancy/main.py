"""Transform Death Rates and Life Expectancy at Birth dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_life_expectancy"
SOURCE_ID = "w9j2-ggv5"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Death Rates and Life Expectancy at Birth",
    "description": (
        "Historical life expectancy at birth and mortality rates in the United States "
        "from 1900 onwards, by race and sex. Tracks improvements in public health "
        "and longevity over more than a century. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "race": "Race category (All Races, White, Black)",
        "sex": "Sex (Both Sexes, Male, Female)",
        "average_life_expectancy": "Life expectancy at birth in years",
        "mortality": "Mortality rate per 100,000 population",
    },
}


def run():
    """Transform, validate, and upload life expectancy data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "race": row.get("race"),
            "sex": row.get("sex"),
            "average_life_expectancy": parse_float(row.get("average_life_expectancy")),
            "mortality": parse_float(row.get("mortality")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
