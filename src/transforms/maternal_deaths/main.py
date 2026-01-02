"""Transform VSRR Provisional Maternal Death Counts and Rates dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_float, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_maternal_deaths"
SOURCE_ID = "e2d5-ggg7"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Maternal Death Counts and Rates",
    "description": (
        "Provisional maternal death counts and rates by month and age group "
        "from the Vital Statistics Rapid Release (VSRR) program. "
        "Provides 12-month rolling totals for maternal mortality surveillance. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "jurisdiction": "Geographic area",
        "year": COLUMN_DESC["year"],
        "month": COLUMN_DESC["month"],
        "group": "Stratification group (Total, Age)",
        "subgroup": "Specific demographic within group",
        "period": "Time period type (12 month-ending)",
        "death_count": "Number of maternal deaths",
        "maternal_mortality_rate": "Maternal deaths per 100,000 live births",
    },
}


def run():
    """Transform, validate, and upload maternal deaths data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "jurisdiction": row.get("jurisdiction"),
            "year": row.get("year_of_death"),
            "month": row.get("month_of_death"),
            "group": row.get("group"),
            "subgroup": row.get("subgroup"),
            "period": row.get("time_period"),
            "death_count": parse_int(row.get("deaths")),
            "maternal_mortality_rate": parse_float(row.get("rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
