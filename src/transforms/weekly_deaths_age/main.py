"""Transform Weekly Deaths by Age dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_weekly_deaths_age"
SOURCE_ID = "y5bj-9g5w"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Weekly Deaths by Jurisdiction and Age",
    "description": (
        "Weekly counts of deaths by jurisdiction and age group "
        "from the National Center for Health Statistics. "
        "Includes predicted (weighted) death counts. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "jurisdiction": COLUMN_DESC["state_name"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "year": COLUMN_DESC["year"],
        "week": "Week number (1-52)",
        "age_group": COLUMN_DESC["age_category"],
        "death_count": "Number of deaths in the week",
        "time_period": "Reference time period for comparison",
        "estimate_type": "Type of estimate (Predicted weighted)",
    },
}


def run():
    """Transform, validate, and upload weekly deaths by age data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "jurisdiction": row.get("jurisdiction"),
            "state_abbr": row.get("state_abbreviation"),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "year": row.get("year"),
            "week": row.get("week"),
            "age_group": row.get("age_group"),
            "death_count": parse_int(row.get("number_of_deaths")),
            "time_period": row.get("time_period"),
            "estimate_type": row.get("type"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
