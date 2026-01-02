"""Transform Weekly Deaths by Race and Hispanic Origin dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_deaths_race_ethnicity"
SOURCE_ID = "qfhf-uhaa"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Weekly Deaths by Jurisdiction and Race/Ethnicity",
    "description": (
        "Weekly counts of deaths by jurisdiction, race, and Hispanic origin "
        "from the National Center for Health Statistics. "
        "Covers multiple years for demographic mortality analysis. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "jurisdiction": COLUMN_DESC["state_name"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "mmwr_year": COLUMN_DESC["mmwr_year"],
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "death_count": "Number of deaths in the week",
        "time_period": "Reference year or period",
    },
}


def run():
    """Transform, validate, and upload deaths by race/ethnicity data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "jurisdiction": row.get("jurisdiction"),
            "state_abbr": row.get("state_abbreviation"),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "mmwr_year": row.get("mmwryear"),
            "mmwr_week": row.get("mmwrweek"),
            "race_ethnicity": row.get("race_ethnicity"),
            "death_count": parse_int(row.get("number_of_deaths")),
            "time_period": row.get("time_period"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
