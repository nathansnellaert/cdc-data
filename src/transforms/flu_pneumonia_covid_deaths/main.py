"""Transform Provisional Death Counts for Influenza, Pneumonia, and COVID-19 dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, parse_mmddyyyy, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_flu_pneumonia_covid_deaths"
SOURCE_ID = "ynw2-4viq"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Deaths for Influenza, Pneumonia, and COVID-19",
    "description": (
        "Weekly provisional death counts for influenza, pneumonia, and COVID-19 by state "
        "from the National Center for Health Statistics. "
        "Tracks respiratory illness mortality over time. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "mmwr_year": COLUMN_DESC["mmwr_year"],
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "state": COLUMN_DESC["state_name"],
        "indicator": "Type of death count (Influenza, Pneumonia, COVID-19, etc.)",
        "death_count": "Number of deaths",
        "percent_expected": "Percent of expected deaths",
        "age_group": COLUMN_DESC["age_category"],
    },
}


def run():
    """Transform, validate, and upload flu/pneumonia/covid deaths data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_mmddyyyy(row.get("data_as_of")),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "mmwr_year": row.get("mmwryear"),
            "mmwr_week": row.get("mmwrweek"),
            "state": row.get("state"),
            "indicator": row.get("indicator"),
            "death_count": parse_int(row.get("deaths")),
            "percent_expected": parse_int(row.get("percent_of_expected")),
            "age_group": row.get("age_group"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
