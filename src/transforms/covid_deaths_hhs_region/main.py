"""Transform COVID-19 Deaths by HHS Region, Race, and Age dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_hhs_region"
SOURCE_ID = "tpcp-uiv5"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by HHS Region, Race, and Age",
    "description": (
        "Weekly COVID-19 death counts by HHS region with breakdowns by race/Hispanic origin "
        "and age group. Includes total deaths for comparison. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "start_date": "Start date of period (YYYY-MM-DD)",
        "end_date": "End date of period (YYYY-MM-DD)",
        "group": "Time grouping (By Week, By Month, etc.)",
        "mmwr_year": "MMWR epidemiological year",
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "hhs_region": "HHS Region (United States or Region 1-10)",
        "race_and_hispanic_origin": COLUMN_DESC["race_ethnicity"],
        "age_group": COLUMN_DESC["age_category"],
        "covid_19_deaths": "Number of COVID-19 deaths",
        "total_deaths": "Total deaths from all causes",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths by HHS region data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_date": parse_date(row.get("start_date")),
            "end_date": parse_date(row.get("end_date")),
            "group": row.get("group"),
            "mmwr_year": row.get("mmwr_year"),
            "mmwr_week": row.get("mmwr_week"),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "hhs_region": row.get("hhs_region"),
            "race_and_hispanic_origin": row.get("race_and_hispanic_origin"),
            "age_group": row.get("age_group"),
            "covid_19_deaths": parse_int(row.get("covid_19_deaths")),
            "total_deaths": parse_int(row.get("total_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
