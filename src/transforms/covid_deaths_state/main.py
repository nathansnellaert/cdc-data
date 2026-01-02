"""Transform Provisional COVID-19 Deaths by Week and State dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_state"
SOURCE_ID = "r8kw-7aab"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by Week and State",
    "description": (
        "Weekly provisional death counts involving COVID-19 by state and jurisdiction "
        "from the National Center for Health Statistics. "
        "Includes deaths from COVID-19, pneumonia, influenza, and combinations thereof. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "state": COLUMN_DESC["state_name"],
        "year": "Season or calendar year",
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "covid_deaths": "Deaths involving COVID-19 (ICD-10 code U07.1)",
        "total_deaths": "Total deaths from all causes",
        "pneumonia_deaths": "Deaths involving pneumonia (ICD-10 codes J12.0-J18.9)",
        "pneumonia_and_covid_deaths": "Deaths involving both pneumonia and COVID-19",
        "influenza_deaths": "Deaths involving influenza (ICD-10 codes J09-J11)",
        "pneumonia_influenza_or_covid_deaths": "Deaths involving pneumonia, influenza, or COVID-19",
        "percent_expected_deaths": "Percent of expected deaths compared to baseline",
    },
}


def run():
    """Transform, validate, and upload COVID deaths by state data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "state": row.get("state"),
            "year": row.get("year"),
            "mmwr_week": row.get("mmwr_week"),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "covid_deaths": parse_int(row.get("covid_19_deaths")),
            "total_deaths": parse_int(row.get("total_deaths")),
            "pneumonia_deaths": parse_int(row.get("pneumonia_deaths")),
            "pneumonia_and_covid_deaths": parse_int(row.get("pneumonia_and_covid_19_deaths")),
            "influenza_deaths": parse_int(row.get("influenza_deaths")),
            "pneumonia_influenza_or_covid_deaths": parse_int(row.get("pneumonia_influenza_or_covid_19_deaths")),
            "percent_expected_deaths": parse_float(row.get("percent_of_expected_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
