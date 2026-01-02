"""Transform Provisional COVID-19 Deaths by Age and Race/Ethnicity dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_age_race"
SOURCE_ID = "ks3g-spdg"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by Age and Race/Ethnicity",
    "description": (
        "Cumulative provisional COVID-19 death counts by age group and race/ethnicity "
        "from the National Center for Health Statistics. "
        "Provides demographic breakdown of COVID-19 mortality at the national level. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "start_date": "Start date of the reporting period",
        "end_date": "End date of the reporting period",
        "state": COLUMN_DESC["state_name"],
        "age_group": COLUMN_DESC["age_category"],
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "covid_deaths": "Deaths involving COVID-19",
        "total_deaths": "Total deaths from all causes",
        "pneumonia_deaths": "Deaths involving pneumonia",
        "influenza_deaths": "Deaths involving influenza",
    },
}


def run():
    """Transform, validate, and upload COVID deaths by age/race data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_date": parse_date(row.get("start_week")),
            "end_date": parse_date(row.get("end_week")),
            "state": row.get("state"),
            "age_group": row.get("age_group_new"),
            "race_ethnicity": row.get("race_and_hispanic_origin"),
            "covid_deaths": parse_int(row.get("covid_19_deaths")),
            "total_deaths": parse_int(row.get("total_deaths")),
            "pneumonia_deaths": parse_int(row.get("pneumonia_deaths")),
            "influenza_deaths": parse_int(row.get("influenza_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
