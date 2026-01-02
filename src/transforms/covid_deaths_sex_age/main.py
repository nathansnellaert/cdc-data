"""Transform Provisional COVID-19 Deaths by Sex and Age dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_sex_age"
SOURCE_ID = "9bhg-hcku"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by Sex and Age",
    "description": (
        "Cumulative provisional death counts involving COVID-19 by state, sex, and age group "
        "from the National Center for Health Statistics. "
        "Includes deaths from COVID-19, pneumonia, influenza, and combinations thereof. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "start_date": "Start date of the reporting period",
        "end_date": "End date of the reporting period",
        "state": COLUMN_DESC["state_name"],
        "sex": COLUMN_DESC["sex"],
        "age_group": COLUMN_DESC["age_category"],
        "covid_deaths": "Deaths involving COVID-19 (ICD-10 code U07.1)",
        "total_deaths": "Total deaths from all causes",
        "pneumonia_deaths": "Deaths involving pneumonia",
        "pneumonia_and_covid_deaths": "Deaths involving both pneumonia and COVID-19",
        "influenza_deaths": "Deaths involving influenza",
    },
}


def run():
    """Transform, validate, and upload COVID deaths by sex and age data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_date": parse_date(row.get("start_date")),
            "end_date": parse_date(row.get("end_date")),
            "state": row.get("state"),
            "sex": row.get("sex"),
            "age_group": row.get("age_group"),
            "covid_deaths": parse_int(row.get("covid_19_deaths")),
            "total_deaths": parse_int(row.get("total_deaths")),
            "pneumonia_deaths": parse_int(row.get("pneumonia_deaths")),
            "pneumonia_and_covid_deaths": parse_int(row.get("pneumonia_and_covid_19_deaths")),
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
