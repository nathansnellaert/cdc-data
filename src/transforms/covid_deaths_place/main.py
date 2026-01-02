"""Transform Provisional COVID-19 Deaths by Place of Death and Age dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_place"
SOURCE_ID = "4va6-ph5s"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by Place of Death and Age",
    "description": (
        "Cumulative provisional COVID-19 deaths by state, place of death, and age group "
        "from the National Center for Health Statistics. "
        "Breaks down mortality by healthcare setting, home, nursing home, etc. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "start_date": "Start date of the reporting period",
        "end_date": "End date of the reporting period",
        "hhs_region": "HHS region number (0 for national)",
        "state": COLUMN_DESC["state_name"],
        "place_of_death": "Location where death occurred (Hospital, Nursing home, Home, etc.)",
        "age_group": COLUMN_DESC["age_category"],
        "covid_deaths": "Deaths involving COVID-19",
    },
}


def run():
    """Transform, validate, and upload COVID deaths by place data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_date": parse_date(row.get("start_week")),
            "end_date": parse_date(row.get("end_week")),
            "hhs_region": row.get("hhs_region"),
            "state": row.get("state"),
            "place_of_death": row.get("place_of_death"),
            "age_group": row.get("age_group"),
            "covid_deaths": parse_int(row.get("covid_19_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
