"""Transform COVID-19 Deaths Focus on Ages 0-18 dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_mmddyyyy
from .test import test

DATASET_ID = "cdc_covid_deaths_youth"
SOURCE_ID = "nr4s-juj3"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths: Focus on Ages 0-18 Years",
    "description": (
        "COVID-19 death counts among children and adolescents (ages 0-18) "
        "with breakdowns by age group, sex, and race. Covers the full pandemic period. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "start_week": "Start date of period (YYYY-MM-DD)",
        "end_week": "End date of period (YYYY-MM-DD)",
        "age_group": "Age group (0-4 years, 5-11 years, 12-17 years, etc.)",
        "covid_19_deaths": "Total COVID-19 deaths",
        "indicator": "Breakdown indicator (Age, Sex, Race)",
        "sex": "Sex category (All, Male, Female)",
        "race_group": "Race/ethnicity group",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths youth data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_mmddyyyy(row.get("data_as_of")),
            "start_week": parse_mmddyyyy(row.get("start_week")),
            "end_week": parse_mmddyyyy(row.get("end_week")),
            "age_group": row.get("age_group"),
            "covid_19_deaths": parse_int(row.get("covid_19_deaths")),
            "indicator": row.get("indicator"),
            "sex": row.get("sex"),
            "race_group": row.get("race_group"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
