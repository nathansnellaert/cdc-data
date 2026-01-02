"""Transform COVID-19 Hospitalizations dataset from COVID-NET."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_hospitalizations"
SOURCE_ID = "6jg4-xsqq"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC COVID-19 Hospitalization Rates",
    "description": (
        "Weekly rates of laboratory-confirmed COVID-19 hospitalizations from the COVID-NET surveillance system. "
        "Covers all ages with breakdowns by age group, sex, and race/ethnicity. "
        "Rates are per 100,000 population. Data includes both weekly and cumulative rates. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": COLUMN_DESC["state_name"],
        "season": COLUMN_DESC["season"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "age_category": COLUMN_DESC["age_category"],
        "sex": COLUMN_DESC["sex"],
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "rate_type": COLUMN_DESC["rate_type"],
        "weekly_rate": COLUMN_DESC["weekly_rate"],
        "cumulative_rate": COLUMN_DESC["cumulative_rate"],
    },
}


def run():
    """Transform, validate, and upload COVID hospitalizations dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "season": row.get("season"),
            "week_ending_date": row.get("_weekenddate"),
            "age_category": row.get("agecategory_legend"),
            "sex": row.get("sex_label"),
            "race_ethnicity": row.get("race_label"),
            "rate_type": row.get("type"),
            "weekly_rate": parse_float(row.get("weeklyrate")),
            "cumulative_rate": parse_float(row.get("cumulativerate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
