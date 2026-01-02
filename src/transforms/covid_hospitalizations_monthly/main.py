"""Transform Monthly Rates of Laboratory-Confirmed COVID-19 Hospitalizations dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_hospitalizations_monthly"
SOURCE_ID = "cf5u-bm9w"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Monthly COVID-19 Hospitalization Rates",
    "description": (
        "Monthly rates of laboratory-confirmed COVID-19 hospitalizations from COVID-NET. "
        "Aggregated monthly data for tracking hospitalization trends. "
        "Rates are per 100,000 population. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": COLUMN_DESC["state_name"],
        "season": COLUMN_DESC["season"],
        "year_month": "Year and month (YYYYMM format)",
        "age_category": COLUMN_DESC["age_category"],
        "sex": COLUMN_DESC["sex"],
        "race": COLUMN_DESC["race_ethnicity"],
        "monthly_rate": "Hospitalization rate per 100,000 for the month",
        "rate_type": COLUMN_DESC["rate_type"],
    },
}


def run():
    """Transform, validate, and upload COVID hospitalizations monthly data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "season": row.get("season"),
            "year_month": row.get("_yearmonth"),
            "age_category": row.get("agecategory_legend"),
            "sex": row.get("sex_label"),
            "race": row.get("race_label"),
            "monthly_rate": parse_float(row.get("monthlyrate")),
            "rate_type": row.get("type"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
