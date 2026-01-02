"""Transform Normal weight, overweight, and obesity among adults dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_adult_obesity_trends"
SOURCE_ID = "3nzu-udr9"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Adult Weight Status Trends (Normal, Overweight, Obese)",
    "description": (
        "Age-adjusted prevalence of normal weight, overweight, and obesity among adults 20+ years "
        "from the National Health Interview Survey. "
        "Includes breakdowns by sex, race/ethnicity, and other demographics. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "indicator": "Weight status indicator description",
        "panel": "BMI category (Normal weight, Overweight, Obesity)",
        "unit": "Unit of measurement (percent of population, age-adjusted)",
        "stub_name": "Demographic category (Sex, Race/ethnicity, etc.)",
        "stub_label": "Specific demographic group",
        "year": "Survey year",
        "estimate": "Prevalence estimate",
        "se": "Standard error",
        "flag": "Data quality flag",
    },
}


def run():
    """Transform, validate, and upload adult obesity trends data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "indicator": row.get("indicator"),
            "panel": row.get("panel"),
            "unit": row.get("unit"),
            "stub_name": row.get("stub_name"),
            "stub_label": row.get("stub_label"),
            "year": row.get("year"),
            "estimate": parse_float(row.get("estimate")),
            "se": parse_float(row.get("se")),
            "flag": row.get("flag"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
