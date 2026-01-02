"""Transform Child and Adolescent Obesity Trends dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_child_obesity_trends"
SOURCE_ID = "9gay-j69q"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Child and Adolescent Obesity Trends (Ages 2-19)",
    "description": (
        "Obesity prevalence among children and adolescents aged 2-19 years in the "
        "United States by selected characteristics. Data from NHANES surveys starting "
        "1988-1994 through present. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "indicator": "Health indicator (Obesity among children and adolescents)",
        "panel": "Age panel",
        "unit": "Unit of measurement (Percent of population)",
        "stub_name": "Category name for stratification",
        "stub_label": "Specific label within category",
        "year": "Survey period (e.g., 1988-1994, 2017-2018)",
        "age": "Age group",
        "estimate": "Prevalence estimate (percentage)",
        "standard_error": "Standard error of the estimate",
    },
}


def run():
    """Transform, validate, and upload child obesity trends data."""
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
            "age": row.get("age"),
            "estimate": parse_float(row.get("estimate")),
            "standard_error": parse_float(row.get("se")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
