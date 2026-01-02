"""Transform Child Health Conditions dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_child_health_conditions"
SOURCE_ID = "2m93-xvra"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Health Conditions Among Children Under Age 18",
    "description": (
        "Health conditions prevalence among children under 18 years including asthma, "
        "allergies, and other conditions by demographic characteristics. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "indicator": "Indicator category",
        "panel": "Specific health condition (asthma, allergies, etc.)",
        "unit": "Unit of measurement (percent)",
        "stub_name": "Demographic category",
        "stub_label": "Specific demographic group",
        "year": "Year or year range",
        "age": "Age group",
        "estimate": "Prevalence estimate",
        "flag": "Data quality flag",
    },
}


def run():
    """Transform, validate, and upload child health conditions data."""
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
