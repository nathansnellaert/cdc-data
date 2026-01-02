"""Transform NCHS Quarterly Provisional Estimates for Selected Birth Indicators."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_birth_indicators_quarterly"
SOURCE_ID = "76vv-a7x8"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Quarterly Provisional Birth Indicators",
    "description": (
        "Quarterly provisional estimates for selected birth indicators "
        "from the National Center for Health Statistics. "
        "Includes teen birth rates, cesarean delivery rates, preterm births, and more. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_quarter": "Year and quarter (e.g., 2025 Q2)",
        "topic": "Broad topic category (Birthweight, NICU Admission, etc.)",
        "topic_subgroup": "Specific indicator type (Teen Birth Rates, Cesarean Rates, etc.)",
        "indicator": "Specific indicator or age group",
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "rate": "Rate value",
        "unit": "Unit of measurement (percent, per 1000 women)",
    },
}


def run():
    """Transform, validate, and upload birth indicators data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_quarter": row.get("year_and_quarter"),
            "topic": row.get("topic"),
            "topic_subgroup": row.get("topic_subgroup"),
            "indicator": row.get("indicator"),
            "race_ethnicity": row.get("race_ethnicity"),
            "rate": parse_float(row.get("rate")),
            "unit": row.get("unit"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
