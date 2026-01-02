"""Transform YRBS Nutrition, Physical Activity, and Obesity dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_yrbs_obesity"
SOURCE_ID = "vba9-s8jp"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC YRBS Nutrition, Physical Activity, and Obesity",
    "description": (
        "Youth Risk Behavior Surveillance System (YRBS) data on nutrition, "
        "physical activity, and obesity among high school students (grades 9-12). "
        "Includes state-level data on fruit/vegetable consumption, physical activity, and weight status. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_start": "Survey start year",
        "year_end": "Survey end year",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "datasource": "Data source (YRBS)",
        "class": "Data class category (Fruits and Vegetables, Physical Activity, etc.)",
        "topic": "Specific topic",
        "question": "Survey question",
        "data_value_type": "Type of data value",
        "data_value": "Percentage value",
        "sex": "Sex category (Male, Female, Total)",
        "stratification_category": "Stratification category",
        "stratification": "Specific stratification value",
    },
}


def run():
    """Transform, validate, and upload YRBS obesity data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_start": row.get("yearstart"),
            "year_end": row.get("yearend"),
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "datasource": row.get("datasource"),
            "class": row.get("class"),
            "topic": row.get("topic"),
            "question": row.get("question"),
            "data_value_type": row.get("data_value_type"),
            "data_value": parse_float(row.get("data_value")),
            "sex": row.get("sex"),
            "stratification_category": row.get("stratificationcategory1"),
            "stratification": row.get("stratification1"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
