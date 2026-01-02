"""Transform Nutrition, Physical Activity, and Obesity - American Community Survey dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_physical_activity_acs"
SOURCE_ID = "8mrp-rmkw"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Physical Activity Data from American Community Survey",
    "description": (
        "Physical activity and obesity data from the American Community Survey. "
        "Tracks walking and biking to work patterns by state. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": COLUMN_DESC["year"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "data_source": "Survey data source (American Community Survey)",
        "topic_class": "Broad topic category",
        "topic": "Specific topic (Physical Activity - Behavior)",
        "question": "Survey question text",
        "data_value": "Percentage or value",
        "data_value_type": "Type of data value (Percentage, Number)",
        "stratification_category": "Demographic category for stratification",
        "stratification": "Specific demographic group",
    },
}


def run():
    """Transform, validate, and upload physical activity ACS data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("yearstart"),
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "data_source": row.get("datasource"),
            "topic_class": row.get("class"),
            "topic": row.get("topic"),
            "question": row.get("question"),
            "data_value": parse_float(row.get("data_value")),
            "data_value_type": row.get("data_value_type"),
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
