"""Transform Youth Nutrition, Physical Activity, and Obesity dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_youth_nutrition_obesity"
SOURCE_ID = "vba9-s8jp"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Youth Nutrition, Physical Activity and Obesity",
    "description": (
        "Youth nutrition, physical activity, and obesity data from the Youth Risk Behavior Surveillance System (YRBSS). "
        "Tracks behaviors among high school students including fruit/vegetable consumption, physical activity levels, "
        "and obesity prevalence. Data is available by state, sex, and race/ethnicity. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_start": "Start year of the survey period",
        "year_end": "End year of the survey period",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "topic_category": "Broad category (e.g., Fruits and Vegetables, Physical Activity)",
        "topic": "Specific topic within the category",
        "question": "Survey question text",
        "data_value": "Percentage or value for the indicator",
        "stratification_category": "Demographic category for stratification (e.g., Sex, Race/Ethnicity)",
        "stratification": "Specific demographic group (e.g., Male, Female, Hispanic)",
    },
}


def run():
    """Transform, validate, and upload youth nutrition/obesity dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_start": str(row.get("yearstart")) if row.get("yearstart") else None,
            "year_end": str(row.get("yearend")) if row.get("yearend") else None,
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "topic_category": row.get("class"),
            "topic": row.get("topic"),
            "question": row.get("question"),
            "data_value": parse_float(row.get("data_value")),
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
