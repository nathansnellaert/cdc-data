"""Transform WIC Nutrition, Physical Activity, and Obesity dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_wic_obesity"
SOURCE_ID = "735e-byxc"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Nutrition, Physical Activity, and Obesity - WIC",
    "description": (
        "Women, Infants, and Children (WIC) program participant data on obesity "
        "and weight status. Includes prevalence of overweight and obesity among "
        "infants, children, and women by state and age group. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_start": "Survey start year",
        "year_end": "Survey end year",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "datasource": "Data source (WIC Participant Characteristics)",
        "class": "Data class category",
        "topic": "Health topic",
        "question": "Survey question/indicator",
        "data_value_type": "Type of data value",
        "data_value": "Percentage value",
        "low_confidence_limit": "Lower 95% confidence limit",
        "high_confidence_limit": "Upper 95% confidence limit",
        "sample_size": "Sample size",
        "age_months": "Age range in months",
        "stratification_category": "Stratification category",
        "stratification": "Specific stratification value",
    },
}


def run():
    """Transform, validate, and upload WIC obesity data."""
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
            "low_confidence_limit": parse_float(row.get("low_confidence_limit")),
            "high_confidence_limit": parse_float(row.get("high_confidence_limit")),
            "sample_size": parse_int(row.get("sample_size")),
            "age_months": row.get("age_months"),
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
