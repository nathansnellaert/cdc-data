"""Transform NHANES Select Mean Dietary Intake Estimates dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_nhanes_dietary"
SOURCE_ID = "8wmh-yzz9"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NHANES Mean Dietary Intake Estimates",
    "description": (
        "National Health and Nutrition Examination Survey (NHANES) data on mean "
        "dietary intake of select nutrients by age, sex, and race/ethnicity. "
        "Includes calcium, iron, vitamins, and other key nutrients. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "survey_years": "NHANES survey cycle (e.g., 1999-2000, 2001-2002)",
        "sex": "Sex (All, Male, Female)",
        "age_group": "Age group",
        "race_ethnicity": "Race and Hispanic origin",
        "nutrient": "Nutrient name (Calcium, Iron, Vitamin D, etc.)",
        "mean": "Mean intake value",
        "standard_error": "Standard error of the mean",
        "confidence_limit_low": "Lower 95% confidence limit",
        "confidence_limit_high": "Upper 95% confidence limit",
    },
}


def run():
    """Transform, validate, and upload NHANES dietary data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "survey_years": row.get("survey_years"),
            "sex": row.get("sex"),
            "age_group": row.get("age_group"),
            "race_ethnicity": row.get("race_and_hispanic_origin"),
            "nutrient": row.get("nutrient"),
            "mean": parse_float(row.get("mean")),
            "standard_error": parse_float(row.get("standard_error")),
            "confidence_limit_low": parse_float(row.get("lower_95_ci_limit")),
            "confidence_limit_high": parse_float(row.get("upper_95_ci_limit")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
