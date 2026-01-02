"""Transform BRFSS Age-Adjusted Prevalence dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_brfss_prevalence"
SOURCE_ID = "d2rk-yvas"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC BRFSS Age-Adjusted Prevalence Data (2011 to present)",
    "description": (
        "Behavioral Risk Factor Surveillance System (BRFSS) age-adjusted prevalence data "
        "for various health indicators including tobacco use, chronic conditions, and health behaviors. "
        "Includes demographic breakdowns by age, sex, race/ethnicity, and income. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Survey year",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "class": "Health topic class (Tobacco Use, Chronic Conditions, etc.)",
        "topic": "Specific health topic",
        "question": "Survey question",
        "response": "Response category",
        "break_out": "Demographic break out category",
        "break_out_category": "Demographic category type",
        "sample_size": "Survey sample size",
        "data_value": "Age-adjusted prevalence percentage",
        "confidence_limit_low": "Lower 95% confidence limit",
        "confidence_limit_high": "Upper 95% confidence limit",
        "data_value_type": "Type of data value (Age-adjusted Prevalence)",
    },
}


def run():
    """Transform, validate, and upload BRFSS prevalence data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "class": row.get("class"),
            "topic": row.get("topic"),
            "question": row.get("question"),
            "response": row.get("response"),
            "break_out": row.get("break_out"),
            "break_out_category": row.get("break_out_category"),
            "sample_size": parse_int(row.get("sample_size")),
            "data_value": parse_float(row.get("data_value")),
            "confidence_limit_low": parse_float(row.get("confidence_limit_low")),
            "confidence_limit_high": parse_float(row.get("confidence_limit_high")),
            "data_value_type": row.get("data_value_type"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
