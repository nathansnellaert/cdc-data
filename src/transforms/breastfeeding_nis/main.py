"""Transform National Immunization Survey Breastfeeding Data dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_breastfeeding_nis"
SOURCE_ID = "8hxn-cvik"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC National Immunization Survey - Breastfeeding",
    "description": (
        "National Immunization Survey data on breastfeeding rates and behaviors "
        "by state. Includes metrics on breastfeeding initiation, duration, and "
        "exclusivity at various time points. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_start": "Survey year start",
        "year_end": "Survey year end",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "data_source": "Data source",
        "class": "Topic class (Breastfeeding)",
        "topic": "Specific topic",
        "question": "Survey question",
        "data_value_type": "Type of data value",
        "data_value": "Data value (percentage)",
        "confidence_limit_low": "Lower 95% confidence limit",
        "confidence_limit_high": "Upper 95% confidence limit",
        "sample_size": "Survey sample size",
        "stratification_category": "Stratification category",
        "stratification": "Stratification value",
    },
}


def run():
    """Transform, validate, and upload breastfeeding data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_start": parse_int(row.get("yearstart")),
            "year_end": parse_int(row.get("yearend")),
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "data_source": row.get("datasource"),
            "class": row.get("class"),
            "topic": row.get("topic"),
            "question": row.get("question"),
            "data_value_type": row.get("data_value_type"),
            "data_value": parse_float(row.get("data_value")),
            "confidence_limit_low": parse_float(row.get("low_confidence_limit")),
            "confidence_limit_high": parse_float(row.get("high_confidence_limit")),
            "sample_size": parse_int(row.get("sample_size")),
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
