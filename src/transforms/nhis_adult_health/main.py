"""Transform NHIS Adult Summary Health Statistics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_nhis_adult_health"
SOURCE_ID = "25m4-6qqq"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NHIS Adult Summary Health Statistics",
    "description": (
        "National Health Interview Survey (NHIS) summary health statistics for adults. "
        "Includes prevalence of chronic conditions, health behaviors, and health care access "
        "by demographic groups. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "outcome_or_indicator": "Health outcome or indicator being measured",
        "grouping_category": "Demographic grouping category (Total, Sex, Age, Race, etc.)",
        "group": "Specific demographic group",
        "percentage": "Prevalence percentage",
        "confidence_interval": "95% confidence interval range",
        "title": "Full indicator title",
        "description": "Description of how indicator was measured",
        "year": "Survey year",
    },
}


def run():
    """Transform, validate, and upload NHIS adult health data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "outcome_or_indicator": row.get("outcome_or_indicator"),
            "grouping_category": row.get("grouping_category"),
            "group": row.get("group"),
            "percentage": parse_float(row.get("percentage")),
            "confidence_interval": row.get("confidence_interval"),
            "title": row.get("title"),
            "description": row.get("description"),
            "year": row.get("year"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
