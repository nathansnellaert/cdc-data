"""Transform Anxiety and Depression Indicators dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date
from .test import test

DATASET_ID = "cdc_anxiety_depression"
SOURCE_ID = "8pt5-q6wp"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Indicators of Anxiety or Depression Based on Symptom Frequency",
    "description": (
        "Indicators of anxiety or depression based on reported frequency of symptoms "
        "during the last 7 days from the Household Pulse Survey. Tracks mental health "
        "trends during and after the COVID-19 pandemic by state and demographic groups. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "indicator": "Mental health indicator (Anxiety, Depression, or combined)",
        "group": "Demographic grouping type (National, By Age, By Sex, etc.)",
        "state": "State name",
        "subgroup": "Specific demographic subgroup",
        "phase": "Survey phase number",
        "time_period": "Time period identifier",
        "time_period_label": "Human-readable time period (e.g., Apr 23 - May 5, 2020)",
        "time_period_start_date": "Start date of survey period (YYYY-MM-DD)",
        "time_period_end_date": "End date of survey period (YYYY-MM-DD)",
        "value": "Percentage of population with symptoms",
        "low_ci": "Lower 95% confidence interval",
        "high_ci": "Upper 95% confidence interval",
        "confidence_interval": "Formatted confidence interval range",
    },
}


def run():
    """Transform, validate, and upload anxiety and depression data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "indicator": row.get("indicator"),
            "group": row.get("group"),
            "state": row.get("state"),
            "subgroup": row.get("subgroup"),
            "phase": row.get("phase"),
            "time_period": row.get("time_period"),
            "time_period_label": row.get("time_period_label"),
            "time_period_start_date": parse_date(row.get("time_period_start_date")),
            "time_period_end_date": parse_date(row.get("time_period_end_date")),
            "value": parse_float(row.get("value")),
            "low_ci": parse_float(row.get("lowci")),
            "high_ci": parse_float(row.get("highci")),
            "confidence_interval": row.get("confidence_interval"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
