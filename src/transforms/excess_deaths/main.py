"""Transform Excess Deaths Associated with COVID-19 dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_excess_deaths"
SOURCE_ID = "xkkf-xrst"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Excess Deaths Associated with COVID-19",
    "description": (
        "Estimates of excess deaths by week and state, comparing observed deaths "
        "to expected counts based on historical trends. Includes all-cause mortality "
        "and estimates of excess deaths attributable to COVID-19. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "week_ending_date": "End date of the surveillance week (YYYY-MM-DD)",
        "state": "State name",
        "observed_number": "Observed death count for the week",
        "upper_bound_threshold": "Upper bound threshold for expected deaths",
        "exceeds_threshold": "Whether observed exceeds threshold",
        "average_expected_count": "Average expected death count",
        "excess_estimate": "Estimated excess deaths for the week",
        "total_excess_estimate": "Cumulative excess deaths estimate",
        "percent_excess_estimate": "Percent excess deaths relative to expected",
        "year": "Year",
        "type": "Estimate type (Predicted weighted, Unweighted, etc.)",
        "outcome": "Outcome category (All causes, specific conditions)",
    },
}


def run():
    """Transform, validate, and upload excess deaths data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "week_ending_date": row.get("week_ending_date"),
            "state": row.get("state"),
            "observed_number": parse_int(row.get("observed_number")),
            "upper_bound_threshold": parse_int(row.get("upper_bound_threshold")),
            "exceeds_threshold": row.get("exceeds_threshold"),
            "average_expected_count": parse_int(row.get("average_expected_count")),
            "excess_estimate": parse_int(row.get("excess_estimate")),
            "total_excess_estimate": parse_int(row.get("total_excess_estimate")),
            "percent_excess_estimate": parse_float(row.get("percent_excess_estimate")),
            "year": row.get("year"),
            "type": row.get("type"),
            "outcome": row.get("outcome"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
