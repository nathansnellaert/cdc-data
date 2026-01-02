"""Transform VSRR State and National Provisional Counts for Live Births, Deaths, and Infant Deaths."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_vital_statistics_monthly"
SOURCE_ID = "hmz2-vwda"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Vital Statistics Monthly Provisional Counts",
    "description": (
        "Monthly provisional counts for live births, deaths, and infant deaths "
        "from the Vital Statistics Rapid Release (VSRR) program. "
        "Provides national-level vital statistics with both monthly and 12-month rolling totals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": "Geographic area (United States)",
        "year": COLUMN_DESC["year"],
        "month": "Month name (e.g., January, February)",
        "period": "Time period type (Monthly or 12 Month-ending)",
        "indicator": "Type of vital event (Live Births, Deaths, Infant Deaths)",
        "count": "Number of events",
    },
}


def run():
    """Transform, validate, and upload vital statistics monthly data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "year": row.get("year"),
            "month": row.get("month"),
            "period": row.get("period"),
            "indicator": row.get("indicator"),
            "count": parse_int(row.get("data_value")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
