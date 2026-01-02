"""Transform NCHS Quarterly Provisional Estimates for Infant Mortality."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_infant_mortality_quarterly"
SOURCE_ID = "jqwm-z2g9"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Quarterly Provisional Infant Mortality Estimates",
    "description": (
        "Quarterly provisional estimates for infant mortality rates by cause and age "
        "from the National Center for Health Statistics. "
        "Includes overall infant mortality, neonatal, and postneonatal rates. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_quarter": "Year and quarter (e.g., 2025 Q1)",
        "topic": "Topic category (Rates by Cause, Rates by Age)",
        "indicator": "Specific indicator (Infant mortality, Neonatal, Unintentional injury)",
        "period": "Time period (12 Month-ending)",
        "rate": "Mortality rate",
        "unit": "Unit of measurement (per 1,000 births or per 100,000 births)",
        "is_significant": "Indicates statistical significance (*)",
    },
}


def run():
    """Transform, validate, and upload infant mortality data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_quarter": row.get("year_and_quarter"),
            "topic": row.get("topic"),
            "indicator": row.get("indicator"),
            "period": row.get("time_period"),
            "rate": parse_float(row.get("rate")),
            "unit": row.get("unit"),
            "is_significant": row.get("significant"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
