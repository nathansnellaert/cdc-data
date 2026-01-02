"""Transform VSRR Provisional Drug Overdose Death Counts dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_provisional_drug_overdose"
SOURCE_ID = "xkb8-kh2a"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Drug Overdose Death Counts",
    "description": (
        "Provisional monthly counts for drug overdose deaths by state and drug category "
        "from the Vital Statistics Rapid Release (VSRR) program. "
        "Includes counts for specific substances like opioids, cocaine, and psychostimulants. "
        "Data is 12-month rolling totals ending in each month. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "year": COLUMN_DESC["year"],
        "month": "Month name (e.g., January, February)",
        "period": "Time period type (12 month-ending)",
        "indicator": "Drug category or type (e.g., Cocaine, Opioids, Heroin)",
        "data_value": "Number of deaths",
        "predicted_value": "Model-predicted death count",
        "percent_complete": "Percentage of death records received",
        "percent_pending": "Percentage of records pending investigation",
    },
}


def run():
    """Transform, validate, and upload provisional drug overdose data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state_abbr": row.get("state"),
            "state_name": row.get("state_name"),
            "year": row.get("year"),
            "month": row.get("month"),
            "period": row.get("period"),
            "indicator": row.get("indicator"),
            "data_value": parse_int(row.get("data_value")),
            "predicted_value": parse_int(row.get("predicted_value")),
            "percent_complete": parse_float(row.get("percent_complete")),
            "percent_pending": parse_float(row.get("percent_pending_investigation")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
