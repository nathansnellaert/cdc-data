"""Transform VSRR Provisional Drug Overdose Death Counts dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_drug_overdose_deaths"
SOURCE_ID = "xkb8-kh2a"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Drug Overdose Death Counts",
    "description": (
        "Provisional counts for drug overdose deaths by state from the National Vital Statistics System. "
        "Includes deaths involving opioids (synthetic, natural, semi-synthetic), heroin, cocaine, "
        "psychostimulants, and other drug categories. Data is reported as 12-month rolling totals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": COLUMN_DESC["year"],
        "month": "Month of the 12-month period ending (e.g., January, February)",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "indicator": "Type of drug or death category (e.g., Opioids, Heroin, Cocaine)",
        "death_count": "Number of deaths in the 12-month period ending in specified month",
        "predicted_value": "Model-predicted count",
        "percent_complete": "Percentage of death records with cause of death completed",
        "percent_pending": "Percentage of records pending investigation",
    },
}


def run():
    """Transform, validate, and upload drug overdose deaths dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": str(row.get("year")) if row.get("year") else None,
            "month": row.get("month"),
            "state_abbr": row.get("state"),
            "state_name": row.get("state_name"),
            "indicator": row.get("indicator"),
            "death_count": parse_int(row.get("data_value")),
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
