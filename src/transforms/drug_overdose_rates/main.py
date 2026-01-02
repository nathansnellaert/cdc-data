"""Transform Drug Overdose Death Rates by Drug Type dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_drug_overdose_rates"
SOURCE_ID = "95ax-ymtc"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Drug Overdose Death Rates by Drug Type and Demographics",
    "description": (
        "Age-adjusted drug overdose death rates by drug type, sex, age, race, and Hispanic origin "
        "from 1999 onwards. Includes rates for all overdoses and specific drug categories. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "indicator": "Indicator description (Drug overdose death rates)",
        "panel": "Drug category (All drug overdose, Opioids, Heroin, etc.)",
        "unit": "Rate unit (Deaths per 100,000 resident population)",
        "stub_name": "Demographic category name",
        "stub_label": "Specific demographic group",
        "year": "Year",
        "age": "Age group",
        "estimate": "Death rate estimate",
    },
}


def run():
    """Transform, validate, and upload drug overdose death rates data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "indicator": row.get("indicator"),
            "panel": row.get("panel"),
            "unit": row.get("unit"),
            "stub_name": row.get("stub_name"),
            "stub_label": row.get("stub_label"),
            "year": row.get("year"),
            "age": row.get("age"),
            "estimate": parse_float(row.get("estimate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
