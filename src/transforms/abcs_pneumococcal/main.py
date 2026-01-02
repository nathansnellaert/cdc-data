"""Transform ABCs Streptococcus pneumoniae Surveillance dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_float
from .test import test

DATASET_ID = "cdc_abcs_pneumococcal"
SOURCE_ID = "en3s-hzsr"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC ABCs - Streptococcus pneumoniae Surveillance",
    "description": (
        "Active Bacterial Core surveillance (ABCs) data on Streptococcus pneumoniae "
        "(pneumococcal disease) including case rates by age group. Pneumococcal disease "
        "can cause pneumonia, meningitis, and bloodstream infections. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Surveillance year",
        "value": "Numeric value (rate per 100,000)",
        "units": "Unit of measurement (Per 100,000 population)",
        "bacteria": "Bacterial species (Streptococcus pneumoniae)",
        "topic": "Category of data (Case Rates)",
        "view_by": "Primary stratification category (Age)",
        "view_by_2": "Age group category",
    },
}


def run():
    """Transform, validate, and upload ABCs pneumococcal data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": parse_int(row.get("year")),
            "value": parse_float(row.get("value")),
            "units": row.get("units"),
            "bacteria": row.get("bacteria"),
            "topic": row.get("topic"),
            "view_by": row.get("viewby"),
            "view_by_2": row.get("viewby2"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
