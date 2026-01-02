"""Transform ABCs Neisseria meningitidis Surveillance dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_float
from .test import test

DATASET_ID = "cdc_abcs_meningitis"
SOURCE_ID = "8bda-nhxv"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC ABCs - Neisseria meningitidis Surveillance",
    "description": (
        "Active Bacterial Core surveillance (ABCs) data on Neisseria meningitidis "
        "(meningococcal disease) including case counts, deaths, and incidence rates. "
        "ABCs is a core component of CDC's Emerging Infections Program. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Surveillance year",
        "value": "Numeric value (count or rate)",
        "units": "Unit of measurement (Counts, Rate per 100,000)",
        "bacteria": "Bacterial species (Neisseria meningitidis)",
        "topic": "Category of data (Number of Cases and Deaths, Incidence Rate)",
        "view_by": "Primary stratification category",
        "view_by_2": "Secondary stratification (survivals, deaths, serogroup, etc.)",
    },
}


def run():
    """Transform, validate, and upload ABCs meningitis data."""
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
