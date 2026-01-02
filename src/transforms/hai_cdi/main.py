"""Transform HAICViz Clostridioides difficile Infection dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_hai_cdi"
SOURCE_ID = "abgz-qs4g"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC HAICViz - Clostridioides difficile Infection (CDI)",
    "description": (
        "Healthcare-associated infection surveillance data on Clostridioides difficile "
        "infection (CDI) case rates by age group and other stratifications. "
        "CDI is a leading cause of healthcare-associated infections. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Surveillance year",
        "topic": "Metric type (Case rates per 100,000)",
        "view_by": "Stratification category (Age, etc.)",
        "grouping": "Case grouping (All cases, etc.)",
        "series": "Specific series/category",
        "value": "Value (case rate)",
    },
}


def run():
    """Transform, validate, and upload HAI CDI data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": parse_int(row.get("yearname")),
            "topic": row.get("topic"),
            "view_by": row.get("viewby"),
            "grouping": row.get("grouping"),
            "series": row.get("series"),
            "value": parse_float(row.get("value")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
