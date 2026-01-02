"""Transform NNDSS Weekly Data dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_nndss_weekly"
SOURCE_ID = "x9gk-5huc"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NNDSS Weekly Notifiable Disease Data",
    "description": (
        "Weekly provisional cases of nationally notifiable diseases from the National Notifiable "
        "Diseases Surveillance System (NNDSS). Includes counts by state/territory, disease, and week. "
        "Covers infectious diseases such as Anthrax, Chikungunya, and other reportable conditions. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": COLUMN_DESC["state_name"],
        "year": COLUMN_DESC["year"],
        "week": "MMWR week number (1-53)",
        "disease": "Name of the notifiable disease",
        "current_week_count": "Number of cases reported in the current week",
        "cumulative_ytd": "Cumulative year-to-date case count",
        "region": "Census region name (e.g., Middle Atlantic, New England)",
    },
}


def run():
    """Transform, validate, and upload NNDSS weekly data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("states"),
            "year": row.get("year"),
            "week": row.get("week"),
            "disease": row.get("label"),
            "current_week_count": parse_int(row.get("m2")),
            "cumulative_ytd": parse_int(row.get("m4")),
            "region": row.get("location2"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
