"""Transform Weekly Rates of Laboratory-Confirmed RSV Hospitalizations dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_rsv_hospitalizations_weekly"
SOURCE_ID = "29hc-w46k"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Weekly RSV Hospitalization Rates",
    "description": (
        "Weekly rates of laboratory-confirmed RSV hospitalizations from the RSV-NET surveillance system. "
        "Includes rates by state, age category, sex, and race/ethnicity. "
        "Rates are per 100,000 population. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": COLUMN_DESC["state_name"],
        "season": COLUMN_DESC["season"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "age_category": COLUMN_DESC["age_category"],
        "sex": COLUMN_DESC["sex"],
        "race": COLUMN_DESC["race_ethnicity"],
        "weekly_rate": COLUMN_DESC["weekly_rate"],
        "cumulative_rate": COLUMN_DESC["cumulative_rate"],
    },
}


def run():
    """Transform, validate, and upload RSV hospitalizations weekly data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "season": row.get("season"),
            "week_ending_date": row.get("week_ending_date"),
            "age_category": row.get("age_category"),
            "sex": row.get("sex"),
            "race": row.get("race"),
            "weekly_rate": parse_float(row.get("rate")),
            "cumulative_rate": parse_float(row.get("cumulative_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
