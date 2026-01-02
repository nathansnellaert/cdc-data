"""Transform RSV Hospitalizations dataset from RSV-NET Surveillance System."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_rsv_hospitalizations"
SOURCE_ID = "29hc-w46k"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC RSV Hospitalization Rates",
    "description": (
        "Weekly rates of laboratory-confirmed RSV (Respiratory Syncytial Virus) hospitalizations "
        "from the RSV-NET surveillance system. Covers children under 18 and adults, with breakdowns "
        "by age, sex, race/ethnicity, and state. Rates are per 100,000 population. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": COLUMN_DESC["state_name"],
        "season": COLUMN_DESC["season"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "age_category": COLUMN_DESC["age_category"],
        "sex": COLUMN_DESC["sex"],
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "weekly_rate": COLUMN_DESC["weekly_rate"],
        "cumulative_rate": COLUMN_DESC["cumulative_rate"],
        "rate_type": COLUMN_DESC["rate_type"],
    },
}


def run():
    """Transform, validate, and upload RSV hospitalizations dataset."""
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
            "race_ethnicity": row.get("race"),
            "weekly_rate": parse_float(row.get("rate")),
            "cumulative_rate": parse_float(row.get("cumulative_rate")),
            "rate_type": row.get("type"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
