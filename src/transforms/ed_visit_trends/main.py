"""Transform NSSP Emergency Department Visit Trajectories by State and Sub State Region dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_ed_visit_trends"
SOURCE_ID = "rdmq-nq56"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Emergency Department Visit Trends by Region",
    "description": (
        "Emergency department visit trends for COVID-19, influenza, and RSV "
        "by state and sub-state region from the National Syndromic Surveillance Program (NSSP). "
        "Shows weekly trajectory direction (Increasing, Decreasing, No Change). "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "week_ending": COLUMN_DESC["week_ending_date"],
        "geography": COLUMN_DESC["state_name"],
        "county": COLUMN_DESC["county_name"],
        "hsa": "Health Service Area",
        "covid_trend": "COVID-19 ED visit trend (Increasing, Decreasing, No Change, Sparse)",
        "influenza_trend": "Influenza ED visit trend",
        "rsv_trend": "RSV ED visit trend",
    },
}


def run():
    """Transform, validate, and upload ED visit trends data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "week_ending": parse_date(row.get("week_end")),
            "geography": row.get("geography"),
            "county": row.get("county"),
            "hsa": row.get("hsa"),
            "covid_trend": row.get("ed_trends_covid"),
            "influenza_trend": row.get("ed_trends_influenza"),
            "rsv_trend": row.get("ed_trends_rsv"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
