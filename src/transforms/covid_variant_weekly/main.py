"""Transform COVID-19 Variant Weekly Proportions dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_covid_variant_weekly"
SOURCE_ID = "jr58-6ysp"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC SARS-CoV-2 Variant Weekly Proportions",
    "description": (
        "Weekly proportions of SARS-CoV-2 variants circulating in the US "
        "by region. Includes nowcast and weighted estimates with confidence intervals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "region": "USA or HHS Region identifier",
        "week_ending": "End date of the surveillance week (YYYY-MM-DD)",
        "variant": "Variant name/lineage",
        "share": "Estimated share/proportion of variant",
        "share_hi": "Upper bound of confidence interval",
        "share_lo": "Lower bound of confidence interval",
        "count_lt10": "Whether sample count is less than 10",
        "model_type": "Model type (Nowcast, Weighted)",
        "time_interval": "Time interval covered",
        "creation_date": "Date the estimate was created",
    },
}


def run():
    """Transform, validate, and upload COVID-19 variant weekly data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "region": row.get("usa_or_hhsregion"),
            "week_ending": row.get("week_ending"),
            "variant": row.get("variant"),
            "share": parse_float(row.get("share")),
            "share_hi": parse_float(row.get("share_hi")),
            "share_lo": parse_float(row.get("share_lo")),
            "count_lt10": row.get("count_lt10"),
            "model_type": row.get("modeltype"),
            "time_interval": row.get("time_interval"),
            "creation_date": row.get("creation_date"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
