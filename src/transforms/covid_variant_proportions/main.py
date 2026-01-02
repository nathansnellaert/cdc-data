"""Transform SARS-CoV-2 Variant Proportions dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date
from .test import test

DATASET_ID = "cdc_covid_variant_proportions"
SOURCE_ID = "jr58-6ysp"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC SARS-CoV-2 Variant Proportions",
    "description": (
        "Estimated proportions of SARS-CoV-2 variants circulating in the United States, "
        "by HHS region or national. Includes smoothed model estimates with confidence intervals. "
        "Covers all major variants including Alpha, Delta, Omicron sublineages, and emerging variants. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "region": "Geographic region (USA or HHS Region number)",
        "week_ending": "End date of the surveillance week (YYYY-MM-DD)",
        "variant": "SARS-CoV-2 variant or lineage name (e.g., BA.5, XBB.1.5)",
        "share": "Estimated proportion of this variant (0-1)",
        "share_hi": "Upper bound of confidence interval",
        "share_lo": "Lower bound of confidence interval",
        "model_type": "Type of model used (smoothed, nowcast)",
        "time_interval": "Time interval for estimate (weekly, biweekly)",
    },
}


def run():
    """Transform, validate, and upload COVID variant proportions dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "region": row.get("usa_or_hhsregion"),
            "week_ending": parse_date(row.get("week_ending")),
            "variant": row.get("variant"),
            "share": parse_float(row.get("share")),
            "share_hi": parse_float(row.get("share_hi")),
            "share_lo": parse_float(row.get("share_lo")),
            "model_type": row.get("modeltype"),
            "time_interval": row.get("time_interval"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
