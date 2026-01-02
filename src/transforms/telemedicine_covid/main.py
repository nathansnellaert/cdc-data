"""Transform Access and Use of Telemedicine During COVID-19 dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_telemedicine_covid"
SOURCE_ID = "8xy9-ubqz"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Access and Use of Telemedicine During COVID-19",
    "description": (
        "Survey data on telemedicine access and utilization during the COVID-19 pandemic. "
        "Includes provider offerings, patient use, and satisfaction by demographic groups. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "round": "Survey round number",
        "indicator": "Telemedicine indicator (Provider offers, Patient used, etc.)",
        "group": "Demographic group category",
        "subgroup": "Demographic subgroup",
        "sample_size": "Survey sample size",
        "response": "Response category",
        "percent": "Percentage of respondents",
    },
}


def run():
    """Transform, validate, and upload telemedicine data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "round": parse_int(row.get("round")),
            "indicator": row.get("indicator"),
            "group": row.get("group"),
            "subgroup": row.get("subgroup"),
            "sample_size": parse_int(row.get("sample_size")),
            "response": row.get("response"),
            "percent": parse_float(row.get("percent")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
