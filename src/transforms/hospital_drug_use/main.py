"""Transform Drug Use Data from Selected Hospitals dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date
from .test import test

DATASET_ID = "cdc_hospital_drug_use"
SOURCE_ID = "gypc-kpgn"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Drug Use Data from Selected Hospitals",
    "description": (
        "Drug use metrics from emergency departments and inpatient settings "
        "at selected hospitals. Tracks various drug-related measures over time. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "setting": "Healthcare setting (ED=Emergency Department, IP=Inpatient)",
        "start_date": "Start date of the measurement period",
        "end_date": "End date of the measurement period",
        "measure": "Type of measurement (count, rate, etc.)",
        "value": "Measured value",
        "figure_type": "Figure identifier",
    },
}


def run():
    """Transform, validate, and upload hospital drug use data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "setting": row.get("setting"),
            "start_date": parse_date(row.get("start_time")),
            "end_date": parse_date(row.get("end_time")),
            "measure": row.get("measure"),
            "value": parse_float(row.get("value")),
            "figure_type": row.get("figure"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
