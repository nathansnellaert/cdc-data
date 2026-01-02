"""Transform Tobacco Preemption Legislation dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import COLUMN_DESC
from .test import test

DATASET_ID = "cdc_tobacco_preemption"
SOURCE_ID = "hj2x-85ya"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Tobacco Preemption Legislation by State",
    "description": (
        "State-level tobacco preemption legislation from the State Tobacco Activities "
        "Tracking and Evaluation (STATE) System. Tracks whether state laws preempt local "
        "governments from enacting stricter tobacco control measures. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": COLUMN_DESC["year"],
        "quarter": COLUMN_DESC["quarter"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "topic": "Preemption legislation topic",
        "measure": "Specific preemption measure",
        "smokefree_indoor_air": "Preemption status for smokefree indoor air",
        "youth_access": "Preemption status for youth access laws",
        "licensure": "Preemption status for licensure laws",
    },
}


def run():
    """Transform, validate, and upload tobacco preemption data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "quarter": row.get("quarter"),
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "topic": row.get("topicdesc"),
            "measure": row.get("measuredesc"),
            "smokefree_indoor_air": row.get("smokefree_indoor_air"),
            "youth_access": row.get("youth_access"),
            "licensure": row.get("licensure"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
