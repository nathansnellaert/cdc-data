"""Transform CDC STATE System Tobacco Legislation - Smokefree Indoor Air dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import COLUMN_DESC
from .test import test

DATASET_ID = "cdc_tobacco_smokefree_indoor_legislation"
SOURCE_ID = "32fd-hyzc"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Tobacco Smokefree Indoor Air Legislation by State",
    "description": (
        "State-level tobacco smokefree indoor air legislation data from the CDC STATE System. "
        "Tracks restrictions on smoking in various indoor locations including "
        "restaurants, bars, workplaces, hospitals, schools, and other venues. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": COLUMN_DESC["year"],
        "quarter": COLUMN_DESC["quarter"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "topic": COLUMN_DESC["topic"],
        "measure": COLUMN_DESC["measure"],
        "provision_group": COLUMN_DESC["provision_group"],
        "provision": COLUMN_DESC["provision"],
        "provision_value": COLUMN_DESC["provision_value"],
        "provision_alt_value": COLUMN_DESC["provision_alt_value"],
        "data_type": COLUMN_DESC["data_type"],
    },
}


def run():
    """Transform, validate, and upload tobacco smokefree legislation dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": str(row.get("year")) if row.get("year") else None,
            "quarter": int(row.get("quarter")) if row.get("quarter") else None,
            "state_abbr": row.get("locationabbr"),
            "state_name": row.get("locationdesc"),
            "topic": row.get("topicdesc"),
            "measure": row.get("measuredesc"),
            "provision_group": row.get("provisiongroupdesc"),
            "provision": row.get("provisiondesc"),
            "provision_value": row.get("provisionvalue"),
            "provision_alt_value": row.get("provisionaltvalue"),
            "data_type": row.get("datatype"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
