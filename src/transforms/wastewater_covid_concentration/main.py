"""Transform NWSS SARS-CoV-2 Concentration in Wastewater dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_wastewater_covid_concentration"
SOURCE_ID = "g653-rqe2"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Wastewater SARS-CoV-2 Concentration Data",
    "description": (
        "SARS-CoV-2 concentration measurements from wastewater treatment plants "
        "from the National Wastewater Surveillance System (NWSS). "
        "Provides raw concentration values for tracking COVID-19 community levels. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "site_id": "Unique identifier for the sampling site",
        "date": "Date of the measurement (YYYY-MM-DD)",
        "concentration": "SARS-CoV-2 concentration (copies/L per capita per day)",
        "normalization": "Normalization method used (flow-population)",
    },
}


def run():
    """Transform, validate, and upload wastewater concentration data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "site_id": row.get("key_plot_id"),
            "date": row.get("date"),
            "concentration": parse_float(row.get("pcr_conc_lin")),
            "normalization": row.get("normalization"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
