"""Transform NWSS Public SARS-CoV-2 Wastewater Metrics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_wastewater_public"
SOURCE_ID = "2ew6-ywp6"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NWSS Public SARS-CoV-2 Wastewater Metric Data",
    "description": (
        "National Wastewater Surveillance System (NWSS) public data on SARS-CoV-2 "
        "detection in wastewater. Includes 15-day trends, detection proportions, "
        "and percentile rankings by treatment plant location. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "wwtp_jurisdiction": "Jurisdiction where treatment plant is located",
        "wwtp_id": "Wastewater treatment plant ID",
        "reporting_jurisdiction": "Reporting jurisdiction",
        "sample_location": "Sample collection location",
        "sample_location_specify": "Specific sample location identifier",
        "key_plot_id": "Unique identifier for plotting",
        "county_names": "County name(s) served",
        "county_fips": COLUMN_DESC["county_fips"],
        "population_served": "Population served by treatment plant",
        "date_start": "Start date of sampling period (YYYY-MM-DD)",
        "date_end": "End date of sampling period (YYYY-MM-DD)",
        "ptc_15d": "Percent change over 15 days (-99 indicates insufficient data)",
        "detect_prop_15d": "Detection proportion over 15 days (percent)",
        "percentile": "Percentile ranking relative to historical data",
        "sampling_prior": "Whether prior sampling exists (yes/no)",
        "first_sample_date": "Date of first sample (YYYY-MM-DD)",
    },
}


def run():
    """Transform, validate, and upload NWSS wastewater public data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "wwtp_jurisdiction": row.get("wwtp_jurisdiction"),
            "wwtp_id": row.get("wwtp_id"),
            "reporting_jurisdiction": row.get("reporting_jurisdiction"),
            "sample_location": row.get("sample_location"),
            "sample_location_specify": row.get("sample_location_specify"),
            "key_plot_id": row.get("key_plot_id"),
            "county_names": row.get("county_names"),
            "county_fips": row.get("county_fips"),
            "population_served": parse_int(row.get("population_served")),
            "date_start": row.get("date_start"),
            "date_end": row.get("date_end"),
            "ptc_15d": parse_float(row.get("ptc_15d")),
            "detect_prop_15d": parse_float(row.get("detect_prop_15d")),
            "percentile": parse_float(row.get("percentile")),
            "sampling_prior": row.get("sampling_prior"),
            "first_sample_date": row.get("first_sample_date"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
