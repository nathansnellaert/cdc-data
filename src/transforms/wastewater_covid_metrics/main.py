"""Transform NWSS Public SARS-CoV-2 Wastewater Metric Data dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_wastewater_covid_metrics"
SOURCE_ID = "2ew6-ywp6"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Wastewater SARS-CoV-2 Surveillance Metrics",
    "description": (
        "SARS-CoV-2 wastewater surveillance metrics from the National Wastewater Surveillance System (NWSS). "
        "Tracks COVID-19 virus levels in wastewater at treatment plants across the United States. "
        "Includes percent change trends, detection proportions, and national percentile rankings. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "jurisdiction": "State or territory where the wastewater treatment plant is located",
        "wwtp_id": "Wastewater treatment plant identifier",
        "county_names": COLUMN_DESC["county_name"],
        "county_fips": COLUMN_DESC["county_fips"],
        "population_served": "Population served by the treatment plant",
        "date_start": "Start date of the measurement period (YYYY-MM-DD)",
        "date_end": "End date of the measurement period (YYYY-MM-DD)",
        "pct_change_15d": "Percent change in SARS-CoV-2 levels over 15 days",
        "detect_prop_15d": "Proportion of samples with virus detected over 15 days (%)",
        "percentile": "National percentile ranking of virus levels",
        "first_sample_date": "Date of first sample collected at this site",
    },
}


def run():
    """Transform, validate, and upload wastewater COVID metrics dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "jurisdiction": row.get("wwtp_jurisdiction"),
            "wwtp_id": row.get("wwtp_id"),
            "county_names": row.get("county_names"),
            "county_fips": row.get("county_fips"),
            "population_served": parse_int(row.get("population_served")),
            "date_start": row.get("date_start"),
            "date_end": row.get("date_end"),
            "pct_change_15d": parse_float(row.get("ptc_15d")),
            "detect_prop_15d": parse_float(row.get("detect_prop_15d")),
            "percentile": parse_float(row.get("percentile")),
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
