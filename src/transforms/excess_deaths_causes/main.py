"""Transform Potentially Excess Deaths from Leading Causes dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_excess_deaths_causes"
SOURCE_ID = "vdpk-qzpr"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Potentially Excess Deaths from Five Leading Causes",
    "description": (
        "Potentially excess deaths from the five leading causes of death "
        "(heart disease, cancer, chronic lower respiratory disease, stroke, and unintentional injury) "
        "by state and age group compared to benchmarks. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "cause_of_death": "Cause of death (Cancer, Heart Disease, Stroke, etc.)",
        "state": COLUMN_DESC["state_name"],
        "state_fips_code": COLUMN_DESC["state_abbr"],
        "hhs_region": "HHS Region number",
        "age_range": "Age range (0-49, 50-64, 65-74, 75-84, 85+)",
        "benchmark": "Benchmark type (Fixed year or Floating)",
        "locality": "Locality type (All, Metropolitan, Nonmetropolitan)",
        "observed_deaths": "Observed number of deaths",
        "population": "Population",
        "expected_deaths": "Expected deaths based on benchmark",
        "potentially_excess_deaths": "Potentially excess deaths above expected",
        "percent_potentially_excess": "Percent of deaths that are potentially excess",
    },
}


def run():
    """Transform, validate, and upload excess deaths by causes data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "cause_of_death": row.get("cause_of_death"),
            "state": row.get("state"),
            "state_fips_code": row.get("state_fips_code"),
            "hhs_region": row.get("hhs_region"),
            "age_range": row.get("age_range"),
            "benchmark": row.get("benchmark"),
            "locality": row.get("locality"),
            "observed_deaths": parse_int(row.get("observed_deaths")),
            "population": parse_int(row.get("population")),
            "expected_deaths": parse_int(row.get("expected_deaths")),
            "potentially_excess_deaths": parse_int(row.get("potentially_excess_deaths")),
            "percent_potentially_excess": parse_float(row.get("percent_potentially_excess_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
