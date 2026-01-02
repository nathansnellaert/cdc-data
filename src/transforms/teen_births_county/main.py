"""Transform Teen Birth Rates by County dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_teen_births_county"
SOURCE_ID = "3h58-x6cd"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Teen Birth Rates for Age Group 15-19 by County",
    "description": (
        "County-level teen birth rates for females aged 15-19 in the United States. "
        "Includes confidence intervals and FIPS codes for geographic analysis. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year": "Year",
        "state": "State name",
        "county": COLUMN_DESC["county_name"],
        "state_fips_code": "State FIPS code",
        "county_fips_code": COLUMN_DESC["county_fips"],
        "combined_fips_code": COLUMN_DESC["fips"],
        "birth_rate": "Teen birth rate per 1,000 females aged 15-19",
        "lower_confidence_limit": "Lower 95% confidence limit",
        "upper_confidence_limit": "Upper 95% confidence limit",
    },
}


def run():
    """Transform, validate, and upload teen birth rates county data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year": row.get("year"),
            "state": row.get("state"),
            "county": row.get("county"),
            "state_fips_code": row.get("state_fips_code"),
            "county_fips_code": row.get("county_fips_code"),
            "combined_fips_code": row.get("combined_fips_code"),
            "birth_rate": parse_float(row.get("birth_rate")),
            "lower_confidence_limit": parse_float(row.get("lower_confidence_limit")),
            "upper_confidence_limit": parse_float(row.get("upper_confidence_limit")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
