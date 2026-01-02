"""Transform NCHS Drug Poisoning Mortality by State dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_nchs_drug_poisoning_state"
SOURCE_ID = "44rk-q6r2"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Drug Poisoning Mortality by State",
    "description": (
        "NCHS data on drug poisoning mortality rates by state, including breakdowns "
        "by age, sex, and race/ethnicity. Includes death counts, population, rates, "
        "and confidence intervals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": "State name or United States total",
        "year": "Year",
        "sex": "Sex (Both Sexes, Male, Female)",
        "age": "Age group",
        "race": "Race/ethnicity",
        "deaths": "Number of deaths",
        "population": "Population",
        "rate": "Drug poisoning mortality rate",
        "standard_error": "Standard error of the rate",
        "confidence_limit_low": "Lower 95% confidence limit",
        "confidence_limit_high": "Upper 95% confidence limit",
        "us_rate": "US comparison rate",
        "us_age_adjusted_rate": "US age-adjusted rate",
        "unit": "Rate unit (per 100,000 population)",
    },
}


def run():
    """Transform, validate, and upload NCHS drug poisoning data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "year": parse_int(row.get("year")),
            "sex": row.get("sex"),
            "age": row.get("age"),
            "race": row.get("race"),
            "deaths": parse_int(row.get("deaths")),
            "population": parse_int(row.get("popul")),
            "rate": parse_float(row.get("rate")),
            "standard_error": parse_float(row.get("se")),
            "confidence_limit_low": parse_float(row.get("slcl")),
            "confidence_limit_high": parse_float(row.get("sucl")),
            "us_rate": parse_float(row.get("usrate")),
            "us_age_adjusted_rate": parse_float(row.get("usageadjrate")),
            "unit": row.get("unit"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
