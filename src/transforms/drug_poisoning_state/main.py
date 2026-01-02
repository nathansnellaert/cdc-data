"""Transform Drug Poisoning Mortality by State dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int
from .test import test

DATASET_ID = "cdc_drug_poisoning_state"
SOURCE_ID = "xbxb-epbu"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Drug Poisoning Mortality by State",
    "description": (
        "State-level drug poisoning mortality data with crude and age-adjusted rates "
        "by sex, age group, and race/Hispanic origin. Includes confidence intervals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "state": "State name",
        "year": "Year",
        "sex": "Sex (Both Sexes, Male, Female)",
        "age_group": "Age group category",
        "race_and_hispanic_origin": "Race and Hispanic origin category",
        "deaths": "Number of drug poisoning deaths",
        "population": "Population",
        "crude_death_rate": "Crude death rate per 100,000",
        "se_crude_rate": "Standard error for crude rate",
        "crude_rate_lower_ci": "Lower 95% CI for crude rate",
        "crude_rate_upper_ci": "Upper 95% CI for crude rate",
        "age_adjusted_rate": "Age-adjusted death rate per 100,000",
        "se_age_adjusted_rate": "Standard error for age-adjusted rate",
        "aa_rate_lower_ci": "Lower 95% CI for age-adjusted rate",
        "aa_rate_upper_ci": "Upper 95% CI for age-adjusted rate",
        "state_crude_rate_in_range": "State crude rate in range category",
        "us_crude_rate": "US crude rate for comparison",
        "us_age_adjusted_rate": "US age-adjusted rate for comparison",
    },
}


def run():
    """Transform, validate, and upload drug poisoning state data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "state": row.get("state"),
            "year": row.get("year"),
            "sex": row.get("sex"),
            "age_group": row.get("age_group"),
            "race_and_hispanic_origin": row.get("race_and_hispanic_origin"),
            "deaths": parse_int(row.get("deaths")),
            "population": parse_int(row.get("population")),
            "crude_death_rate": parse_float(row.get("crude_death_rate")),
            "se_crude_rate": parse_float(row.get("standard_error_for_crude_rate")),
            "crude_rate_lower_ci": parse_float(row.get("lower_confidence_limit_for_crude_rate")),
            "crude_rate_upper_ci": parse_float(row.get("upper_confidence_limit_for_crude_rate")),
            "age_adjusted_rate": parse_float(row.get("age_adjusted_rate")),
            "se_age_adjusted_rate": parse_float(row.get("standard_error_for_age_adjusted_rate")),
            "aa_rate_lower_ci": parse_float(row.get("lower_confidence_limit_for_age_adjusted_rate")),
            "aa_rate_upper_ci": parse_float(row.get("upper_confidence_limit_for_age_adjusted_rate")),
            "state_crude_rate_in_range": row.get("state_crude_rate_in_range"),
            "us_crude_rate": parse_float(row.get("us_crude_rate")),
            "us_age_adjusted_rate": parse_float(row.get("us_age_adjusted_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
