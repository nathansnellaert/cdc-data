"""Transform Drug Poisoning Mortality Modeled by County dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_drug_poisoning_modeled"
SOURCE_ID = "rpvx-m2md"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Drug Poisoning Mortality by County (Model-Based)",
    "description": (
        "County-level drug poisoning mortality with model-based death rates "
        "and confidence intervals. Uses statistical modeling for small area estimation. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "fips": COLUMN_DESC["fips"],
        "year": "Year",
        "state": COLUMN_DESC["state_name"],
        "fips_state": "State FIPS code",
        "county": COLUMN_DESC["county_name"],
        "population": "County population",
        "model_based_death_rate": "Model-based death rate per 100,000",
        "standard_deviation": "Standard deviation of estimate",
        "lower_95_ci": "Lower 95% confidence interval",
        "upper_95_ci": "Upper 95% confidence interval",
    },
}


def run():
    """Transform, validate, and upload drug poisoning modeled data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "fips": row.get("fips"),
            "year": row.get("year"),
            "state": row.get("state"),
            "fips_state": row.get("fipsstate"),
            "county": row.get("county"),
            "population": parse_int(row.get("population")),
            "model_based_death_rate": parse_float(row.get("model_based_death_rate")),
            "standard_deviation": parse_float(row.get("standard_deviation")),
            "lower_95_ci": parse_float(row.get("lower95ci")),
            "upper_95_ci": parse_float(row.get("upper95ci")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
