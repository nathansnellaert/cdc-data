"""Transform Drug Poisoning Mortality by County dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_drug_poisoning_county"
SOURCE_ID = "pbkm-d27e"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NCHS Drug Poisoning Mortality by County",
    "description": (
        "County-level drug poisoning mortality data showing estimated age-adjusted "
        "death rates. Rates are grouped into 11 categories to protect confidentiality. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "fips": COLUMN_DESC["fips"],
        "year": "Year",
        "state": "State name",
        "state_abbr": COLUMN_DESC["state_abbr"],
        "fips_state": "State FIPS code",
        "county": COLUMN_DESC["county_name"],
        "population": "County population",
        "death_rate_category": "Estimated age-adjusted death rate in range categories",
    },
}


def run():
    """Transform, validate, and upload drug poisoning county data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "fips": row.get("fips"),
            "year": row.get("year"),
            "state": row.get("state"),
            "state_abbr": row.get("st"),
            "fips_state": row.get("fips_state"),
            "county": row.get("county"),
            "population": parse_int(row.get("population")),
            "death_rate_category": row.get("estimated_age_adjusted_death_rate_11_categories_in_ranges"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
