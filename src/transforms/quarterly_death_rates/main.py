"""Transform NCHS Quarterly Provisional Estimates for Selected Indicators of Mortality."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_quarterly_death_rates"
SOURCE_ID = "489q-934x"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Quarterly Provisional Death Rates by Cause",
    "description": (
        "Quarterly provisional age-adjusted death rates by cause of death "
        "from the National Center for Health Statistics. "
        "Includes rates by sex and demographic breakdowns. Deaths per 100,000 population. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_quarter": "Year and quarter (e.g., 2022 Q3)",
        "period": "Time period type (3-month or 12-month ending)",
        "cause_of_death": "Cause of death category (Cancer, Heart disease, etc.)",
        "rate_type": "Type of rate (Age-adjusted)",
        "unit": "Unit of measurement (Deaths per 100,000)",
        "rate_overall": "Overall age-adjusted death rate",
        "rate_female": "Death rate for females",
        "rate_male": "Death rate for males",
    },
}


def run():
    """Transform, validate, and upload quarterly death rates data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_quarter": row.get("year_and_quarter"),
            "period": row.get("time_period"),
            "cause_of_death": row.get("cause_of_death"),
            "rate_type": row.get("rate_type"),
            "unit": row.get("unit"),
            "rate_overall": parse_float(row.get("rate_overall")),
            "rate_female": parse_float(row.get("rate_sex_female")),
            "rate_male": parse_float(row.get("rate_sex_male")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
