"""Transform Rates of Laboratory-Confirmed RSV, COVID-19, and Flu Hospitalizations dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_respiratory_hospitalizations_combined"
SOURCE_ID = "kvib-3txy"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Combined RSV, COVID-19, and Flu Hospitalization Rates",
    "description": (
        "Combined hospitalization rates for RSV, COVID-19, and influenza from FluSurv-NET. "
        "Allows comparison of respiratory illness hospitalizations across pathogens. "
        "Rates are per 100,000 population by age group, sex, and site. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "surveillance_network": "Surveillance network name (FluSurv-NET)",
        "season": COLUMN_DESC["season"],
        "mmwr_year": COLUMN_DESC["mmwr_year"],
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "age_group": COLUMN_DESC["age_category"],
        "sex": COLUMN_DESC["sex"],
        "race_ethnicity": COLUMN_DESC["race_ethnicity"],
        "site": "Surveillance site location",
        "virus": "Virus type (RSV, COVID-19, Influenza)",
        "weekly_rate": COLUMN_DESC["weekly_rate"],
        "cumulative_rate": COLUMN_DESC["cumulative_rate"],
    },
}


def run():
    """Transform, validate, and upload respiratory hospitalizations data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "surveillance_network": row.get("surveillance_network"),
            "season": row.get("season"),
            "mmwr_year": row.get("mmwr_year"),
            "mmwr_week": row.get("mmwr_week"),
            "age_group": row.get("age_group"),
            "sex": row.get("sex"),
            "race_ethnicity": row.get("race_ethnicity"),
            "site": row.get("site"),
            "virus": row.get("virus"),
            "weekly_rate": parse_float(row.get("weekly_rate")),
            "cumulative_rate": parse_float(row.get("cumulative_rate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
