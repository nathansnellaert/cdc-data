"""Transform NSSP ED Visits for COVID-19, Flu, RSV by Demographics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date
from .test import test

DATASET_ID = "cdc_ed_visits_respiratory"
SOURCE_ID = "7xva-uux8"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NSSP ED Visits - COVID-19, Flu, RSV by Demographics",
    "description": (
        "National Syndromic Surveillance Program (NSSP) data on emergency department "
        "visits for COVID-19, influenza, RSV, and combined respiratory illnesses. "
        "Includes demographic breakdowns by sex, age, and race/ethnicity. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "week_end": "Week ending date",
        "geography": "Geographic area (United States or region)",
        "pathogen": "Respiratory pathogen (COVID-19, Influenza, RSV, Combined)",
        "demographics_type": "Type of demographic breakdown",
        "demographics_values": "Demographic category value",
        "percent_visits": "Percent of ED visits for this pathogen",
    },
}


def run():
    """Transform, validate, and upload NSSP ED visits data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "week_end": parse_date(row.get("week_end")),
            "geography": row.get("geography"),
            "pathogen": row.get("pathogen"),
            "demographics_type": row.get("demographics_type"),
            "demographics_values": row.get("demographics_values"),
            "percent_visits": parse_float(row.get("percent_visits")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
