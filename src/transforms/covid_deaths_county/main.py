"""Transform COVID-19 Deaths by County dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_county"
SOURCE_ID = "kn79-hsxy"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Death Counts by County",
    "description": (
        "County-level provisional COVID-19 death counts with urban-rural classification. "
        "Aggregates deaths from the start of the pandemic through mid-2023. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "start_week": "Start date of period (YYYY-MM-DD)",
        "end_week": "End date of period (YYYY-MM-DD)",
        "state_name": COLUMN_DESC["state_abbr"],
        "county_name": COLUMN_DESC["county_name"],
        "county_fips_code": COLUMN_DESC["county_fips"],
        "urban_rural_code": "Urban-rural classification (Metro, Noncore, etc.)",
        "total_death": "Total COVID-19 deaths",
        "footnote": "Data quality footnotes",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths by county data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_week": parse_date(row.get("start_week")),
            "end_week": parse_date(row.get("end_week")),
            "state_name": row.get("state_name"),
            "county_name": row.get("county_name"),
            "county_fips_code": row.get("county_fips_code"),
            "urban_rural_code": row.get("urban_rural_code"),
            "total_death": parse_int(row.get("total_death")),
            "footnote": row.get("footnote"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
