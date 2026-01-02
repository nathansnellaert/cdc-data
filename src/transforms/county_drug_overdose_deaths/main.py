"""Transform County-Level Drug Overdose Death Counts dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_county_drug_overdose_deaths"
SOURCE_ID = "gb4e-yj24"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC County-Level Drug Overdose Death Counts",
    "description": (
        "Provisional county-level drug overdose death counts from the National Vital Statistics System. "
        "Provides monthly death counts at the county level, with suppression applied for small counts "
        "to protect confidentiality. Includes urban-rural classification codes. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "year": COLUMN_DESC["year"],
        "month": COLUMN_DESC["month"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "state_name": COLUMN_DESC["state_name"],
        "county_name": COLUMN_DESC["county_name"],
        "fips": COLUMN_DESC["fips"],
        "state_fips": "State FIPS code",
        "county_fips": COLUMN_DESC["county_fips"],
        "urban_rural_code": "NCHS Urban-Rural Classification code (2013)",
        "percent_pending": "Percentage of records pending investigation",
    },
}


def run():
    """Transform, validate, and upload county drug overdose deaths dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "year": str(row.get("year")) if row.get("year") else None,
            "month": int(row.get("month")) if row.get("month") else None,
            "state_abbr": row.get("st_abbrev"),
            "state_name": row.get("state_name"),
            "county_name": row.get("countyname"),
            "fips": row.get("fips"),
            "state_fips": row.get("statefips"),
            "county_fips": row.get("countyfips"),
            "urban_rural_code": row.get("code2013"),
            "percent_pending": parse_float(row.get("percentage_of_records_pending")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
