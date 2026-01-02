"""Transform Provisional COVID-19 Deaths by Hospital Referral Region dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_hospital_region"
SOURCE_ID = "mqmc-4b9n"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC COVID-19 Deaths by Hospital Referral Region",
    "description": (
        "Provisional COVID-19 death counts by Hospital Referral Region (HRR), including "
        "weekly death counts, age breakdowns for 65+ population, and total death counts. "
        "Data organized by MMWR epidemiological week. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date when the data was last updated",
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "mmwr_year": COLUMN_DESC["mmwr_year"],
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "hrr_name": "Hospital Referral Region name",
        "hrr_number": "Hospital Referral Region identifier",
        "state": COLUMN_DESC["state_abbr"],
        "total_deaths": "Total deaths from all causes",
        "covid_deaths": "Deaths involving COVID-19",
        "covid_deaths_65_plus": "COVID-19 deaths among those 65 years and older",
        "covid_deaths_65_74": "COVID-19 deaths among those 65-74 years",
        "covid_deaths_75_84": "COVID-19 deaths among those 75-84 years",
        "covid_deaths_85_plus": "COVID-19 deaths among those 85 years and older",
    },
}


def run():
    """Transform, validate, and upload COVID deaths by HRR dataset."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "mmwr_year": str(row.get("mmwr_year")) if row.get("mmwr_year") else None,
            "mmwr_week": int(row.get("mmwr_week")) if row.get("mmwr_week") else None,
            "hrr_name": row.get("hrr_name"),
            "hrr_number": row.get("hrr_number"),
            "state": row.get("state"),
            "total_deaths": parse_float(row.get("total_deaths")),
            "covid_deaths": parse_float(row.get("covid_19_deaths")),
            "covid_deaths_65_plus": parse_float(row.get("covid_19_deaths_over_65_years")),
            "covid_deaths_65_74": parse_float(row.get("covid_19_deaths_65_to_74_years")),
            "covid_deaths_75_84": parse_float(row.get("covid_19_deaths_75_to_84_years")),
            "covid_deaths_85_plus": parse_float(row.get("covid_19_deaths_over_85_years")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
