"""Transform COVID-19 Deaths by County and Race dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_covid_deaths_county_race"
SOURCE_ID = "k8wy-p9cg"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by County, Race and Hispanic Origin",
    "description": (
        "County-level COVID-19 death counts with distribution by race and Hispanic origin. "
        "Includes urban/rural classification and all-cause death comparisons. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "start_week": "Start date of period (YYYY-MM-DD)",
        "end_week": "End date of period (YYYY-MM-DD)",
        "state": COLUMN_DESC["state_abbr"],
        "county_name": COLUMN_DESC["county_name"],
        "urban_rural_code": "Urban-rural classification code",
        "urban_rural_desc": "Urban-rural classification description",
        "fips_state": "State FIPS code",
        "fips_county": "County FIPS code",
        "fips_code": "Full FIPS code",
        "indicator": "Indicator type (Distribution, Count)",
        "all_deaths_total": "Total all-cause deaths",
        "covid_19_deaths_total": "Total COVID-19 deaths",
        "non_hispanic_white": "Non-Hispanic White proportion/count",
        "non_hispanic_black": "Non-Hispanic Black proportion/count",
        "non_hispanic_american_indian": "Non-Hispanic American Indian proportion/count",
        "non_hispanic_asian": "Non-Hispanic Asian proportion/count",
        "non_hispanic_nhopi": "Non-Hispanic Native Hawaiian/Pacific Islander proportion/count",
        "hispanic": "Hispanic proportion/count",
        "other": "Other race/ethnicity proportion/count",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths by county and race data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_week": parse_date(row.get("start_week")),
            "end_week": parse_date(row.get("end_week")),
            "state": row.get("state"),
            "county_name": row.get("county_name"),
            "urban_rural_code": row.get("urbanruralcode"),
            "urban_rural_desc": row.get("urbanruraldesc"),
            "fips_state": row.get("fipsstate"),
            "fips_county": row.get("fipscounty"),
            "fips_code": row.get("fipscode"),
            "indicator": row.get("indicator"),
            "all_deaths_total": parse_int(row.get("all_deaths_total")),
            "covid_19_deaths_total": parse_int(row.get("covid_19_deaths_total")),
            "non_hispanic_white": parse_float(row.get("non_hispanic_white")),
            "non_hispanic_black": parse_float(row.get("non_hispanic_black")),
            "non_hispanic_american_indian": parse_float(row.get("non_hispanic_american_indian")),
            "non_hispanic_asian": parse_float(row.get("non_hispanic_asian")),
            "non_hispanic_nhopi": parse_float(row.get("non_hispanic_nhopi")),
            "hispanic": parse_float(row.get("hispanic")),
            "other": parse_float(row.get("other")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
