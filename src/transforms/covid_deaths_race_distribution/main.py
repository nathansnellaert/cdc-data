"""Transform COVID-19 Deaths Distribution by Race dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date
from .test import test

DATASET_ID = "cdc_covid_deaths_race_distribution"
SOURCE_ID = "pj7m-y5uh"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths: Distribution by Race and Hispanic Origin",
    "description": (
        "Distribution of COVID-19 deaths by race and Hispanic origin at national and state level. "
        "Includes counts and weighted distribution percentages by demographic groups. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "start_week": "Start date of period (YYYY-MM-DD)",
        "end_week": "End date of period (YYYY-MM-DD)",
        "year": "Year or year range",
        "group": "Grouping type (By Total, By Year, By Month)",
        "state": "State name",
        "indicator": "Indicator type (Count, Distribution percentage)",
        "non_hispanic_white": "Non-Hispanic White count or percentage",
        "non_hispanic_black": "Non-Hispanic Black/African American count or percentage",
        "non_hispanic_american_indian": "Non-Hispanic American Indian/Alaska Native count or percentage",
        "non_hispanic_asian_pacific_islander": "Non-Hispanic Asian/Pacific Islander count or percentage",
        "nh_nhopi": "Non-Hispanic Native Hawaiian/Other Pacific Islander count or percentage",
        "non_hispanic_more_than_one_race": "Non-Hispanic More Than One Race count or percentage",
        "hispanic_latino_total": "Hispanic/Latino Total count or percentage",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths race distribution data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_week": parse_date(row.get("start_week")),
            "end_week": parse_date(row.get("end_week")),
            "year": row.get("year"),
            "group": row.get("group"),
            "state": row.get("state"),
            "indicator": row.get("indicator"),
            "non_hispanic_white": parse_int(row.get("non_hispanic_white")),
            "non_hispanic_black": parse_int(row.get("non_hispanic_black_african_american")),
            "non_hispanic_american_indian": parse_int(row.get("non_hispanic_american_indian_alaska_native")),
            "non_hispanic_asian_pacific_islander": parse_int(row.get("non_hispanic_asian_pacific_islander")),
            "nh_nhopi": parse_int(row.get("nh_nhopi")),
            "non_hispanic_more_than_one_race": parse_int(row.get("non_hispanic_more_than_one_race")),
            "hispanic_latino_total": parse_int(row.get("hispanic_latino_total")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
