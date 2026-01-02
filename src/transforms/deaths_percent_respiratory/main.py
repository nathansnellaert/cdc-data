"""Transform Provisional Percent of Deaths for Respiratory Illnesses dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_deaths_percent_respiratory"
SOURCE_ID = "53g5-jf7x"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Percent of Deaths - COVID-19, Influenza, RSV",
    "description": (
        "Weekly provisional data on percent of deaths attributable to COVID-19, "
        "influenza, RSV, and combined respiratory illnesses. Includes breakdowns "
        "by race/ethnicity, age group, and geographic area. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date data was retrieved",
        "start_date": "Week start date",
        "end_date": "Week end date",
        "group": "Grouping type (By Week, By Month)",
        "year": "Year",
        "month": "Month",
        "mmwr_week": "MMWR epidemiological week number",
        "week_ending_date": "Week ending date",
        "state": "State or United States",
        "demographic_type": "Demographic stratification type",
        "demographic_values": "Demographic category value",
        "pathogen": "Respiratory pathogen (COVID-19, Influenza, RSV, Combined)",
        "deaths": "Number of deaths",
        "total_deaths": "Total deaths in period",
        "percent_deaths": "Percent of deaths from pathogen",
    },
}


def run():
    """Transform, validate, and upload respiratory deaths percent data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "start_date": parse_date(row.get("start_date")),
            "end_date": parse_date(row.get("end_date")),
            "group": row.get("group"),
            "year": parse_int(row.get("year")),
            "month": parse_int(row.get("month")),
            "mmwr_week": parse_int(row.get("mmwr_week")),
            "week_ending_date": parse_date(row.get("weekending_date")),
            "state": row.get("state"),
            "demographic_type": row.get("demographic_type"),
            "demographic_values": row.get("demographic_values"),
            "pathogen": row.get("pathogen"),
            "deaths": parse_float(row.get("deaths")),
            "total_deaths": parse_float(row.get("total_deaths")),
            "percent_deaths": parse_float(row.get("percent_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
