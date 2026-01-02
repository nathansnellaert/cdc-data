"""Transform COVID-19 Test Positivity dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_covid_test_positivity"
SOURCE_ID = "gvsb-yw6g"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC COVID-19 Test Positivity by Region",
    "description": (
        "Weekly percent positivity of COVID-19 nucleic acid amplification tests (NAAT) by HHS region "
        "from the National Respiratory and Enteric Virus Surveillance System (NREVSS). "
        "Includes test volumes and rolling averages. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "region": "Geographic level (National or HHS Region 1-10)",
        "week_ending": "End date of the surveillance week (YYYY-MM-DD)",
        "percent_positive": "Weekly percent positivity",
        "percent_positive_2wk": "Two-week rolling percent positivity",
        "percent_positive_4wk": "Four-week rolling percent positivity",
        "percent_change": "Percentage point change from previous period",
        "tests_count": "Number of tests performed in the week",
        "tests_count_2wk": "Number of tests over two weeks",
        "tests_count_4wk": "Number of tests over four weeks",
    },
}


def run():
    """Transform, validate, and upload COVID test positivity data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "region": row.get("level"),
            "week_ending": parse_date(row.get("mmwrweek_end")),
            "percent_positive": parse_float(row.get("percent_pos")),
            "percent_positive_2wk": parse_float(row.get("percent_pos_2_week")),
            "percent_positive_4wk": parse_float(row.get("percent_pos_4_week")),
            "percent_change": parse_float(row.get("perc_diff")),
            "tests_count": parse_int(row.get("number_tested")),
            "tests_count_2wk": parse_int(row.get("number_tested_2_week")),
            "tests_count_4wk": parse_int(row.get("number_tested_4_week")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
