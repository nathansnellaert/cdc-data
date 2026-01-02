"""Transform Weekly Deaths by Cause dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_float, parse_date, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_weekly_deaths_cause"
SOURCE_ID = "u6jv-9ijr"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Weekly Deaths by Jurisdiction and Cause of Death",
    "description": (
        "Weekly counts of deaths by jurisdiction and select causes of death "
        "from the National Center for Health Statistics. "
        "Includes comparisons to 2015-2019 baseline averages. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "jurisdiction": COLUMN_DESC["state_name"],
        "state_abbr": COLUMN_DESC["state_abbr"],
        "week_ending_date": COLUMN_DESC["week_ending_date"],
        "mmwr_year": COLUMN_DESC["mmwr_year"],
        "mmwr_week": COLUMN_DESC["mmwr_week"],
        "cause_group": "Broad category of cause of death",
        "cause_subgroup": "Specific cause of death within the group",
        "death_count": "Number of deaths in the week",
        "avg_deaths_baseline": "Average deaths in same week during 2015-2019",
        "diff_from_baseline": "Difference from 2015-2019 baseline",
        "pct_diff_from_baseline": "Percent difference from 2015-2019 baseline",
    },
}


def run():
    """Transform, validate, and upload weekly deaths by cause data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "jurisdiction": row.get("jurisdiction"),
            "state_abbr": row.get("state_abbreviation"),
            "week_ending_date": parse_date(row.get("week_ending_date")),
            "mmwr_year": row.get("mmwryear"),
            "mmwr_week": row.get("mmwrweek"),
            "cause_group": row.get("cause_group"),
            "cause_subgroup": row.get("cause_subgroup"),
            "death_count": parse_int(row.get("number_of_deaths")),
            "avg_deaths_baseline": parse_int(row.get("average_number_of_deaths")),
            "diff_from_baseline": parse_int(row.get("difference_from_2015_2019_to_2020")),
            "pct_diff_from_baseline": parse_float(row.get("percent_difference_from_15_19_to_20")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
