"""Transform COVID-19 Deaths by Jurisdiction dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_covid_deaths_jurisdiction"
SOURCE_ID = "mpx5-t7tu"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Death Counts, Rates, and Percent by Jurisdiction",
    "description": (
        "Provisional COVID-19 death counts, crude and age-adjusted rates, and percent "
        "of total deaths by jurisdiction of residence. Includes multiple time groupings. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date of data snapshot (YYYY-MM-DD)",
        "jurisdiction": "Jurisdiction of residence",
        "group": "Time grouping (By Total, By Year, By Month, etc.)",
        "period_start": "Start date of period (YYYY-MM-DD)",
        "period_end": "End date of period (YYYY-MM-DD)",
        "covid_deaths": "Number of COVID-19 deaths",
        "covid_pct_of_total": "COVID-19 deaths as percent of total deaths",
        "crude_covid_rate": "Crude COVID-19 death rate per 100,000",
        "aa_covid_rate": "Age-adjusted COVID-19 death rate per 100,000",
        "crude_covid_rate_ann": "Annualized crude COVID-19 death rate",
        "aa_covid_rate_ann": "Annualized age-adjusted COVID-19 death rate",
    },
}


def run():
    """Transform, validate, and upload COVID-19 deaths by jurisdiction data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "jurisdiction": row.get("jurisdiction_residence"),
            "group": row.get("group"),
            "period_start": parse_date(row.get("data_period_start")),
            "period_end": parse_date(row.get("data_period_end")),
            "covid_deaths": parse_int(row.get("covid_deaths")),
            "covid_pct_of_total": parse_float(row.get("covid_pct_of_total")),
            "crude_covid_rate": parse_float(row.get("crude_covid_rate")),
            "aa_covid_rate": parse_float(row.get("aa_covid_rate")),
            "crude_covid_rate_ann": parse_float(row.get("crude_covid_rate_ann")),
            "aa_covid_rate_ann": parse_float(row.get("aa_covid_rate_ann")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
