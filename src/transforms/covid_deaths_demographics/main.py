"""Transform Provisional COVID-19 Deaths by Demographics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_covid_deaths_demographics"
SOURCE_ID = "dmnu-8erf"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional COVID-19 Deaths by Demographics",
    "description": (
        "Provisional COVID-19 death counts and rates by jurisdiction of residence "
        "with demographic stratifications by sex, age, and race/ethnicity. "
        "Includes crude and age-adjusted rates, both period and annualized. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date data was retrieved",
        "jurisdiction": "Jurisdiction of residence",
        "period_start": "Data period start date",
        "period_end": "Data period end date",
        "group": "Demographic group (Sex, Age, Race/Ethnicity)",
        "subgroup1": "Specific demographic subgroup",
        "covid_deaths": "Number of COVID-19 deaths",
        "crude_rate": "Crude COVID death rate per 100,000",
        "age_adjusted_rate": "Age-adjusted COVID death rate per 100,000",
        "crude_rate_annualized": "Annualized crude rate",
        "age_adjusted_rate_annualized": "Annualized age-adjusted rate",
    },
}


def run():
    """Transform, validate, and upload COVID deaths demographics data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "jurisdiction": row.get("jurisdiction_residence"),
            "period_start": parse_date(row.get("data_period_start")),
            "period_end": parse_date(row.get("data_period_end")),
            "group": row.get("group"),
            "subgroup1": row.get("subgroup1"),
            "covid_deaths": parse_int(row.get("covid_deaths")),
            "crude_rate": parse_float(row.get("crude_covid_rate")),
            "age_adjusted_rate": parse_float(row.get("aa_covid_rate")),
            "crude_rate_annualized": parse_float(row.get("crude_covid_rate_ann")),
            "age_adjusted_rate_annualized": parse_float(row.get("aa_covid_rate_ann")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
