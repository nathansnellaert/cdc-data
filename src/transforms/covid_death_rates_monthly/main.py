"""Transform Monthly COVID-19 Death Rates by Demographics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_covid_death_rates_monthly"
SOURCE_ID = "exs3-hbne"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Monthly COVID-19 Death Rates by Demographics",
    "description": (
        "Monthly COVID-19 death rates per 100,000 population by HHS region with "
        "double stratification by age group, race/ethnicity, and sex. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date data was retrieved",
        "jurisdiction": "Jurisdiction of residence (HHS Region)",
        "period_start": "Data period start date",
        "period_end": "Data period end date",
        "group": "Primary stratification (Age, Race/Ethnicity, Sex)",
        "subgroup1": "Subgroup category",
        "covid_deaths": "Number of COVID-19 deaths",
        "crude_rate": "Crude death rate per 100,000",
        "confidence_limit_low": "Lower 95% confidence limit for crude rate",
        "confidence_limit_high": "Upper 95% confidence limit for crude rate",
    },
}


def run():
    """Transform, validate, and upload COVID death rates data."""
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
            "crude_rate": parse_float(row.get("crude_rate")),
            "confidence_limit_low": parse_float(row.get("conf_int_95pct_lower_crude")),
            "confidence_limit_high": parse_float(row.get("conf_int_95pct_upper_crude")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
