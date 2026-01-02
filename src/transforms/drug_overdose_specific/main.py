"""Transform Provisional Drug Overdose Deaths by Specific Drug dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date
from .test import test

DATASET_ID = "cdc_drug_overdose_specific"
SOURCE_ID = "8hzs-zshh"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Provisional Drug Overdose Deaths by Specific Drug",
    "description": (
        "Provisional counts of drug overdose deaths for specific drugs including "
        "cocaine, heroin, fentanyl, methamphetamine, and prescription opioids. "
        "Data by jurisdiction with 12-month rolling totals. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "data_as_of": "Date data was retrieved",
        "death_year": "Year of death",
        "death_month": "Month of death",
        "jurisdiction": "State or jurisdiction",
        "drug_involved": "Specific drug involved in overdose death",
        "time_period": "Time period type (12 month-ending)",
        "month_ending_date": "End date of 12-month period",
        "drug_overdose_deaths": "Number of drug overdose deaths",
    },
}


def run():
    """Transform, validate, and upload drug overdose specific data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "data_as_of": parse_date(row.get("data_as_of")),
            "death_year": parse_int(row.get("death_year")),
            "death_month": parse_int(row.get("death_month")),
            "jurisdiction": row.get("jurisdiction_occurrence"),
            "drug_involved": row.get("drug_involved"),
            "time_period": row.get("time_period"),
            "month_ending_date": parse_date(row.get("month_ending_date")),
            "drug_overdose_deaths": parse_int(row.get("drug_overdose_deaths")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
