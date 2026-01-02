"""Transform COVID-NET Hospitalizations Patient Characteristics dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float
from .test import test

DATASET_ID = "cdc_covidnet_hospitalizations"
SOURCE_ID = "bigw-pgk2"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC COVID-NET Patient Characteristics of Hospitalizations",
    "description": (
        "COVID-NET surveillance data on patient characteristics of laboratory-confirmed "
        "COVID-19 hospitalizations. Includes stratifications by race/ethnicity, age, sex, "
        "ICU admission, ventilation, and outcomes. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "season": "Surveillance season year",
        "strata": "Stratification category (Race/Ethnicity, Age, Sex)",
        "age_category": "Age category",
        "race_ethnicity": "Race and ethnicity",
        "sex": "Sex",
        "covid": "COVID status indicator",
        "icu": "ICU admission indicator",
        "medical_condition": "Underlying medical condition",
        "mechanical_ventilation": "Mechanical ventilation indicator",
        "death": "Death indicator",
        "time_period": "Time period type (Week, Month)",
        "time": "Time value",
        "estimate_type": "Type of estimate (Counts, Rates)",
        "estimate": "Estimate value",
    },
}


def run():
    """Transform, validate, and upload COVID-NET hospitalizations data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "season": parse_float(row.get("season")),
            "strata": row.get("strata"),
            "age_category": row.get("age_category"),
            "race_ethnicity": row.get("race_ethnicity"),
            "sex": row.get("sex"),
            "covid": row.get("covid"),
            "icu": row.get("icu"),
            "medical_condition": row.get("medical_condition"),
            "mechanical_ventilation": row.get("mechanical_ventilation"),
            "death": row.get("death"),
            "time_period": row.get("time_period"),
            "time": row.get("time"),
            "estimate_type": row.get("estimate_type"),
            "estimate": parse_float(row.get("estimate")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
