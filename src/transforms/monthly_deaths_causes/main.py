"""Transform Monthly Provisional Deaths by Cause dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_int, parse_date
from .test import test

DATASET_ID = "cdc_monthly_deaths_causes"
SOURCE_ID = "65mz-jvh5"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Monthly Provisional Deaths by Select Causes",
    "description": (
        "Monthly provisional counts of deaths for select causes of death by sex, "
        "age, and race/ethnicity. Includes deaths from COVID-19, heart disease, "
        "cancer, respiratory diseases, diabetes, and more. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "analysis_date": "Date of analysis",
        "year": "Year of death",
        "month": "Month of death",
        "start_date": "Start date of period",
        "end_date": "End date of period",
        "jurisdiction": "Jurisdiction (United States)",
        "sex": "Sex (M, F)",
        "race_ethnicity": "Race and ethnicity",
        "age_group": "Age group",
        "all_cause": "All cause deaths",
        "natural_cause": "Natural cause deaths",
        "septicemia": "Deaths from septicemia (A40-A41)",
        "malignant_neoplasms": "Deaths from cancer (C00-C97)",
        "diabetes": "Deaths from diabetes (E10-E14)",
        "alzheimer": "Deaths from Alzheimer disease (G30)",
        "influenza_pneumonia": "Deaths from influenza and pneumonia (J09-J18)",
        "chronic_lower_respiratory": "Deaths from chronic lower respiratory diseases",
        "other_respiratory": "Deaths from other respiratory diseases",
        "nephritis": "Deaths from kidney disease",
        "symptoms_abnormal": "Deaths from symptoms and abnormal findings",
        "heart_disease": "Deaths from diseases of heart (I00-I09, I11, I13, I20-I51)",
        "cerebrovascular": "Deaths from cerebrovascular diseases",
        "covid_19_multiple": "COVID-19 deaths (multiple cause)",
        "covid_19_underlying": "COVID-19 deaths (underlying cause)",
    },
}


def run():
    """Transform, validate, and upload monthly deaths data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "analysis_date": parse_date(row.get("analysisdate")),
            "year": parse_int(row.get("date_of_death_year")),
            "month": parse_int(row.get("date_of_death_month")),
            "start_date": parse_date(row.get("start_date")),
            "end_date": parse_date(row.get("end_date")),
            "jurisdiction": row.get("jurisdiction_of_occurrence"),
            "sex": row.get("sex"),
            "race_ethnicity": row.get("race_ethnicity"),
            "age_group": row.get("agegroup"),
            "all_cause": parse_int(row.get("allcause")),
            "natural_cause": parse_int(row.get("naturalcause")),
            "septicemia": parse_int(row.get("septicemia_a40_a41")),
            "malignant_neoplasms": parse_int(row.get("malignant_neoplasms_c00_c97")),
            "diabetes": parse_int(row.get("diabetes_mellitus_e10_e14")),
            "alzheimer": parse_int(row.get("alzheimer_disease_g30")),
            "influenza_pneumonia": parse_int(row.get("influenza_and_pneumonia_j09")),
            "chronic_lower_respiratory": parse_int(row.get("chronic_lower_respiratory")),
            "other_respiratory": parse_int(row.get("other_diseases_of_respiratory")),
            "nephritis": parse_int(row.get("nephritis_nephrotic_syndrome")),
            "symptoms_abnormal": parse_int(row.get("symptoms_signs_and_abnormal")),
            "heart_disease": parse_int(row.get("diseases_of_heart_i00_i09")),
            "cerebrovascular": parse_int(row.get("cerebrovascular_diseases")),
            "covid_19_multiple": parse_int(row.get("covid_19_u071_multiple_cause")),
            "covid_19_underlying": parse_int(row.get("covid_19_u071_underlying")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
