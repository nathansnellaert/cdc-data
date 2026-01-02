"""Transform NHIS Vision and Eye Health Surveillance dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, COLUMN_DESC
from .test import test

DATASET_ID = "cdc_nhis_vision"
SOURCE_ID = "2t2r-sf6s"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC NHIS Vision and Eye Health Surveillance",
    "description": (
        "National Health Interview Survey (NHIS) data on vision and eye health, "
        "including prevalence of eye conditions, vision care utilization, and "
        "demographic breakdowns by age, sex, and race/ethnicity. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "year_start": "Survey year start",
        "year_end": "Survey year end",
        "location_abbr": COLUMN_DESC["state_abbr"],
        "location_desc": "Location description",
        "data_source": "Data source (NHIS)",
        "topic": "Health topic category",
        "category": "Specific category within topic",
        "question": "Survey question",
        "response": "Response category",
        "age": "Age group",
        "sex": "Sex",
        "race_ethnicity": "Race and ethnicity",
        "risk_factor": "Risk factor category",
        "risk_factor_response": "Risk factor response",
        "data_value_unit": "Unit of measurement",
        "data_value_type": "Type of data value (crude/age-adjusted)",
        "data_value": "Data value (percentage or count)",
        "confidence_limit_low": "Lower 95% confidence limit",
        "confidence_limit_high": "Upper 95% confidence limit",
        "sample_size": "Survey sample size",
    },
}


def run():
    """Transform, validate, and upload NHIS vision data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "year_start": parse_int(row.get("yearstart")),
            "year_end": parse_int(row.get("yearend")),
            "location_abbr": row.get("locationabbr"),
            "location_desc": row.get("locationdesc"),
            "data_source": row.get("datasource"),
            "topic": row.get("topic"),
            "category": row.get("category"),
            "question": row.get("question"),
            "response": row.get("response"),
            "age": row.get("age"),
            "sex": row.get("sex"),
            "race_ethnicity": row.get("raceethnicity"),
            "risk_factor": row.get("riskfactor"),
            "risk_factor_response": row.get("riskfactorresponse"),
            "data_value_unit": row.get("data_value_unit"),
            "data_value_type": row.get("data_value_type"),
            "data_value": parse_float(row.get("data_value")),
            "confidence_limit_low": parse_float(row.get("low_confidence_limit")),
            "confidence_limit_high": parse_float(row.get("high_confidence_limit")),
            "sample_size": parse_int(row.get("sample_size")),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
