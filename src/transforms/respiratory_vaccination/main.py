"""Transform Weekly Respiratory Virus Vaccination Data dataset."""

import pyarrow as pa

from subsets_utils import upload_data, publish, load_raw_json
from ..utils import parse_float, parse_int, parse_date
from .test import test

DATASET_ID = "cdc_respiratory_vaccination"
SOURCE_ID = "5c6r-xi2t"

METADATA = {
    "id": DATASET_ID,
    "title": "CDC Weekly Respiratory Virus Vaccination Data (NIS)",
    "description": (
        "Weekly respiratory virus vaccination data for children and adults from the "
        "National Immunization Survey (NIS). Covers flu, COVID-19, and RSV vaccines "
        "with demographic breakdowns. "
        f"Source dataset ID: {SOURCE_ID}"
    ),
    "column_descriptions": {
        "vaccine": "Vaccine type (FLU, COVID, RSV)",
        "influenza_season": "Influenza season (if applicable)",
        "geographic_level": "Geographic level (National, Regional, State)",
        "geographic_name": "Geographic area name",
        "demographic_level": "Demographic stratification level",
        "demographic_name": "Demographic category",
        "indicator_label": "Vaccination status indicator",
        "indicator_category_label": "Indicator category (Yes, No)",
        "month_week": "Month and week label",
        "week_ending": "Week ending date",
        "estimate": "Vaccination estimate percentage",
        "ci_half_width_90pct": "90% confidence interval half-width",
        "ci_half_width_95pct": "95% confidence interval half-width",
        "sample_size_unweighted": "Unweighted sample size",
        "suppression_flag": "Data suppression indicator",
        "data_source": "Data source (NIS-ACM)",
    },
}


def run():
    """Transform, validate, and upload respiratory vaccination data."""
    raw = load_raw_json(f"dataset_{SOURCE_ID}")
    data = raw["data"]

    records = []
    for row in data:
        records.append({
            "vaccine": row.get("vaccine"),
            "influenza_season": row.get("influenza_season"),
            "geographic_level": row.get("geographic_level"),
            "geographic_name": row.get("geographic_name"),
            "demographic_level": row.get("demographic_level"),
            "demographic_name": row.get("demographic_name"),
            "indicator_label": row.get("indicator_label"),
            "indicator_category_label": row.get("indicator_category_label"),
            "month_week": row.get("month_week"),
            "week_ending": parse_date(row.get("week_ending")),
            "estimate": parse_float(row.get("nd_weekly_estimate")),
            "ci_half_width_90pct": parse_float(row.get("ci_half_width_90pct")),
            "ci_half_width_95pct": parse_float(row.get("ci_half_width_95pct")),
            "sample_size_unweighted": parse_int(row.get("n_unweighted")),
            "suppression_flag": row.get("suppression_flag"),
            "data_source": row.get("data_source"),
        })

    table = pa.Table.from_pylist(records)
    print(f"  Transformed {len(table):,} records")

    test(table)
    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
    print(f"  {DATASET_ID}: {len(table):,} rows published")


if __name__ == "__main__":
    run()
