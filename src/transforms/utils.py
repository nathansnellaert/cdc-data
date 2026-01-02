"""Shared utilities for CDC data transforms."""

import pyarrow as pa


def fix_null_columns(table: pa.Table) -> pa.Table:
    """Cast columns with null type to string type.

    Delta Lake doesn't support null-typed columns. This happens when
    a column has all null values. This function casts such columns to string.
    """
    new_columns = []
    changed = False

    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_null(field.type):
            # Cast null column to string
            col = col.cast(pa.string())
            changed = True
        new_columns.append(col)

    if changed:
        return pa.Table.from_arrays(new_columns, names=table.column_names)
    return table


def parse_float(value: str) -> float | None:
    """Parse a string to float, returning None for invalid values."""
    if not value or value in ("N/A", "null", "-", "", "-99", "NULL"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_int(value: str) -> int | None:
    """Parse a string to int, returning None for invalid values."""
    if not value or value in ("N/A", "null", "-", "", "NULL"):
        return None
    try:
        return int(str(value).replace(",", ""))
    except (ValueError, TypeError):
        return None


def parse_date(value: str) -> str | None:
    """Parse datetime string to ISO date format YYYY-MM-DD."""
    if not value:
        return None
    # Handle ISO format like 2023-10-29T00:00:00.000
    if "T" in value:
        return value.split("T")[0]
    # Handle format like "2025-04-26 00:00:00"
    if " " in value:
        return value.split(" ")[0]
    return value


def parse_mmddyyyy(value: str) -> str | None:
    """Parse MM/DD/YYYY date string to ISO format YYYY-MM-DD."""
    if not value or value in ("N/A", "null", "-", ""):
        return None
    parts = value.split("/")
    if len(parts) == 3:
        month, day, year = parts
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return value


# Common column descriptions that appear across many datasets
COLUMN_DESC = {
    # State/geography
    "state_abbr": "Two-letter state abbreviation",
    "state_name": "Full state name",
    "county_name": "County name",
    "county_fips": "County FIPS code",
    "fips": "Full FIPS code (state + county)",

    # Time
    "year": "Year (YYYY)",
    "quarter": "Quarter of the year (1-4)",
    "month": "Month (1-12)",
    "week_ending_date": "End date of the surveillance week (YYYY-MM-DD)",
    "season": "Respiratory/flu season (e.g., 2022-23)",
    "mmwr_year": "MMWR epidemiological year",
    "mmwr_week": "MMWR epidemiological week number (1-53)",

    # Demographics
    "age_category": "Age group category",
    "sex": "Sex category (Male, Female, All)",
    "race_ethnicity": "Race and/or ethnicity category",

    # Rates
    "weekly_rate": "Rate per 100,000 population for that week",
    "cumulative_rate": "Cumulative rate per 100,000 for the period",
    "rate_type": "Type of rate calculation (Crude Rate, Age-Adjusted Rate)",

    # Legislation (STATE System)
    "topic": "Legislation topic category",
    "measure": "Specific measure being tracked",
    "provision_group": "Group category of the provision",
    "provision": "Specific provision description",
    "provision_value": "Value or status of the provision",
    "provision_alt_value": "Alternative value representation",
    "data_type": "Type of data (Number, Yes/No, Ranking, etc.)",
    "citation": "Legal citation for the legislation",
    "enacted_date": "Date the legislation was enacted (YYYY-MM-DD)",
    "effective_date": "Date the legislation became effective (YYYY-MM-DD)",
}
