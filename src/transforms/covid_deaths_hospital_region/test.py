"""Validation for CDC COVID-19 deaths by hospital region dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate COVID deaths by HRR dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 10000,
    })

    # Year range validation
    years = [int(y) for y in table.column("mmwr_year").to_pylist() if y]
    assert min(years) >= 2015, f"Years before 2015 unexpected: {min(years)}"
    assert max(years) <= 2026, f"Years too far in future: {max(years)}"

    # MMWR week validation (1-53)
    weeks = [w for w in table.column("mmwr_week").to_pylist() if w is not None]
    assert all(1 <= w <= 53 for w in weeks), "MMWR weeks should be 1-53"

    # State abbreviations should be 2 characters
    states = set(table.column("state").to_pylist())
    for state in states:
        if state:
            assert len(state) == 2, f"Invalid state abbreviation: {state}"

    # Should have most US states
    assert len(states) >= 45, f"Expected at least 45 states, got {len(states)}"

    # Death counts should be non-negative
    for col in ["total_deaths", "covid_deaths"]:
        values = [v for v in table.column(col).to_pylist() if v is not None]
        assert all(v >= 0 for v in values), f"{col} must be non-negative"
