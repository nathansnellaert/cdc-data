"""Validation for CDC county-level drug overdose deaths dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate county drug overdose deaths dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 10000,
    })

    # Year range validation
    years = [int(y) for y in table.column("year").to_pylist() if y]
    assert min(years) >= 2015, f"Years before 2015 unexpected: {min(years)}"
    assert max(years) <= 2026, f"Years too far in future: {max(years)}"

    # Month validation (1-12)
    months = [m for m in table.column("month").to_pylist() if m is not None]
    assert all(1 <= m <= 12 for m in months), "Months should be 1-12"

    # State abbreviations should be 2 characters
    states = set(table.column("state_abbr").to_pylist())
    for state in states:
        if state:
            assert len(state) == 2, f"Invalid state abbreviation: {state}"

    # Should have most US states
    assert len(states) >= 45, f"Expected at least 45 states, got {len(states)}"

    # FIPS codes should be reasonable length
    fips_codes = [f for f in table.column("fips").to_pylist() if f][:100]
    for fips in fips_codes:
        assert 4 <= len(fips) <= 5, f"FIPS should be 4-5 digits: {fips}"
