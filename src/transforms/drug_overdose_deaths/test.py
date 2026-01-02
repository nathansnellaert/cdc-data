"""Validation for CDC drug overdose deaths dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate drug overdose deaths dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 10000,
    })

    # Year range validation
    years = [int(y) for y in table.column("year").to_pylist() if y]
    assert min(years) >= 2015, f"Years before 2015 unexpected: {min(years)}"
    assert max(years) <= 2026, f"Years too far in future: {max(years)}"

    # State abbreviations should be 2 characters
    states = set(table.column("state_abbr").to_pylist())
    for state in states:
        if state and state not in ("US", "YC"):  # US = national, YC = New York City
            assert len(state) == 2, f"Invalid state abbreviation: {state}"

    # Should have most US states + DC + some territories
    assert len(states) >= 50, f"Expected at least 50 states, got {len(states)}"

    # Month names should be valid
    valid_months = {
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    }
    months = set(table.column("month").to_pylist())
    for month in months:
        if month:
            assert month in valid_months, f"Invalid month: {month}"

    # Death counts should be non-negative where present
    counts = [c for c in table.column("death_count").to_pylist() if c is not None]
    assert all(c >= 0 for c in counts), "Death counts must be non-negative"

    # Percentages should be in valid range
    pct_complete = [p for p in table.column("percent_complete").to_pylist() if p is not None]
    assert all(0 <= p <= 100 for p in pct_complete), "Percent complete should be 0-100"
