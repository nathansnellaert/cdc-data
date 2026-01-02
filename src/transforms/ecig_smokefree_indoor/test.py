"""Validation for CDC e-cigarette smokefree indoor legislation dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate e-cigarette smokefree indoor legislation dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 100000,
    })

    # Year range validation
    years = [int(y) for y in table.column("year").to_pylist() if y]
    assert min(years) >= 1995, f"Years before 1995 unexpected: {min(years)}"
    assert max(years) <= 2030, f"Years too far in future: {max(years)}"

    # State abbreviations should be 2 characters
    states = set(table.column("state_abbr").to_pylist())
    for state in states:
        if state:
            assert len(state) == 2, f"Invalid state abbreviation: {state}"

    # Should have all US states + DC
    assert len(states) >= 50, f"Expected at least 50 states, got {len(states)}"

    # Quarter validation
    quarters = [q for q in table.column("quarter").to_pylist() if q is not None]
    assert all(1 <= q <= 4 for q in quarters), "Quarters should be 1-4"
