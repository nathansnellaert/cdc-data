"""Validation for CDC youth nutrition and obesity dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate youth nutrition and obesity dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 10000,
    })

    # Year range validation
    years = [int(y) for y in table.column("year_start").to_pylist() if y]
    assert min(years) >= 1990, f"Years before 1990 unexpected: {min(years)}"
    assert max(years) <= 2025, f"Years too far in future: {max(years)}"

    # State abbreviations should be 2-3 characters (includes territories like GU, VI)
    states = set(table.column("state_abbr").to_pylist())
    for state in states:
        if state:
            assert 2 <= len(state) <= 3, f"Invalid state abbreviation: {state}"

    # Should have most US states
    assert len(states) >= 40, f"Expected at least 40 states, got {len(states)}"

    # Data values should be percentages (0-100) where present
    values = [v for v in table.column("data_value").to_pylist() if v is not None]
    assert all(0 <= v <= 100 for v in values[:1000]), "Data values should be percentages (0-100)"

    # Should have multiple topic categories
    categories = set(table.column("topic_category").to_pylist())
    assert len(categories) >= 3, f"Expected at least 3 topic categories, got {len(categories)}"
