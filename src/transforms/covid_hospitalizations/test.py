"""Validation for CDC COVID-19 hospitalizations dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate COVID hospitalizations dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 10000,
    })

    # Season format validation (YYYY-YY pattern)
    seasons = set(table.column("season").to_pylist())
    for season in seasons:
        if season:
            assert "-" in season, f"Season should be in YYYY-YY format: {season}"

    # Week ending date format (YYYY-MM-DD)
    dates = [d for d in table.column("week_ending_date").to_pylist() if d][:100]
    for date in dates:
        parts = date.split("-")
        assert len(parts) == 3, f"Date should be YYYY-MM-DD: {date}"
        assert len(parts[0]) == 4, f"Year should be 4 digits: {date}"

    # Rates should be non-negative where present
    weekly_rates = [r for r in table.column("weekly_rate").to_pylist() if r is not None]
    assert all(r >= 0 for r in weekly_rates), "Weekly rates must be non-negative"

    cumulative_rates = [r for r in table.column("cumulative_rate").to_pylist() if r is not None]
    assert all(r >= 0 for r in cumulative_rates), "Cumulative rates must be non-negative"

    # Should have multiple states
    states = set(table.column("state").to_pylist())
    assert len(states) >= 10, f"Expected at least 10 states, got {len(states)}"
