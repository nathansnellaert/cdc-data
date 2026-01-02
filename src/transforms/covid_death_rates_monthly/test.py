"""Tests for covid_death_rates_monthly transform."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate the transformed data."""
    validate(table, {
        "not_null": [],
        "min_rows": 50000,
    })
    print("  Tests passed")
