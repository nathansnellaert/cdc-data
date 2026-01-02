"""Tests for covid_deaths_county transform."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate the transformed data."""
    validate(table, {
        "not_null": [],
        "min_rows": 1000,
    })
    print("  Tests passed")
