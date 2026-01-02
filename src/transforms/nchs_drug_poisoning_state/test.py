"""Tests for nchs_drug_poisoning_state transform."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate the transformed data."""
    validate(table, {
        "not_null": [],
        "min_rows": 2500,
    })
    print("  Tests passed")
