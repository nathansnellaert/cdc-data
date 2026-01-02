"""Tests for abcs_group_b_strep transform."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate the transformed data."""
    validate(table, {
        "not_null": [],
        "min_rows": 3000,
    })
    print("  Tests passed")
