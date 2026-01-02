"""Validation for CDC SARS-CoV-2 variant proportions dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate COVID variant proportions dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 100000,
    })

    # Date format validation (YYYY-MM-DD)
    dates = [d for d in table.column("week_ending").to_pylist() if d][:100]
    for date in dates:
        parts = date.split("-")
        assert len(parts) == 3, f"Date should be YYYY-MM-DD: {date}"
        assert len(parts[0]) == 4, f"Year should be 4 digits: {date}"

    # Share should be between 0 and 1
    shares = [s for s in table.column("share").to_pylist() if s is not None]
    assert all(0 <= s <= 1 for s in shares[:1000]), "Share values should be between 0 and 1"

    # Confidence intervals should be valid
    share_hi = [s for s in table.column("share_hi").to_pylist() if s is not None]
    share_lo = [s for s in table.column("share_lo").to_pylist() if s is not None]
    assert all(0 <= s <= 1 for s in share_hi[:1000]), "share_hi should be between 0 and 1"
    assert all(0 <= s <= 1 for s in share_lo[:1000]), "share_lo should be between 0 and 1"

    # Should have USA and HHS regions
    regions = set(table.column("region").to_pylist())
    assert "USA" in regions, "Should have USA as a region"

    # Should have multiple variants
    variants = set(table.column("variant").to_pylist())
    assert len(variants) >= 10, f"Expected at least 10 variants, got {len(variants)}"
