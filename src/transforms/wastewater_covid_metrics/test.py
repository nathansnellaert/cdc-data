"""Validation for CDC wastewater SARS-CoV-2 metrics dataset."""

import pyarrow as pa

from subsets_utils import validate


def test(table: pa.Table) -> None:
    """Validate wastewater COVID metrics dataset output."""
    # Schema validation
    validate(table, {
        "not_null": [],
        "min_rows": 100000,
    })

    # Date format validation (YYYY-MM-DD)
    dates = [d for d in table.column("date_end").to_pylist() if d][:100]
    for date in dates:
        parts = date.split("-")
        assert len(parts) == 3, f"Date should be YYYY-MM-DD: {date}"
        assert len(parts[0]) == 4, f"Year should be 4 digits: {date}"

    # Percentile should be between 0 and 100
    percentiles = [p for p in table.column("percentile").to_pylist() if p is not None]
    assert all(0 <= p <= 100 for p in percentiles[:1000]), "Percentile should be 0-100"

    # Detection proportion should be between 0 and 100
    detect_props = [d for d in table.column("detect_prop_15d").to_pylist() if d is not None]
    assert all(0 <= d <= 100 for d in detect_props[:1000]), "Detection proportion should be 0-100"

    # Should have multiple jurisdictions
    jurisdictions = set(table.column("jurisdiction").to_pylist())
    assert len(jurisdictions) >= 40, f"Expected at least 40 jurisdictions, got {len(jurisdictions)}"
