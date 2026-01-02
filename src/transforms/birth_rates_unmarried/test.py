"""Validation for CDC birth rates for unmarried women dataset."""

import pyarrow as pa

from subsets_utils import validate
from subsets_utils.testing import assert_valid_year, assert_positive, assert_in_range


def test(table: pa.Table) -> None:
    """Validate birth rates dataset output."""
    # Schema validation - all columns must be present
    validate(table, {
        "not_null": [],
        "min_rows": 100,
    })

    # Year format validation
    assert_valid_year(table, "year")

    # Year range validation
    years = [int(y) for y in table.column("year").to_pylist()]
    assert min(years) >= 1970, f"Years before 1970 unexpected: {min(years)}"
    assert max(years) <= 2025, f"Future years unexpected: {max(years)}"

    # Birth rate should be positive (where not null)
    birth_rates = [r for r in table.column("birth_rate").to_pylist() if r is not None]
    assert all(r >= 0 for r in birth_rates), "Birth rates must be non-negative"
    assert all(r <= 500 for r in birth_rates), "Birth rates above 500 are suspect"

    # Check expected age groups exist
    age_groups = set(table.column("age_group").to_pylist())
    assert len(age_groups) >= 3, f"Expected at least 3 age groups, got {len(age_groups)}"

    # Check expected race/ethnicity categories exist
    races = set(table.column("race_ethnicity").to_pylist())
    assert len(races) >= 3, f"Expected at least 3 race/ethnicity categories, got {len(races)}"
