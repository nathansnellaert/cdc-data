
from cdc_client import get_catalog
from subsets_utils import save_raw_json


def run():
    """Fetch CDC dataset catalogue and save raw JSON"""
    print("  Fetching dataset catalogue...")

    datasets = get_catalog()

    print(f"  Found {len(datasets):,} datasets")

    save_raw_json(datasets, "datasets")
