
from cdc_client import get_dataset
from subsets_utils import save_raw_json

# PLACES: Local Data for Better Health dataset ID
PLACES_COUNTY_ID = 'swc5-untb'


def run():
    """Fetch CDC PLACES county-level health indicators and save raw JSON"""
    print("  Fetching PLACES county health indicators...")

    all_records = []
    offset = 0
    limit = 50000

    while True:
        print(f"    Fetching rows {offset:,} to {offset + limit:,}...")
        batch = get_dataset(PLACES_COUNTY_ID, limit=limit, offset=offset)

        if not batch:
            break

        all_records.extend(batch)
        print(f"      Got {len(batch):,} rows")

        if len(batch) < limit:
            break

        offset += limit

    print(f"  Total: {len(all_records):,} records")

    save_raw_json(all_records, "health_indicators", compress=True)
