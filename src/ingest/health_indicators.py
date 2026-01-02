import gzip
import json
import os

from cdc_client import get_dataset
from subsets_utils import get_data_dir

# PLACES: Local Data for Better Health dataset ID
PLACES_COUNTY_ID = 'swc5-untb'


def run():
    """Fetch CDC PLACES county-level health indicators and stream to disk."""
    print("  Fetching PLACES county health indicators...")

    data_dir = get_data_dir()
    raw_dir = os.path.join(data_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    output_path = os.path.join(raw_dir, "health_indicators.ndjson.gz")

    offset = 0
    limit = 50000
    total_records = 0

    with gzip.open(output_path, 'wt', encoding='utf-8') as f:
        while True:
            print(f"    Fetching rows {offset:,} to {offset + limit:,}...")
            batch = get_dataset(PLACES_COUNTY_ID, limit=limit, offset=offset)

            if not batch:
                break

            for record in batch:
                f.write(json.dumps(record) + '\n')

            total_records += len(batch)
            print(f"      Got {len(batch):,} rows")

            if len(batch) < limit:
                break

            offset += limit

    print(f"  Total: {total_records:,} records")
    print(f"  -> R2: Saved health_indicators.ndjson.gz")
