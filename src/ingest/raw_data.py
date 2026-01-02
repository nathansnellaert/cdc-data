import gzip
import json
import os
from httpx import HTTPStatusError
from tqdm import tqdm

from cdc_client import get_dataset, get_dataset_metadata
from subsets_utils import load_state, save_state, get_data_dir
from selected_datasets import SELECTED_DATASETS

# Datasets too large to fit in memory - stream directly to disk
LARGE_DATASET_THRESHOLD = 200000  # Stream if > 200k rows


def stream_large_dataset(dataset_id: str, name: str, score: int, metadata: dict, first_batch: list) -> int:
    """Stream a large dataset directly to disk in NDJSON format to avoid OOM."""
    data_dir = get_data_dir()
    raw_dir = os.path.join(data_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)

    output_path = os.path.join(raw_dir, f"dataset_{dataset_id}.ndjson.gz")

    limit = 50000
    total_rows = 0

    with gzip.open(output_path, 'wt', encoding='utf-8') as f:
        # Write header record first
        header = {
            "_header": True,
            "id": dataset_id,
            "name": name,
            "score": score,
            "metadata": metadata,
        }
        f.write(json.dumps(header) + '\n')

        # Write first batch (already fetched)
        for row in first_batch:
            f.write(json.dumps(row) + '\n')
        total_rows += len(first_batch)
        offset = limit

        # Continue fetching remaining batches
        while True:
            rows = get_dataset(dataset_id, limit=limit, offset=offset)
            if not rows:
                break

            for row in rows:
                f.write(json.dumps(row) + '\n')

            total_rows += len(rows)
            offset += limit

            if len(rows) < limit:
                break

    return total_rows


def run():
    """Fetch raw data for selected high-quality CDC datasets."""
    state = load_state("raw_data")
    completed = set(state.get("completed", []))
    skipped = set(state.get("skipped", []))

    pending = [(id, score) for id, score in SELECTED_DATASETS.items()
               if id not in completed and id not in skipped]

    if not pending:
        print("  All datasets up to date")
        return

    print(f"  Fetching {len(pending)} datasets...")

    # Import here to avoid circular import
    from subsets_utils import save_raw_json

    for dataset_id, score in tqdm(pending, desc="Datasets", unit="ds"):
        try:
            metadata = get_dataset_metadata(dataset_id)
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                tqdm.write(f"  {dataset_id}: NOT FOUND (skipping)")
                skipped.add(dataset_id)
                save_state("raw_data", {"completed": list(completed), "skipped": list(skipped)})
                continue
            raise

        name = metadata.get("name", dataset_id)

        # Check estimated row count from metadata
        row_count_estimate = metadata.get("columns_field_name", metadata).get("row_count", 0)
        if isinstance(row_count_estimate, str):
            row_count_estimate = int(row_count_estimate) if row_count_estimate.isdigit() else 0

        # Fetch first batch to check size
        first_batch = get_dataset(dataset_id, limit=50000, offset=0)

        if not first_batch:
            tqdm.write(f"  {dataset_id}: {name[:50]}... (0 rows - skipping)")
            skipped.add(dataset_id)
            save_state("raw_data", {"completed": list(completed), "skipped": list(skipped)})
            continue

        # If first batch is full, dataset may be large - stream to avoid OOM
        if len(first_batch) == 50000:
            tqdm.write(f"  {dataset_id}: {name[:50]}... (streaming)")
            total_rows = stream_large_dataset(dataset_id, name, score, metadata, first_batch)
            tqdm.write(f"    -> {total_rows:,} rows")
        else:
            # Small dataset - save normally
            tqdm.write(f"  {dataset_id}: {name[:50]}... ({len(first_batch):,} rows)")
            save_raw_json({
                "id": dataset_id,
                "name": name,
                "score": score,
                "metadata": metadata,
                "data": first_batch,
            }, f"dataset_{dataset_id}", compress=True)

        # Update state after each save
        completed.add(dataset_id)
        save_state("raw_data", {"completed": list(completed), "skipped": list(skipped)})

    print(f"  Done. Fetched {len(pending)} datasets ({len(skipped)} skipped).")
