from httpx import HTTPStatusError
from tqdm import tqdm

from cdc_client import get_dataset, get_dataset_metadata
from subsets_utils import save_raw_json, load_state, save_state
from selected_datasets import SELECTED_DATASETS


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

        # Fetch all data with pagination
        all_rows = []
        offset = 0
        limit = 50000

        while True:
            rows = get_dataset(dataset_id, limit=limit, offset=offset)
            if not rows:
                break
            all_rows.extend(rows)
            offset += limit
            if len(rows) < limit:
                break

        tqdm.write(f"  {dataset_id}: {name[:50]}... ({len(all_rows):,} rows)")

        # Save dataset with metadata
        save_raw_json({
            "id": dataset_id,
            "name": name,
            "score": score,
            "metadata": metadata,
            "data": all_rows,
        }, f"dataset_{dataset_id}", compress=True)

        # Update state after each save
        completed.add(dataset_id)
        save_state("raw_data", {"completed": list(completed), "skipped": list(skipped)})

    print(f"  Done. Fetched {len(pending)} datasets ({len(skipped)} skipped).")
