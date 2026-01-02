"""Analyze all CDC datasets to understand structure and calculate interest scores."""

import gzip
import json
from pathlib import Path
from datetime import datetime


def analyze_dataset(file_path: Path) -> dict:
    """Analyze a single dataset file."""
    with gzip.open(file_path, 'rt') as f:
        raw = json.load(f)

    dataset_id = raw.get('id', file_path.stem.replace('dataset_', '').replace('.json', ''))
    meta = raw.get('metadata', {})
    data = raw.get('data', [])

    # Extract basic info
    name = meta.get('name', raw.get('name', 'Unknown'))
    description = meta.get('description', '')
    category = meta.get('category', '')
    row_count = len(data)

    # Extract columns
    columns = list(data[0].keys()) if data else []

    # Find year range
    years = set()
    date_columns = []
    for col in columns:
        if 'year' in col.lower() or 'date' in col.lower():
            date_columns.append(col)

    for row in data:
        for col in date_columns:
            val = row.get(col)
            if val and isinstance(val, str):
                # Extract 4-digit year
                if len(val) == 4 and val.isdigit():
                    years.add(int(val))
                elif len(val) >= 4 and val[:4].isdigit():
                    years.add(int(val[:4]))

    min_year = min(years) if years else None
    max_year = max(years) if years else None
    year_span = (max_year - min_year + 1) if min_year and max_year else 0

    # Calculate interest score
    score = calculate_interest_score(
        row_count=row_count,
        column_count=len(columns),
        year_span=year_span,
        max_year=max_year,
        has_description=bool(description),
    )

    return {
        'id': dataset_id,
        'name': name,
        'description': description[:200] if description else '',
        'category': category,
        'row_count': row_count,
        'column_count': len(columns),
        'columns': columns,
        'min_year': min_year,
        'max_year': max_year,
        'year_span': year_span,
        'interest_score': score,
    }


def calculate_interest_score(row_count: int, column_count: int, year_span: int, max_year: int, has_description: bool) -> int:
    """Calculate interest score from 0-100."""
    score = 0

    # Row count (0-30 points)
    if row_count >= 10000:
        score += 30
    elif row_count >= 1000:
        score += 25
    elif row_count >= 100:
        score += 15
    elif row_count >= 10:
        score += 5

    # Column count (0-10 points)
    if 3 <= column_count <= 20:
        score += 10  # Good granularity
    elif column_count > 20:
        score += 5   # Complex but usable

    # Year span (0-25 points)
    if year_span >= 20:
        score += 25
    elif year_span >= 10:
        score += 20
    elif year_span >= 5:
        score += 10
    elif year_span >= 2:
        score += 5

    # Recency (0-25 points)
    current_year = datetime.now().year
    if max_year:
        if max_year >= current_year - 1:
            score += 25  # Very recent
        elif max_year >= current_year - 3:
            score += 15  # Recent
        elif max_year >= current_year - 5:
            score += 5   # Somewhat recent

    # Has description (0-10 points)
    if has_description:
        score += 10

    return min(100, score)


def main():
    data_dir = Path('data/raw')
    output_file = Path('src/dataset_scores.json')

    results = []

    for file_path in sorted(data_dir.glob('dataset_*.json.gz')):
        try:
            info = analyze_dataset(file_path)
            results.append(info)
            print(f"{info['interest_score']:3d} | {info['row_count']:>8,} rows | {info['year_span']:>3}y ({info['min_year']}-{info['max_year']}) | {info['name'][:60]}")
        except Exception as e:
            print(f"ERROR: {file_path.name} - {e}")

    # Sort by interest score descending
    results.sort(key=lambda x: x['interest_score'], reverse=True)

    # Save to file
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {len(results)} dataset scores to {output_file}")

    # Summary
    high_interest = [r for r in results if r['interest_score'] >= 70]
    medium_interest = [r for r in results if 40 <= r['interest_score'] < 70]
    low_interest = [r for r in results if r['interest_score'] < 40]

    print(f"\nSummary:")
    print(f"  High interest (>=70): {len(high_interest)}")
    print(f"  Medium interest (40-69): {len(medium_interest)}")
    print(f"  Low interest (<40): {len(low_interest)}")


if __name__ == '__main__':
    main()
