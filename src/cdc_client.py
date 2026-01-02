# Add parent directory (connector root) to path for utils

"""CDC Socrata Open Data API client with rate limiting."""

from ratelimit import limits, sleep_and_retry
from subsets_utils import get

BASE_URL = "https://data.cdc.gov"

# CDC Socrata API: without app token, requests share a limited pool
# Be conservative with rate limiting
@sleep_and_retry
@limits(calls=5, period=1)
def rate_limited_get(endpoint, params=None, headers=None):
    """Make a rate-limited GET request to CDC Socrata API."""
    url = f"{BASE_URL}/{endpoint}"
    default_headers = {
        'Accept': 'application/json',
    }
    if headers:
        default_headers.update(headers)
    response = get(url, params=params, headers=default_headers, timeout=120.0)
    return response


def get_catalog():
    """
    Get list of all available datasets (views).

    Returns:
        List of dataset metadata
    """
    response = rate_limited_get('api/views')
    response.raise_for_status()
    return response.json()


def get_dataset(dataset_id, limit=50000, offset=0):
    """
    Get data from a specific dataset using SODA 2.0.

    Args:
        dataset_id: The dataset identifier (e.g., 'vbim-akqf')
        limit: Number of rows to fetch (max usually 50000)
        offset: Offset for pagination

    Returns:
        List of records
    """
    params = {
        '$limit': limit,
        '$offset': offset
    }

    response = rate_limited_get(
        f'resource/{dataset_id}.json',
        params=params
    )
    response.raise_for_status()
    return response.json()


def get_dataset_metadata(dataset_id):
    """
    Get metadata for a specific dataset.

    Args:
        dataset_id: The dataset identifier

    Returns:
        Dataset metadata dict
    """
    response = rate_limited_get(f'api/views/{dataset_id}.json')
    response.raise_for_status()
    return response.json()
