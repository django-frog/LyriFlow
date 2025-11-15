import os
import json
from typing import Any

BASE_CACHE_DIR = "data/cache"


def _ensure_namespace(namespace: str):
    """Ensure the namespace directory exists."""
    path = f"{BASE_CACHE_DIR}/{namespace}"
    os.makedirs(path, exist_ok=True)
    return path


def _cache_path(namespace: str, key: str) -> str:
    """Return the full path to a cached file."""
    dir_path = _ensure_namespace(namespace)
    return f"{dir_path}/{key}.json"


def cache_save(namespace: str, key: str, data: dict) -> None:
    """
    Save a dictionary to cache.
    namespace: e.g. "lyrics", "preprocessed", "sentiment"
    key: usually song_id
    """
    path = _cache_path(namespace, key)
    with open(path, "w") as f:
        json.dump(data, f)


def cache_load(namespace: str, key: str) -> dict | None:
    """
    Load dictionary from cache.
    Returns None if not found.
    """
    path = _cache_path(namespace, key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None  # corrupted file → ignore
