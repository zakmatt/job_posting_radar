"""Nodes for the upsert pipeline."""

import logging
from collections.abc import Callable
from typing import Any

from job_posting_radar.config import AppSettings
from job_posting_radar.metrics import push_metrics
from job_posting_radar.pipelines.upsert.store import VectorStore

logger = logging.getLogger(__name__)


def _load_partition(partition: Callable[[], dict[str, Any]] | dict[str, Any]) -> dict[str, Any]:
    """Load partition data, handling both lazy loaders and direct data.

    Args:
        partition: Either a callable that returns data or the data itself.

    Returns:
        The loaded partition data.
    """
    if callable(partition):
        return partition()
    return partition


def upsert_to_qdrant_node(
    vector_records: dict[str, Callable[[], dict[str, Any]] | dict[str, Any]],
    params: dict[str, Any],
) -> None:
    """Upsert vector records to Qdrant.

    Takes prepared vector records and upserts them to the Qdrant collection.

    Args:
        vector_records: Dictionary mapping point_id to record with 'point_id', 'vector', 'payload' (lazy loaders).
        params: Upsert parameters including 'batch_size'.
    """
    settings = AppSettings()
    store = VectorStore(settings=settings)
    batch_size = params.get("batch_size", 50)

    # Load all partitions
    loaded_records = [_load_partition(partition) for partition in vector_records.values()]
    total_items = len(loaded_records)

    logger.info("Upserting to Qdrant", extra={"count": total_items})

    for i in range(0, total_items, batch_size):
        batch = loaded_records[i : i + batch_size]

        batch_ids = [record["point_id"] for record in batch]
        batch_vectors = [record["vector"] for record in batch]
        batch_payloads = [record["payload"] for record in batch]

        store.upsert_postings(ids=batch_ids, vectors=batch_vectors, payloads=batch_payloads)

    push_metrics("upsert", settings=settings)
    logger.info("Upsert complete", extra={"count": total_items})
