"""Nodes for the normalization pipeline."""

import logging
from collections.abc import Callable
from typing import Any

from job_posting_radar.config import AppSettings
from job_posting_radar.metrics import JOBS_NORMALIZED_TOTAL, NORMALIZATION_ERRORS_TOTAL, push_metrics
from job_posting_radar.pipelines.normalization.normalizers import normalize_justjoin, normalize_nofluff

logger = logging.getLogger(__name__)

SOURCE_NOFLUFF = "nofluff"
SOURCE_JUSTJOIN = "justjoin"


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


def normalize_postings_node(
    raw_nofluff: dict[str, Callable[[], dict[str, Any]] | dict[str, Any]],
    raw_justjoin: dict[str, Callable[[], dict[str, Any]] | dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Normalize raw postings from both sources.

    Args:
        raw_nofluff: Dictionary of NFJ partitions (values are lazy loaders).
        raw_justjoin: Dictionary of JJ partitions (values are lazy loaders).

    Returns:
        Dictionary mapping content_hash to normalized posting content.
    """
    settings = AppSettings()
    results: dict[str, dict[str, Any]] = {}

    # Process NoFluff
    for partition_id, partition in raw_nofluff.items():
        try:
            raw_content = _load_partition(partition)
            normalized = normalize_nofluff(raw_content)
            # Use mode="json" to serialize datetime objects as ISO strings
            results[normalized.content_hash] = normalized.model_dump(mode="json")
            JOBS_NORMALIZED_TOTAL.labels(source=SOURCE_NOFLUFF).inc()
        except Exception as exc:
            NORMALIZATION_ERRORS_TOTAL.labels(source=SOURCE_NOFLUFF).inc()
            logger.warning(
                "Failed to normalize NFJ posting",
                extra={"partition_id": partition_id, "error": str(exc)},
            )

    # Process JustJoin
    for partition_id, partition in raw_justjoin.items():
        try:
            raw_content = _load_partition(partition)
            normalized = normalize_justjoin(raw_content)
            # Use mode="json" to serialize datetime objects as ISO strings
            results[normalized.content_hash] = normalized.model_dump(mode="json")
            JOBS_NORMALIZED_TOTAL.labels(source=SOURCE_JUSTJOIN).inc()
        except Exception as exc:
            NORMALIZATION_ERRORS_TOTAL.labels(source=SOURCE_JUSTJOIN).inc()
            logger.warning(
                "Failed to normalize JJ posting",
                extra={"partition_id": partition_id, "error": str(exc)},
            )

    push_metrics("normalize", settings=settings)
    return results
