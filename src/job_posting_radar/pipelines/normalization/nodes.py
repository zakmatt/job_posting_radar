"""Nodes for the normalization pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from job_posting_radar.normalization.normalizers import normalize_nofluff, normalize_justjoin
from job_posting_radar.metrics import JOBS_NORMALIZED_TOTAL, NORMALIZATION_ERRORS_TOTAL, push_metrics
from job_posting_radar.config import AppSettings

logger = logging.getLogger(__name__)

SOURCE_NOFLUFF = "nofluff"
SOURCE_JUSTJOIN = "justjoin"


def normalize_postings_node(
    raw_nofluff: Dict[str, Any],
    raw_justjoin: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """Normalize raw postings from both sources.

    Args:
        raw_nofluff: Dictionary of NFJ partitions.
        raw_justjoin: Dictionary of JJ partitions.
        params: Normalization parameters.

    Returns:
        Dictionary mapping content_hash to normalized posting content.
    """
    settings = AppSettings()
    results = {}
    
    # Process NoFluff
    for partition_id, raw_content in raw_nofluff.items():
        try:
            # NFJ partitions are raw data
            # Kedro PartitionedDataset provides the content directly if loaded as dict
            normalized = normalize_nofluff(raw_content)
            results[normalized.content_hash] = normalized.model_dump()
            JOBS_NORMALIZED_TOTAL.labels(source=SOURCE_NOFLUFF).inc()
        except Exception as exc:
            NORMALIZATION_ERRORS_TOTAL.labels(source=SOURCE_NOFLUFF).inc()
            logger.warning(f"Failed to normalize NFJ {partition_id}", extra={"error": str(exc)})

    # Process JustJoin
    for partition_id, raw_content in raw_justjoin.items():
        try:
            normalized = normalize_justjoin(raw_content)
            results[normalized.content_hash] = normalized.model_dump()
            JOBS_NORMALIZED_TOTAL.labels(source=SOURCE_JUSTJOIN).inc()
        except Exception as exc:
            NORMALIZATION_ERRORS_TOTAL.labels(source=SOURCE_JUSTJOIN).inc()
            logger.warning(f"Failed to normalize JJ {partition_id}", extra={"error": str(exc)})

    push_metrics("normalize", settings=settings)
    return results

