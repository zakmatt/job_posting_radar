"""Nodes for the embedding pipeline."""

import logging
from collections.abc import Callable
from typing import Any

from job_posting_radar.config import AppSettings
from job_posting_radar.metrics import EMBEDDING_LATENCY_SECONDS, JOBS_EMBEDDED_TOTAL, push_metrics
from job_posting_radar.pipelines.embedding.embeddings import EmbeddingGenerator
from job_posting_radar.pipelines.normalization.models import NormalizedJobPosting

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


def generate_embeddings_node(
    normalized_postings: dict[str, Callable[[], dict[str, Any]] | dict[str, Any]],
    params: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Generate embeddings for normalized postings.

    Takes normalized job postings and generates vector embeddings for each.
    The embeddings are added to the posting data under the 'embedding' key.

    Args:
        normalized_postings: Dictionary mapping content_hash to posting dict (lazy loaders).
        params: Embedding parameters including 'batch_size'.

    Returns:
        Dictionary mapping content_hash to posting dict with 'embedding' added.
    """
    settings = AppSettings()
    generator = EmbeddingGenerator(settings=settings)
    batch_size = params.get("batch_size", 50)

    # Load all partitions first
    loaded_postings = {
        content_hash: _load_partition(partition)
        for content_hash, partition in normalized_postings.items()
    }

    items = list(loaded_postings.items())
    total_items = len(items)
    results: dict[str, dict[str, Any]] = {}

    logger.info("Generating embeddings", extra={"count": total_items, "batch_size": batch_size})

    for i in range(0, total_items, batch_size):
        batch = items[i : i + batch_size]
        batch_keys: list[str] = []
        batch_texts: list[str] = []
        batch_contents: list[dict[str, Any]] = []

        for content_hash, content in batch:
            try:
                model = NormalizedJobPosting(**content)
                batch_keys.append(content_hash)
                batch_texts.append(model.embedding_text)
                batch_contents.append(content)
            except Exception as exc:
                logger.warning(
                    "Failed to prepare posting for embedding",
                    extra={"content_hash": content_hash, "error": str(exc)},
                )

        if not batch_texts:
            continue

        with EMBEDDING_LATENCY_SECONDS.time():
            vectors = generator.generate(batch_texts)

        for key, content, vector in zip(batch_keys, batch_contents, vectors):
            content_with_embedding = content.copy()
            content_with_embedding["embedding"] = vector
            results[key] = content_with_embedding
            JOBS_EMBEDDED_TOTAL.labels(source=content.get("source", "unknown")).inc()

    push_metrics("embed", settings=settings)
    logger.info("Embeddings generated", extra={"count": len(results)})
    return results


def prepare_vector_records_node(
    embedded_postings: dict[str, Callable[[], dict[str, Any]] | dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Prepare records for vector database ingestion.

    Transforms embedded postings into the format required for Qdrant upsert.
    Each record contains point_id, vector, and payload.

    Args:
        embedded_postings: Dictionary mapping content_hash to posting dict with 'embedding' (lazy loaders).

    Returns:
        Dictionary mapping point_id to record dict with 'point_id', 'vector', 'payload'.
    """
    records: dict[str, dict[str, Any]] = {}

    for content_hash, partition in embedded_postings.items():
        try:
            content = _load_partition(partition)
            model = NormalizedJobPosting(**{k: v for k, v in content.items() if k != "embedding"})
            point_id = model.point_id
            vector = content.get("embedding")

            if vector is None:
                logger.warning(
                    "Missing embedding for posting",
                    extra={"content_hash": content_hash},
                )
                continue

            # Payload excludes the embedding (stored separately in Qdrant)
            payload = {k: v for k, v in content.items() if k != "embedding"}

            records[point_id] = {
                "point_id": point_id,
                "vector": vector,
                "payload": payload,
            }
        except Exception as exc:
            logger.warning(
                "Failed to prepare vector record",
                extra={"content_hash": content_hash, "error": str(exc)},
            )

    logger.info("Vector records prepared", extra={"count": len(records)})
    return records
