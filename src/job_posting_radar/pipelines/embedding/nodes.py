"""Nodes for the embedding pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from job_posting_radar.vector.embeddings import EmbeddingGenerator
from job_posting_radar.vector.store import VectorStore
from job_posting_radar.config import AppSettings
from job_posting_radar.metrics import JOBS_EMBEDDED_TOTAL, EMBEDDING_LATENCY_SECONDS, push_metrics
from job_posting_radar.pipelines.normalization.models import NormalizedJobPosting

logger = logging.getLogger(__name__)


def embed_and_upsert_node(
    normalized_postings: Dict[str, Any],
    params: Dict[str, Any],
) -> None:
    """Embed normalized postings and upsert to Qdrant.

    Args:
        normalized_postings: Dictionary of normalized postings.
        params: Embedding parameters.
    """
    settings = AppSettings()
    generator = EmbeddingGenerator(settings=settings)
    store = VectorStore(settings=settings)
    
    batch_size = params.get("batch_size", 50)
    items = list(normalized_postings.items())
    total_items = len(items)
    
    logger.info(f"Embedding and upserting {total_items} postings")

    for i in range(0, total_items, batch_size):
        batch = items[i : i + batch_size]
        batch_ids = []
        batch_texts = []
        batch_payloads = []
        batch_sources = []

        for partition_id, content in batch:
            try:
                # content is already a dict from model_dump()
                # We need point_id and embedding_text which are computed fields
                # Re-instantiate model to get computed fields if they are not in the dict
                # Wait, model_dump() doesn't include computed fields by default in Pydantic v2
                # unless specified. Let's re-instantiate.
                model = NormalizedJobPosting(**content)
                
                point_id = model.point_id
                embedding_text = model.embedding_text
                
                batch_ids.append(point_id)
                batch_texts.append(embedding_text)
                batch_payloads.append(content)
                batch_sources.append(content.get("source", "unknown"))
            except Exception as exc:
                logger.warning(f"Failed to process posting {partition_id}", extra={"error": str(exc)})

        if not batch_texts:
            continue

        # Generate embeddings
        with EMBEDDING_LATENCY_SECONDS.time():
            vectors = generator.generate(batch_texts)

        # Upsert to Qdrant
        store.upsert_postings(ids=batch_ids, vectors=vectors, payloads=batch_payloads)
        
        # Metrics
        for source in batch_sources:
            JOBS_EMBEDDED_TOTAL.labels(source=source).inc()

    push_metrics("embed", settings=settings)
    logger.info(f"Successfully upserted {total_items} postings to Qdrant")

