"""Custom metrics for the job posting radar pipeline."""

from __future__ import annotations

import logging
from typing import Optional

from prometheus_client import CollectorRegistry, Counter, Histogram, push_to_gateway

from job_posting_radar.config import AppSettings

logger = logging.getLogger(__name__)

# Registry for pushgateway
REGISTRY = CollectorRegistry()

# Metrics definitions
JOBS_INGESTED_TOTAL = Counter(
    "jobs_ingested_total",
    "Total number of jobs successfully ingested",
    ["source"],
    registry=REGISTRY,
)

JOBS_NORMALIZED_TOTAL = Counter(
    "jobs_normalized_total",
    "Total number of jobs successfully normalized",
    ["source"],
    registry=REGISTRY,
)

JOBS_EMBEDDED_TOTAL = Counter(
    "jobs_embedded_total",
    "Total number of jobs successfully embedded and upserted",
    ["source"],
    registry=REGISTRY,
)

INGESTION_ERRORS_TOTAL = Counter(
    "ingestion_errors_total",
    "Total number of failed ingestion attempts",
    ["source"],
    registry=REGISTRY,
)

NORMALIZATION_ERRORS_TOTAL = Counter(
    "normalization_errors_total",
    "Total number of failed normalization attempts",
    ["source"],
    registry=REGISTRY,
)

EMBEDDING_LATENCY_SECONDS = Histogram(
    "embedding_latency_seconds",
    "Time spent generating embeddings",
    registry=REGISTRY,
)


def push_metrics(job_name: str, settings: Optional[AppSettings] = None) -> None:
    """Push metrics to the Prometheus Pushgateway.

    Args:
        job_name: The name of the batch job (e.g., 'ingest', 'normalize').
        settings: Application settings. Defaults to AppSettings().
    """
    settings = settings or AppSettings()
    gateway_url = f"{settings.pushgateway_host}:{settings.pushgateway_port}"
    
    try:
        # We use grouping_key to keep metrics separate per job
        push_to_gateway(gateway_url, job=job_name, registry=REGISTRY)
        logger.debug("Pushed metrics to gateway", extra={"job": job_name, "url": gateway_url})
    except Exception as exc:
        logger.warning("Failed to push metrics to gateway", extra={"error": str(exc), "url": gateway_url})

