"""Project pipeline registry."""
from typing import Dict

from kedro.pipeline import Pipeline
from job_posting_radar.pipelines import ingestion, normalization, embedding


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    ingest_pipeline = ingestion.create_pipeline()
    norm_pipeline = normalization.create_pipeline()
    embed_pipeline = embedding.create_pipeline()

    return {
        "ingest": ingest_pipeline,
        "normalize": norm_pipeline,
        "embed": embed_pipeline,
        "__default__": ingest_pipeline + norm_pipeline + embed_pipeline,
    }

