"""Embedding pipeline."""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import embed_and_upsert_node


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=embed_and_upsert_node,
                inputs=["normalized_postings", "params:embedding"],
                outputs=None,
                name="embed_and_upsert_node",
            ),
        ]
    )

