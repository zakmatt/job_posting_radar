"""Upsert pipeline."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import upsert_to_qdrant_node


def create_pipeline(**kwargs) -> Pipeline:
    """Create the upsert pipeline.

    Persists vector records to Qdrant vector database.

    Returns:
        Kedro Pipeline with upsert node.
    """
    return pipeline(
        [
            node(
                func=upsert_to_qdrant_node,
                inputs=["vector_records", "params:upsert"],
                outputs=None,
                name="upsert_to_qdrant_node",
            ),
        ]
    )
