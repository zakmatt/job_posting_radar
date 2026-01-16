"""Embedding pipeline."""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import generate_embeddings_node, prepare_vector_records_node


def create_pipeline(**kwargs) -> Pipeline:
    """Create the embedding pipeline.

    Pipeline stages:
        1. generate_embeddings - Generate vector embeddings for normalized postings
        2. prepare_vector_records - Transform into Qdrant-ready format

    Returns:
        Kedro Pipeline with two sequential nodes.
    """
    return pipeline(
        [
            node(
                func=generate_embeddings_node,
                inputs=["normalized_postings", "params:embedding"],
                outputs="embedded_postings",
                name="generate_embeddings_node",
            ),
            node(
                func=prepare_vector_records_node,
                inputs="embedded_postings",
                outputs="vector_records",
                name="prepare_vector_records_node",
            ),
        ]
    )
