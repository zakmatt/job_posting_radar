"""Ingestion pipeline."""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import ingest_nofluff_node, ingest_justjoin_node


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=ingest_nofluff_node,
                inputs="params:ingestion",
                outputs="raw_nofluff",
                name="ingest_nofluff_node",
            ),
            node(
                func=ingest_justjoin_node,
                inputs="params:ingestion",
                outputs="raw_justjoin",
                name="ingest_justjoin_node",
            ),
        ]
    )

