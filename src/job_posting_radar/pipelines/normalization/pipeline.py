"""Normalization pipeline."""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import normalize_postings_node


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=normalize_postings_node,
                inputs=["raw_nofluff", "raw_justjoin", "params:normalization"],
                outputs="normalized_postings",
                name="normalize_postings_node",
            ),
        ]
    )

