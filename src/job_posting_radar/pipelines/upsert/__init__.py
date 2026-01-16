"""Upsert pipeline for persisting vector records to Qdrant."""

from .pipeline import create_pipeline

__all__ = ["create_pipeline"]
