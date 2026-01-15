"""Module for interacting with the Qdrant vector database."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.exceptions import UnexpectedResponse

from job_posting_radar.config import AppSettings

logger = logging.getLogger(__name__)


class VectorStore:
    """Wrapper for Qdrant vector database operations."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        """Initialize the Qdrant client and ensure collection exists.

        Args:
            settings: Application settings. Defaults to AppSettings().
        """
        self.settings = settings or AppSettings()
        self.client = QdrantClient(
            host=self.settings.qdrant_host,
            port=self.settings.qdrant_port,
        )
        self.collection_name = self.settings.qdrant_collection_name
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        """Create the collection if it doesn't already exist and setup indexes."""
        try:
            self.client.get_collection(self.collection_name)
            logger.debug("Collection already exists", extra={"collection_name": self.collection_name})
        except (UnexpectedResponse, Exception):
            logger.info("Creating new collection", extra={"collection_name": self.collection_name})
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.settings.embedding_dimension,
                    distance=models.Distance.COSINE,
                ),
            )
            self._setup_indexes()

    def _setup_indexes(self) -> None:
        """Create keyword indexes for fast filtering."""
        logger.info("Setting up payload indexes", extra={"collection": self.collection_name})
        
        index_fields = [
            "source",
            "work_mode",
            "seniority",
            "locations[].city",
            "locations[].country",
        ]
        
        for field in index_fields:
            self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name=field,
                field_schema=models.PayloadSchemaType.KEYWORD,
            )


    def upsert_postings(
        self,
        ids: List[str],
        vectors: List[List[float]],
        payloads: List[Dict[str, Any]],
    ) -> None:
        """Upsert job postings and their embeddings into Qdrant.

        Args:
            ids: List of unique identifiers (e.g., content_hash).
            vectors: List of embedding vectors.
            payloads: List of dictionaries containing the posting metadata.
        """
        if not ids:
            return

        logger.info(
            "Upserting vectors to Qdrant",
            extra={"count": len(ids), "collection": self.collection_name},
        )
        self.client.upsert(
            collection_name=self.collection_name,
            points=models.Batch(
                ids=ids,
                vectors=vectors,
                payloads=payloads,
            ),
        )

