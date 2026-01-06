"""Module for generating text embeddings using Sentence Transformers."""

from __future__ import annotations

import logging
from typing import List, Optional

from sentence_transformers import SentenceTransformer

from app.config import AppSettings

logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    """Generator for text embeddings using a pre-trained transformer model."""

    def __init__(self, settings: Optional[AppSettings] = None) -> None:
        """Initialize the embedding generator.

        Args:
            settings: Application settings. Defaults to AppSettings().
        """
        self.settings = settings or AppSettings()
        self.model_name = self.settings.embedding_model_name
        logger.info("Loading embedding model", extra={"model": self.model_name})
        self.model = SentenceTransformer(self.model_name)

    def generate(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of strings.

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors (list of floats).
        """
        if not texts:
            return []

        logger.debug("Generating embeddings", extra={"count": len(texts)})
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()

