"""Shared pytest fixtures for job-posting-radar tests."""

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


@dataclass
class MockScoredPoint:
    """Mock Qdrant ScoredPoint for testing."""

    id: str
    score: float
    payload: dict[str, Any]
    vector: list[float] | None = None


@dataclass
class MockCollectionInfo:
    """Mock Qdrant collection info."""

    points_count: int = 100


@pytest.fixture
def sample_job_payload() -> dict[str, Any]:
    """Sample job payload for testing."""
    return {
        "title": "Senior Python Developer",
        "company": "TechCorp",
        "source": "nofluff",
        "source_id": "ABC123",
        "work_mode": "remote",
        "job_url": "https://nofluffjobs.com/job/ABC123",
        "content_hash": "hash123",
        "locations": [
            {"city": "Warszawa", "country": "Poland"},
            {"city": "Kraków", "country": "Poland"},
        ],
        "salaries": [
            {
                "employment_type": "b2b",
                "from_amount": 25000,
                "to_amount": 35000,
                "currency": "PLN",
                "period": "month",
                "gross": False,
            }
        ],
        "skills_required": ["Python", "FastAPI", "PostgreSQL"],
    }


@pytest.fixture
def sample_job_payload_justjoin() -> dict[str, Any]:
    """Sample JustJoin job payload for testing."""
    return {
        "title": "Senior Python Developer",
        "company": "TechCorp",
        "source": "justjoin",
        "source_id": "jj-senior-python",
        "work_mode": "remote",
        "job_url": "https://justjoin.it/offers/jj-senior-python",
        "content_hash": "hash123",  # Same hash = duplicate
        "locations": [
            {"city": "Warszawa", "country": "Poland"},
        ],
        "salaries": [
            {
                "employment_type": "b2b",
                "from_amount": 25000,
                "to_amount": 35000,
                "currency": "PLN",
                "period": "month",
                "gross": False,
            }
        ],
        "skills_required": ["Python", "FastAPI"],
    }


@pytest.fixture
def sample_scored_point(sample_job_payload: dict[str, Any]) -> MockScoredPoint:
    """Sample Qdrant ScoredPoint for testing."""
    return MockScoredPoint(
        id="point-1",
        score=0.95,
        payload=sample_job_payload,
        vector=[0.1] * 384,
    )


@pytest.fixture
def sample_scored_point_justjoin(
    sample_job_payload_justjoin: dict[str, Any],
) -> MockScoredPoint:
    """Sample JustJoin ScoredPoint for testing."""
    return MockScoredPoint(
        id="point-2",
        score=0.93,
        payload=sample_job_payload_justjoin,
        vector=[0.1] * 384,
    )


@pytest.fixture
def multiple_scored_points(
    sample_job_payload: dict[str, Any],
) -> list[MockScoredPoint]:
    """Multiple scored points with different content hashes."""
    points = []
    for i in range(5):
        payload = sample_job_payload.copy()
        payload["source_id"] = f"job-{i}"
        payload["content_hash"] = f"hash-{i}"
        payload["title"] = f"Developer Position {i}"
        points.append(
            MockScoredPoint(
                id=f"point-{i}",
                score=0.9 - (i * 0.05),
                payload=payload,
            )
        )
    return points


@pytest.fixture
def mock_qdrant_client():
    """Create a mock Qdrant client."""
    with patch("app.services.QdrantClient") as mock_class:
        mock_client = MagicMock()
        mock_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_embedding_generator():
    """Create a mock embedding generator."""
    with patch("app.services.EmbeddingGenerator") as mock_class:
        mock_generator = MagicMock()
        mock_generator.generate.return_value = [[0.1] * 384]
        mock_class.return_value = mock_generator
        yield mock_generator


@pytest.fixture
def mock_settings():
    """Create mock settings."""
    with patch("app.services.AppSettings") as mock_class:
        mock_settings = MagicMock()
        mock_settings.qdrant_host = "localhost"
        mock_settings.qdrant_port = 6333
        mock_settings.qdrant_collection_name = "test_collection"
        mock_class.return_value = mock_settings
        yield mock_settings
