"""Integration tests for app/main.py - FastAPI endpoints."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_service
from app.models import HealthResponse, JobResult, SearchResponse, SimilarResponse


@pytest.fixture
def mock_service():
    """Create a mock JobSearchService."""
    with patch("app.main.JobSearchService") as mock_class:
        mock_svc = MagicMock()
        mock_class.return_value = mock_svc
        yield mock_svc


@pytest.fixture
def client(mock_service: MagicMock):
    """Create test client with mocked service."""
    # Reset the global service
    import app.main as main_module

    main_module._service = mock_service

    with TestClient(app) as test_client:
        yield test_client

    # Cleanup
    main_module._service = None


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_healthy(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test health endpoint returns healthy status."""
        mock_service.health_check.return_value = {
            "status": "healthy",
            "qdrant_connected": True,
            "collection_name": "job_posts",
            "points_count": 1000,
        }

        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["qdrant_connected"] is True
        assert data["points_count"] == 1000

    def test_health_unhealthy(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test health endpoint returns unhealthy status."""
        mock_service.health_check.return_value = {
            "status": "unhealthy",
            "qdrant_connected": False,
            "collection_name": "job_posts",
            "points_count": None,
        }

        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["qdrant_connected"] is False


class TestSearchEndpoint:
    """Tests for /search endpoint."""

    def test_search_basic(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test basic search functionality."""
        mock_service.search.return_value = {
            "query": "python developer",
            "total": 1,
            "duplicates_collapsed": 0,
            "results": [
                JobResult(
                    score=0.95,
                    title="Python Developer",
                    source="nofluff",
                    source_id="ABC123",
                )
            ],
        }

        response = client.get("/search", params={"q": "python developer"})

        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "python developer"
        assert data["total"] == 1
        assert len(data["results"]) == 1
        assert data["results"][0]["title"] == "Python Developer"

    def test_search_with_all_params(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test search with all parameters."""
        mock_service.search.return_value = {
            "query": "senior engineer",
            "total": 5,
            "duplicates_collapsed": 2,
            "results": [],
        }

        response = client.get(
            "/search",
            params={
                "q": "senior engineer",
                "limit": 10,
                "mode": "remote",
                "city": "Warszawa",
                "source": "nofluff",
                "collapse": True,
            },
        )

        assert response.status_code == 200
        mock_service.search.assert_called_once_with(
            query="senior engineer",
            limit=10,
            mode="remote",
            city="Warszawa",
            source="nofluff",
            collapse=True,
        )

    def test_search_missing_query(self, client: TestClient) -> None:
        """Test search fails when query is missing."""
        response = client.get("/search")

        assert response.status_code == 422  # Validation error

    def test_search_empty_query(self, client: TestClient) -> None:
        """Test search fails when query is empty."""
        response = client.get("/search", params={"q": ""})

        assert response.status_code == 422  # Validation error

    def test_search_limit_validation(self, client: TestClient) -> None:
        """Test search limit parameter validation."""
        # Limit too low
        response = client.get("/search", params={"q": "python", "limit": 0})
        assert response.status_code == 422

        # Limit too high
        response = client.get("/search", params={"q": "python", "limit": 101})
        assert response.status_code == 422

    def test_search_invalid_mode(self, client: TestClient) -> None:
        """Test search fails with invalid mode."""
        response = client.get("/search", params={"q": "python", "mode": "invalid"})

        assert response.status_code == 422

    def test_search_invalid_source(self, client: TestClient) -> None:
        """Test search fails with invalid source."""
        response = client.get("/search", params={"q": "python", "source": "indeed"})

        assert response.status_code == 422

    def test_search_service_error(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test search handles service errors."""
        mock_service.search.side_effect = Exception("Database error")

        response = client.get("/search", params={"q": "python"})

        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestSimilarEndpoint:
    """Tests for /similar endpoint."""

    def test_similar_basic(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test basic similar jobs functionality."""
        mock_service.find_similar.return_value = {
            "reference_job": JobResult(
                score=1.0,
                title="Senior Python Developer",
                source="nofluff",
                source_id="ABC123",
            ),
            "total": 5,
            "duplicates_collapsed": 1,
            "results": [],
        }

        response = client.get(
            "/similar",
            params={"source": "nofluff", "source_id": "ABC123"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["reference_job"]["title"] == "Senior Python Developer"
        assert data["total"] == 5

    def test_similar_with_all_params(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test similar with all parameters."""
        mock_service.find_similar.return_value = {
            "reference_job": JobResult(
                score=1.0,
                title="Developer",
                source="justjoin",
                source_id="JJ123",
            ),
            "total": 0,
            "duplicates_collapsed": 0,
            "results": [],
        }

        response = client.get(
            "/similar",
            params={
                "source": "justjoin",
                "source_id": "JJ123",
                "limit": 15,
                "cross_source": True,
                "exclude_same_company": True,
                "collapse": False,
            },
        )

        assert response.status_code == 200
        mock_service.find_similar.assert_called_once_with(
            source="justjoin",
            source_id="JJ123",
            limit=15,
            cross_source=True,
            exclude_same_company=True,
            collapse=False,
        )

    def test_similar_missing_source(self, client: TestClient) -> None:
        """Test similar fails when source is missing."""
        response = client.get("/similar", params={"source_id": "ABC123"})

        assert response.status_code == 422

    def test_similar_missing_source_id(self, client: TestClient) -> None:
        """Test similar fails when source_id is missing."""
        response = client.get("/similar", params={"source": "nofluff"})

        assert response.status_code == 422

    def test_similar_invalid_source(self, client: TestClient) -> None:
        """Test similar fails with invalid source."""
        response = client.get(
            "/similar",
            params={"source": "linkedin", "source_id": "ABC123"},
        )

        assert response.status_code == 422

    def test_similar_job_not_found(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test similar returns 404 when job not found."""
        mock_service.find_similar.side_effect = ValueError("Job not found")

        response = client.get(
            "/similar",
            params={"source": "nofluff", "source_id": "NOTFOUND"},
        )

        assert response.status_code == 404
        assert "Job not found" in response.json()["detail"]

    def test_similar_service_error(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test similar handles service errors."""
        mock_service.find_similar.side_effect = Exception("Connection error")

        response = client.get(
            "/similar",
            params={"source": "nofluff", "source_id": "ABC123"},
        )

        assert response.status_code == 500
        assert "Connection error" in response.json()["detail"]


class TestWorkModeEnum:
    """Tests for WorkMode enum validation."""

    def test_valid_work_modes(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test all valid work modes are accepted."""
        mock_service.search.return_value = {
            "query": "test",
            "total": 0,
            "duplicates_collapsed": 0,
            "results": [],
        }

        for mode in ["remote", "hybrid", "onsite"]:
            response = client.get("/search", params={"q": "test", "mode": mode})
            assert response.status_code == 200, f"Mode {mode} should be valid"


class TestSourceEnum:
    """Tests for Source enum validation."""

    def test_valid_sources(self, client: TestClient, mock_service: MagicMock) -> None:
        """Test all valid sources are accepted."""
        mock_service.search.return_value = {
            "query": "test",
            "total": 0,
            "duplicates_collapsed": 0,
            "results": [],
        }

        for source in ["nofluff", "justjoin"]:
            response = client.get("/search", params={"q": "test", "source": source})
            assert response.status_code == 200, f"Source {source} should be valid"


class TestResponseModels:
    """Tests for response model structure."""

    def test_search_response_structure(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test search response has correct structure."""
        mock_service.search.return_value = {
            "query": "python",
            "total": 1,
            "duplicates_collapsed": 0,
            "results": [
                JobResult(
                    score=0.9,
                    title="Python Dev",
                    company="TechCo",
                    locations=[],
                    work_mode="remote",
                    salaries=[],
                    source="nofluff",
                    source_id="123",
                    job_url="https://example.com",
                    skills_required=["Python"],
                    also_on=["justjoin"],
                )
            ],
        }

        response = client.get("/search", params={"q": "python"})
        data = response.json()

        # Check top-level fields
        assert "query" in data
        assert "total" in data
        assert "duplicates_collapsed" in data
        assert "results" in data

        # Check result fields
        result = data["results"][0]
        assert "score" in result
        assert "title" in result
        assert "company" in result
        assert "locations" in result
        assert "work_mode" in result
        assert "salaries" in result
        assert "source" in result
        assert "source_id" in result
        assert "job_url" in result
        assert "skills_required" in result
        assert "also_on" in result

    def test_health_response_structure(
        self,
        client: TestClient,
        mock_service: MagicMock,
    ) -> None:
        """Test health response has correct structure."""
        mock_service.health_check.return_value = {
            "status": "healthy",
            "qdrant_connected": True,
            "collection_name": "jobs",
            "points_count": 100,
        }

        response = client.get("/health")
        data = response.json()

        assert "status" in data
        assert "qdrant_connected" in data
        assert "collection_name" in data
        assert "points_count" in data
