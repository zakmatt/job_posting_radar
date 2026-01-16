"""Unit tests for app/services.py - JobSearchService."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from qdrant_client.models import FieldCondition, Filter, MatchValue

from app.models import JobResult, LocationResponse, SalaryResponse
from app.services import CollapsedResult, JobSearchService


class TestCollapsedResult:
    """Tests for CollapsedResult dataclass."""

    def test_default_values(self) -> None:
        """Test CollapsedResult has correct default values."""
        result = CollapsedResult(score=0.9, payload={"title": "Dev"})
        assert result.score == 0.9
        assert result.payload == {"title": "Dev"}
        assert result.sources == []
        assert result.urls == {}

    def test_with_sources_and_urls(self) -> None:
        """Test CollapsedResult with sources and URLs."""
        result = CollapsedResult(
            score=0.85,
            payload={"title": "Engineer"},
            sources=["nofluff", "justjoin"],
            urls={"nofluff": "https://nfj.com/1", "justjoin": "https://jj.it/1"},
        )
        assert result.sources == ["nofluff", "justjoin"]
        assert result.urls["nofluff"] == "https://nfj.com/1"


class TestJobSearchServiceInit:
    """Tests for JobSearchService initialization."""

    def test_init_with_default_settings(self) -> None:
        """Test service initializes with default settings."""
        with patch("app.services.AppSettings") as mock_settings_class:
            mock_settings = MagicMock()
            mock_settings_class.return_value = mock_settings

            service = JobSearchService()

            assert service.settings == mock_settings
            assert service._client is None
            assert service._generator is None

    def test_init_with_provided_settings(self) -> None:
        """Test service initializes with provided settings."""
        mock_settings = MagicMock()

        service = JobSearchService(settings=mock_settings)

        assert service.settings == mock_settings


class TestJobSearchServiceClient:
    """Tests for JobSearchService client property."""

    def test_client_lazy_initialization(self) -> None:
        """Test client is lazily initialized."""
        mock_settings = MagicMock()
        mock_settings.qdrant_host = "localhost"
        mock_settings.qdrant_port = 6333

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            assert service._client is None

            # Access client property
            client = service.client

            assert client == mock_client
            mock_client_class.assert_called_once_with(host="localhost", port=6333)

    def test_client_returns_same_instance(self) -> None:
        """Test client property returns same instance on repeated calls."""
        mock_settings = MagicMock()
        mock_settings.qdrant_host = "localhost"
        mock_settings.qdrant_port = 6333

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            client1 = service.client
            client2 = service.client

            assert client1 is client2
            assert mock_client_class.call_count == 1


class TestJobSearchServiceGenerator:
    """Tests for JobSearchService generator property."""

    def test_generator_lazy_initialization(self) -> None:
        """Test generator is lazily initialized."""
        mock_settings = MagicMock()

        with patch("app.services.EmbeddingGenerator") as mock_gen_class:
            mock_generator = MagicMock()
            mock_gen_class.return_value = mock_generator

            service = JobSearchService(settings=mock_settings)
            assert service._generator is None

            # Access generator property
            gen = service.generator

            assert gen == mock_generator
            mock_gen_class.assert_called_once_with(settings=mock_settings)


class TestJobSearchServiceHealthCheck:
    """Tests for JobSearchService.health_check method."""

    def test_health_check_healthy(self) -> None:
        """Test health check returns healthy status."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "test_jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_collection_info = MagicMock()
            mock_collection_info.points_count = 500
            mock_client.get_collection.return_value = mock_collection_info
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            result = service.health_check()

            assert result["status"] == "healthy"
            assert result["qdrant_connected"] is True
            assert result["collection_name"] == "test_jobs"
            assert result["points_count"] == 500

    def test_health_check_unhealthy(self) -> None:
        """Test health check returns unhealthy when Qdrant fails."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "test_jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client.get_collection.side_effect = Exception("Connection refused")
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            result = service.health_check()

            assert result["status"] == "unhealthy"
            assert result["qdrant_connected"] is False
            assert result["collection_name"] == "test_jobs"
            assert result["points_count"] is None


class TestJobSearchServiceBuildFilter:
    """Tests for JobSearchService._build_filter method."""

    def test_build_filter_no_params_returns_none(self) -> None:
        """Test build_filter returns None when no filters specified."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._build_filter()

        assert result is None

    def test_build_filter_with_mode(self) -> None:
        """Test build_filter with mode parameter."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._build_filter(mode="remote")

        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "work_mode"

    def test_build_filter_with_city(self) -> None:
        """Test build_filter with city parameter."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._build_filter(city="Warszawa")

        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "locations[].city"

    def test_build_filter_with_source(self) -> None:
        """Test build_filter with source parameter."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._build_filter(source="nofluff")

        assert result is not None
        assert len(result.must) == 1
        assert result.must[0].key == "source"

    def test_build_filter_with_all_params(self) -> None:
        """Test build_filter with all parameters."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._build_filter(mode="hybrid", city="Kraków", source="justjoin")

        assert result is not None
        assert len(result.must) == 3


class TestJobSearchServiceCollapseDuplicates:
    """Tests for JobSearchService._collapse_duplicates method."""

    def test_collapse_empty_results(self) -> None:
        """Test collapsing empty results returns empty list."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._collapse_duplicates([])

        assert result == []

    def test_collapse_no_duplicates(
        self,
        multiple_scored_points: list[Any],
    ) -> None:
        """Test collapsing results with no duplicates."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._collapse_duplicates(multiple_scored_points)

        assert len(result) == 5
        # Check sorted by score descending
        assert result[0].score >= result[1].score

    def test_collapse_with_duplicates(
        self,
        sample_scored_point: Any,
        sample_scored_point_justjoin: Any,
    ) -> None:
        """Test collapsing results with duplicates."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        # Both have same content_hash
        results = [sample_scored_point, sample_scored_point_justjoin]
        collapsed = service._collapse_duplicates(results)

        assert len(collapsed) == 1
        assert len(collapsed[0].sources) == 2
        assert "nofluff" in collapsed[0].sources
        assert "justjoin" in collapsed[0].sources

    def test_collapse_keeps_highest_score(self) -> None:
        """Test that collapse keeps the highest score."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        # Create mock points
        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.8
        point1.payload = {"content_hash": "same", "source": "nofluff", "job_url": ""}

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.95  # Higher score
        point2.payload = {"content_hash": "same", "source": "justjoin", "job_url": ""}

        collapsed = service._collapse_duplicates([point1, point2])

        assert len(collapsed) == 1
        assert collapsed[0].score == 0.95

    def test_collapse_excludes_source_id(self) -> None:
        """Test that collapse can exclude specific source_id."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        point1 = MagicMock()
        point1.id = "1"
        point1.score = 0.9
        point1.payload = {
            "content_hash": "hash1",
            "source": "nofluff",
            "source_id": "exclude-me",
            "job_url": "",
        }

        point2 = MagicMock()
        point2.id = "2"
        point2.score = 0.85
        point2.payload = {
            "content_hash": "hash2",
            "source": "nofluff",
            "source_id": "keep-me",
            "job_url": "",
        }

        collapsed = service._collapse_duplicates([point1, point2], exclude_source_id="exclude-me")

        assert len(collapsed) == 1
        assert collapsed[0].payload["source_id"] == "keep-me"


class TestJobSearchServicePayloadToJobResult:
    """Tests for JobSearchService._payload_to_job_result method."""

    def test_payload_to_job_result_full(
        self,
        sample_job_payload: dict[str, Any],
    ) -> None:
        """Test converting full payload to JobResult."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._payload_to_job_result(sample_job_payload, score=0.92)

        assert isinstance(result, JobResult)
        assert result.score == 0.92
        assert result.title == "Senior Python Developer"
        assert result.company == "TechCorp"
        assert result.source == "nofluff"
        assert result.source_id == "ABC123"
        assert result.work_mode == "remote"
        assert len(result.locations) == 2
        assert result.locations[0].city == "Warszawa"
        assert len(result.salaries) == 1
        assert result.salaries[0].from_amount == 25000
        assert result.skills_required == ["Python", "FastAPI", "PostgreSQL"]

    def test_payload_to_job_result_minimal(self) -> None:
        """Test converting minimal payload to JobResult."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        minimal_payload = {
            "source": "justjoin",
            "source_id": "test-123",
        }

        result = service._payload_to_job_result(minimal_payload, score=0.5)

        assert result.title == "Unknown"
        assert result.company is None
        assert result.locations == []
        assert result.salaries == []
        assert result.skills_required == []


class TestJobSearchServicePointToJobResult:
    """Tests for JobSearchService._point_to_job_result method."""

    def test_point_to_job_result(
        self,
        sample_scored_point: Any,
    ) -> None:
        """Test converting ScoredPoint to JobResult."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        result = service._point_to_job_result(sample_scored_point)

        assert result.score == 0.95
        assert result.title == "Senior Python Developer"


class TestJobSearchServiceCollapsedToJobResult:
    """Tests for JobSearchService._collapsed_to_job_result method."""

    def test_collapsed_to_job_result_single_source(
        self,
        sample_job_payload: dict[str, Any],
    ) -> None:
        """Test converting CollapsedResult with single source."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        collapsed = CollapsedResult(
            score=0.88,
            payload=sample_job_payload,
            sources=["nofluff"],
            urls={"nofluff": "https://nfj.com/1"},
        )

        result = service._collapsed_to_job_result(collapsed)

        assert result.score == 0.88
        assert result.also_on == []

    def test_collapsed_to_job_result_multiple_sources(
        self,
        sample_job_payload: dict[str, Any],
    ) -> None:
        """Test converting CollapsedResult with multiple sources."""
        mock_settings = MagicMock()
        service = JobSearchService(settings=mock_settings)

        collapsed = CollapsedResult(
            score=0.88,
            payload=sample_job_payload,
            sources=["nofluff", "justjoin"],
            urls={"nofluff": "https://nfj.com/1", "justjoin": "https://jj.it/1"},
        )

        result = service._collapsed_to_job_result(collapsed)

        assert result.also_on == ["justjoin"]


class TestJobSearchServiceFindJobBySourceId:
    """Tests for JobSearchService._find_job_by_source_id method."""

    def test_find_job_found(
        self,
        sample_job_payload: dict[str, Any],
    ) -> None:
        """Test finding job by source_id when it exists."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_point = MagicMock()
            mock_point.payload = sample_job_payload
            mock_point.vector = [0.1] * 384
            mock_client.scroll.return_value = ([mock_point], None)
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            payload, vector = service._find_job_by_source_id("nofluff", "ABC123")

            assert payload == sample_job_payload
            assert vector == [0.1] * 384

    def test_find_job_not_found(self) -> None:
        """Test finding job by source_id when it doesn't exist."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client.scroll.return_value = ([], None)
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            payload, vector = service._find_job_by_source_id("nofluff", "NOTFOUND")

            assert payload is None
            assert vector is None


class TestJobSearchServiceSearch:
    """Tests for JobSearchService.search method."""

    def test_search_basic(
        self,
        sample_scored_point: Any,
    ) -> None:
        """Test basic search functionality."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with (
            patch("app.services.QdrantClient") as mock_client_class,
            patch("app.services.EmbeddingGenerator") as mock_gen_class,
        ):
            mock_client = MagicMock()
            mock_client.search.return_value = [sample_scored_point]
            mock_client_class.return_value = mock_client

            mock_generator = MagicMock()
            mock_generator.generate.return_value = [[0.1] * 384]
            mock_gen_class.return_value = mock_generator

            service = JobSearchService(settings=mock_settings)
            result = service.search("python developer")

            assert result["query"] == "python developer"
            assert result["total"] == 1
            assert len(result["results"]) == 1
            mock_generator.generate.assert_called_once_with(["python developer"])

    def test_search_with_filters(
        self,
        sample_scored_point: Any,
    ) -> None:
        """Test search with filters."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with (
            patch("app.services.QdrantClient") as mock_client_class,
            patch("app.services.EmbeddingGenerator") as mock_gen_class,
        ):
            mock_client = MagicMock()
            mock_client.search.return_value = [sample_scored_point]
            mock_client_class.return_value = mock_client

            mock_generator = MagicMock()
            mock_generator.generate.return_value = [[0.1] * 384]
            mock_gen_class.return_value = mock_generator

            service = JobSearchService(settings=mock_settings)
            result = service.search(
                "python developer",
                mode="remote",
                city="Warszawa",
                source="nofluff",
            )

            # Verify filter was passed to search
            call_kwargs = mock_client.search.call_args.kwargs
            assert call_kwargs["query_filter"] is not None

    def test_search_no_collapse(
        self,
        multiple_scored_points: list[Any],
    ) -> None:
        """Test search without collapsing duplicates."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with (
            patch("app.services.QdrantClient") as mock_client_class,
            patch("app.services.EmbeddingGenerator") as mock_gen_class,
        ):
            mock_client = MagicMock()
            mock_client.search.return_value = multiple_scored_points
            mock_client_class.return_value = mock_client

            mock_generator = MagicMock()
            mock_generator.generate.return_value = [[0.1] * 384]
            mock_gen_class.return_value = mock_generator

            service = JobSearchService(settings=mock_settings)
            result = service.search("developer", limit=5, collapse=False)

            assert result["duplicates_collapsed"] == 0


class TestJobSearchServiceFindSimilar:
    """Tests for JobSearchService.find_similar method."""

    def test_find_similar_job_not_found(self) -> None:
        """Test find_similar raises ValueError when job not found."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()
            mock_client.scroll.return_value = ([], None)
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)

            with pytest.raises(ValueError, match="Job not found"):
                service.find_similar(source="nofluff", source_id="NOTFOUND")

    def test_find_similar_basic(
        self,
        sample_job_payload: dict[str, Any],
        multiple_scored_points: list[Any],
    ) -> None:
        """Test basic find_similar functionality."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()

            # Mock scroll to find reference job
            mock_ref_point = MagicMock()
            mock_ref_point.payload = sample_job_payload
            mock_ref_point.vector = [0.1] * 384
            mock_client.scroll.return_value = ([mock_ref_point], None)

            # Mock search for similar jobs
            mock_client.search.return_value = multiple_scored_points
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            result = service.find_similar(source="nofluff", source_id="ABC123")

            assert "reference_job" in result
            assert result["reference_job"].title == "Senior Python Developer"
            assert "results" in result

    def test_find_similar_exclude_same_company(
        self,
        sample_job_payload: dict[str, Any],
    ) -> None:
        """Test find_similar with exclude_same_company."""
        mock_settings = MagicMock()
        mock_settings.qdrant_collection_name = "jobs"

        with patch("app.services.QdrantClient") as mock_client_class:
            mock_client = MagicMock()

            mock_ref_point = MagicMock()
            mock_ref_point.payload = sample_job_payload
            mock_ref_point.vector = [0.1] * 384
            mock_client.scroll.return_value = ([mock_ref_point], None)
            mock_client.search.return_value = []
            mock_client_class.return_value = mock_client

            service = JobSearchService(settings=mock_settings)
            service.find_similar(
                source="nofluff",
                source_id="ABC123",
                exclude_same_company=True,
            )

            # Verify must_not filter was applied
            call_kwargs = mock_client.search.call_args.kwargs
            assert call_kwargs["query_filter"] is not None
            assert call_kwargs["query_filter"].must_not is not None
