"""Shared services for job search operations."""

from dataclasses import dataclass, field
from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue

from job_posting_radar.config import AppSettings
from job_posting_radar.pipelines.embedding.embeddings import EmbeddingGenerator

from app.models import JobResult, LocationResponse, SalaryResponse


@dataclass
class CollapsedResult:
    """A search result with potential duplicates collapsed."""

    score: float
    payload: dict[str, Any]
    sources: list[str] = field(default_factory=list)
    urls: dict[str, str] = field(default_factory=dict)


class JobSearchService:
    """Service for searching and finding similar jobs."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        """Initialize the search service.

        Args:
            settings: Application settings. Defaults to AppSettings().
        """
        self.settings = settings or AppSettings()
        self._client: QdrantClient | None = None
        self._generator: EmbeddingGenerator | None = None

    @property
    def client(self) -> QdrantClient:
        """Get or create Qdrant client.

        Returns:
            Qdrant client instance.
        """
        if self._client is None:
            self._client = QdrantClient(
                host=self.settings.qdrant_host,
                port=self.settings.qdrant_port,
            )
        return self._client

    @property
    def generator(self) -> EmbeddingGenerator:
        """Get or create embedding generator.

        Returns:
            Embedding generator instance.
        """
        if self._generator is None:
            self._generator = EmbeddingGenerator(settings=self.settings)
        return self._generator

    def health_check(self) -> dict[str, Any]:
        """Check service health.

        Returns:
            Health status dictionary.
        """
        try:
            info = self.client.get_collection(self.settings.qdrant_collection_name)
            return {
                "status": "healthy",
                "qdrant_connected": True,
                "collection_name": self.settings.qdrant_collection_name,
                "points_count": info.points_count,
            }
        except Exception:
            return {
                "status": "unhealthy",
                "qdrant_connected": False,
                "collection_name": self.settings.qdrant_collection_name,
                "points_count": None,
            }

    def search(
        self,
        query: str,
        limit: int = 20,
        mode: str | None = None,
        city: str | None = None,
        source: str | None = None,
        collapse: bool = True,
    ) -> dict[str, Any]:
        """Search for jobs by semantic query.

        Args:
            query: Search query text.
            limit: Maximum number of results.
            mode: Work mode filter (remote/hybrid/onsite).
            city: City filter.
            source: Source filter (nofluff/justjoin).
            collapse: Whether to collapse duplicates.

        Returns:
            Search results dictionary.
        """
        # Generate embedding for query
        query_vector = self.generator.generate([query])[0]

        # Build filter
        search_filter = self._build_filter(mode=mode, city=city, source=source)

        # Fetch more results when collapsing to ensure enough unique results
        fetch_limit = limit * 3 if collapse else limit

        # Search Qdrant
        results = self.client.search(
            collection_name=self.settings.qdrant_collection_name,
            query_vector=query_vector,
            query_filter=search_filter,
            limit=fetch_limit,
            with_payload=True,
        )

        if collapse:
            collapsed = self._collapse_duplicates(results)[:limit]
            job_results = [self._collapsed_to_job_result(r) for r in collapsed]
            duplicates_collapsed = len(results) - len(collapsed)
        else:
            job_results = [self._point_to_job_result(p) for p in results[:limit]]
            duplicates_collapsed = 0

        return {
            "query": query,
            "total": len(job_results),
            "duplicates_collapsed": max(0, duplicates_collapsed),
            "results": job_results,
        }

    def find_similar(
        self,
        source: str,
        source_id: str,
        limit: int = 20,
        cross_source: bool = False,
        exclude_same_company: bool = False,
        collapse: bool = True,
    ) -> dict[str, Any]:
        """Find jobs similar to a reference job.

        Args:
            source: Source of reference job (nofluff/justjoin).
            source_id: Source-specific job ID.
            limit: Maximum number of results.
            cross_source: Include jobs from both sources.
            exclude_same_company: Exclude jobs from same company.
            collapse: Whether to collapse duplicates.

        Returns:
            Similar jobs results dictionary.

        Raises:
            ValueError: If reference job not found.
        """
        # Find reference job
        payload, vector = self._find_job_by_source_id(source, source_id)
        if payload is None or vector is None:
            raise ValueError(f"Job not found: source={source}, source_id={source_id}")

        reference_job = self._payload_to_job_result(payload, score=1.0)

        # Build filter
        conditions = []
        if not cross_source:
            conditions.append(
                FieldCondition(key="source", match=MatchValue(value=source))
            )

        search_filter = Filter(must=conditions) if conditions else None

        if exclude_same_company and payload.get("company"):
            must_not = [
                FieldCondition(key="company", match=MatchValue(value=payload["company"]))
            ]
            if search_filter:
                search_filter.must_not = must_not
            else:
                search_filter = Filter(must_not=must_not)

        # Fetch more when collapsing
        fetch_limit = limit * 3 if collapse else limit
        fetch_limit += 1  # +1 for reference job

        # Search for similar
        results = self.client.search(
            collection_name=self.settings.qdrant_collection_name,
            query_vector=vector,
            query_filter=search_filter,
            limit=fetch_limit,
            with_payload=True,
        )

        if collapse:
            collapsed = self._collapse_duplicates(results, exclude_source_id=source_id)
            collapsed = collapsed[:limit]
            job_results = [self._collapsed_to_job_result(r) for r in collapsed]
            duplicates_collapsed = len(results) - len(collapsed) - 1
        else:
            # Filter out reference job
            filtered = [p for p in results if p.payload.get("source_id") != source_id]
            job_results = [self._point_to_job_result(p) for p in filtered[:limit]]
            duplicates_collapsed = 0

        return {
            "reference_job": reference_job,
            "total": len(job_results),
            "duplicates_collapsed": max(0, duplicates_collapsed),
            "results": job_results,
        }

    def _build_filter(
        self,
        mode: str | None = None,
        city: str | None = None,
        source: str | None = None,
    ) -> Filter | None:
        """Build Qdrant filter from parameters.

        Args:
            mode: Work mode filter.
            city: City filter.
            source: Source filter.

        Returns:
            Qdrant Filter or None.
        """
        conditions = []

        if mode:
            conditions.append(
                FieldCondition(key="work_mode", match=MatchValue(value=mode))
            )
        if city:
            conditions.append(
                FieldCondition(key="locations[].city", match=MatchValue(value=city))
            )
        if source:
            conditions.append(
                FieldCondition(key="source", match=MatchValue(value=source))
            )

        return Filter(must=conditions) if conditions else None

    def _find_job_by_source_id(
        self,
        source: str,
        source_id: str,
    ) -> tuple[dict[str, Any] | None, list[float] | None]:
        """Find a job by source and source_id.

        Args:
            source: Job source.
            source_id: Source-specific ID.

        Returns:
            Tuple of (payload, vector) or (None, None).
        """
        search_filter = Filter(
            must=[
                FieldCondition(key="source", match=MatchValue(value=source)),
                FieldCondition(key="source_id", match=MatchValue(value=source_id)),
            ]
        )

        results, _ = self.client.scroll(
            collection_name=self.settings.qdrant_collection_name,
            scroll_filter=search_filter,
            limit=1,
            with_payload=True,
            with_vectors=True,
        )

        if not results:
            return None, None

        return results[0].payload, results[0].vector

    def _collapse_duplicates(
        self,
        results: list[Any],
        exclude_source_id: str | None = None,
    ) -> list[CollapsedResult]:
        """Collapse duplicate postings by content_hash.

        Args:
            results: Qdrant search results.
            exclude_source_id: Source ID to exclude.

        Returns:
            List of collapsed results.
        """
        groups: dict[str, CollapsedResult] = {}

        for point in results:
            payload = point.payload
            score = point.score
            source_id = payload.get("source_id", "")

            if exclude_source_id and source_id == exclude_source_id:
                continue

            content_hash = payload.get("content_hash", point.id)
            source = payload.get("source", "unknown")
            job_url = payload.get("job_url", "")

            if content_hash not in groups:
                groups[content_hash] = CollapsedResult(
                    score=score,
                    payload=payload,
                    sources=[source],
                    urls={source: job_url},
                )
            else:
                existing = groups[content_hash]
                if source not in existing.sources:
                    existing.sources.append(source)
                    existing.urls[source] = job_url
                if score > existing.score:
                    existing.score = score
                    existing.payload = payload

        return sorted(groups.values(), key=lambda x: x.score, reverse=True)

    def _point_to_job_result(self, point: Any) -> JobResult:
        """Convert Qdrant point to JobResult.

        Args:
            point: Qdrant ScoredPoint.

        Returns:
            JobResult instance.
        """
        return self._payload_to_job_result(point.payload, point.score)

    def _collapsed_to_job_result(self, collapsed: CollapsedResult) -> JobResult:
        """Convert CollapsedResult to JobResult.

        Args:
            collapsed: Collapsed result.

        Returns:
            JobResult instance.
        """
        result = self._payload_to_job_result(collapsed.payload, collapsed.score)
        # Add other sources
        primary = collapsed.sources[0] if collapsed.sources else None
        result.also_on = [s for s in collapsed.sources if s != primary]
        return result

    def _payload_to_job_result(
        self,
        payload: dict[str, Any],
        score: float,
    ) -> JobResult:
        """Convert payload dict to JobResult.

        Args:
            payload: Job payload dictionary.
            score: Similarity score.

        Returns:
            JobResult instance.
        """
        locations = [
            LocationResponse(city=loc.get("city"), country=loc.get("country"))
            for loc in payload.get("locations", [])
        ]

        salaries = [
            SalaryResponse(
                employment_type=sal.get("employment_type"),
                from_amount=sal.get("from_amount"),
                to_amount=sal.get("to_amount"),
                currency=sal.get("currency"),
                period=sal.get("period"),
                gross=sal.get("gross"),
            )
            for sal in payload.get("salaries", [])
        ]

        return JobResult(
            score=score,
            title=payload.get("title", "Unknown"),
            company=payload.get("company"),
            locations=locations,
            work_mode=payload.get("work_mode"),
            salaries=salaries,
            source=payload.get("source", "unknown"),
            source_id=payload.get("source_id", ""),
            job_url=payload.get("job_url"),
            skills_required=payload.get("skills_required", []),
        )
