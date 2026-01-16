"""HTTP client for fetching job postings from No Fluff Jobs."""

import logging
import random
import time
from collections.abc import Iterable
from typing import Any

import requests
from bs4 import BeautifulSoup

from job_posting_radar.config import AppSettings

logger = logging.getLogger(__name__)


class NoFluffJobsClient:
    """Lightweight client for the No Fluff Jobs website."""

    def __init__(
        self,
        settings: AppSettings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        """Initialize the NoFluff Jobs client.

        Args:
            settings: Application settings. Defaults to AppSettings().
            session: Requests session to use. Defaults to a new session.
        """
        self.settings = settings or AppSettings()
        self.session = session or requests.Session()
        self.base_url = self.settings.nofluff_base_url.rstrip("/")
        self._last_request_at: float | None = None

    def fetch_page(
        self,
        page: int,
        criteria: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Fetch a page of offers via the public XHR endpoint.

        Args:
            page: 1-based page number to fetch.
            criteria: Reserved for future filters (currently unused).

        Returns:
            Dictionary containing the extracted postings and the raw API payload.
        """
        if page < 1:
            raise ValueError("page must be >= 1")

        url = f"{self.base_url}{self.settings.nofluff_search_path}"
        params = self._build_search_params(page=page, criteria=criteria)
        response = self._request("GET", url, params=params)

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(f"Failed to parse JSON from {url}") from exc

        postings = self._extract_postings(payload)
        postings_with_salary = [p for p in postings if _has_salary(p)]

        return {"postings": postings_with_salary, "search": payload}

    def fetch_job_details(self, job_slug: str) -> dict[str, Any]:
        """Fetch detailed information for a single offer via the posting API.

        Args:
            job_slug: URL slug identifying the job posting.

        Returns:
            Dictionary with raw payload and extracted sections.
        """
        if not job_slug:
            raise ValueError("job_slug must be provided")
        identifier = job_slug.lstrip("/")
        url = f"{self.base_url}{self.settings.nofluff_posting_path}/{identifier}"
        logger.info("Fetching job details", extra={"identifier": identifier})

        response = self._request("GET", url)
        payload = response.json()
        sections = _extract_detail_sections(payload)
        return {"raw": payload, "sections": sections}

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Perform an HTTP request with retry, backoff, and basic rate limiting.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: URL to request.
            **kwargs: Additional arguments passed to requests.

        Returns:
            Response object from the successful request.

        Raises:
            RuntimeError: If all retry attempts fail.
        """
        headers = {"User-Agent": self.settings.user_agent}
        merged_headers = {**headers, **kwargs.pop("headers", {})}

        backoff = self.settings.backoff_initial_seconds
        attempts = self.settings.request_max_retries
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            self._honor_rate_limit()
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.settings.nofluff_timeout_seconds,
                    headers=merged_headers,
                    **kwargs,
                )
                self._last_request_at = time.monotonic()
                if response.status_code in {429, 500, 502, 503, 504}:
                    last_error = RuntimeError(
                        f"Transient error {response.status_code} from {url}"
                    )
                    logger.warning(
                        "Transient HTTP error",
                        extra={
                            "status": response.status_code,
                            "url": url,
                            "attempt": attempt,
                        },
                    )
                else:
                    response.raise_for_status()
                    return response
            except requests.RequestException as exc:
                last_error = exc
                logger.warning(
                    "HTTP request failed",
                    extra={"url": url, "attempt": attempt, "error": str(exc)},
                )

            if attempt == attempts:
                break

            delay = backoff + random.uniform(0, max(0.1, backoff / 2))
            logger.debug(
                "Retrying after backoff",
                extra={"delay_seconds": delay, "attempt": attempt, "url": url},
            )
            time.sleep(delay)
            backoff *= self.settings.backoff_multiplier

        raise RuntimeError(f"Failed to fetch {url}") from last_error

    def _honor_rate_limit(self) -> None:
        """Sleep to respect simple rate limits between requests."""
        if self._last_request_at is None:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.settings.nofluff_rate_limit_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _extract_postings(self, next_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Find the postings list within the search payload.

        Args:
            next_data: Raw search API response.

        Returns:
            List of posting dictionaries.

        Raises:
            RuntimeError: If postings list cannot be found.
        """
        postings = next_data.get("postings")
        if isinstance(postings, list):
            return postings

        for candidate in _walk_for_postings(next_data):
            if candidate:
                return candidate
        raise RuntimeError("Could not locate postings list in search payload")

    def _build_search_params(
        self, *, page: int, criteria: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Construct query parameters for the listing endpoint.

        Args:
            page: Page number to fetch.
            criteria: Additional filter criteria.

        Returns:
            Dictionary of query parameters.
        """
        params: dict[str, Any] = {
            "pageTo": page,
            "pageSize": self.settings.nofluff_page_size,
            "withSalaryMatch": True,
            "salaryCurrency": self.settings.nofluff_salary_currency,
            "salaryPeriod": self.settings.nofluff_salary_period,
            "region": self.settings.nofluff_region,
            "language": self.settings.nofluff_language,
        }
        if criteria:
            params.update(criteria)
        return params


def _walk_for_postings(obj: Any) -> Iterable[list[dict[str, Any]]]:
    """Yield any list of posting-like dicts found in a nested structure.

    Args:
        obj: Object to search through.

    Yields:
        Lists of posting dictionaries found in the structure.
    """
    stack = [obj]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for value in current.values():
                stack.append(value)
        elif isinstance(current, list):
            if current and all(isinstance(item, dict) for item in current):
                sample = current[0]
                if any(key in sample for key in ("title", "postingId", "id", "slug", "company")):
                    yield current  # type: ignore[misc]
            for value in current:
                stack.append(value)


def _has_salary(posting: dict[str, Any]) -> bool:
    """Return True if a posting contains a salary range.

    Args:
        posting: Posting dictionary to check.

    Returns:
        True if salary information is present.
    """
    if not isinstance(posting, dict):
        return False
    if posting.get("salary") or posting.get("salaryRange"):
        return True
    employment_types = posting.get("employmentTypes") or posting.get("employmentType")
    if isinstance(employment_types, list):
        for entry in employment_types:
            if not isinstance(entry, dict):
                continue
            salary = entry.get("salary") or entry.get("salaryRange")
            if salary:
                return True
            if entry.get("from") or entry.get("to"):
                return True
    return False


def _extract_detail_sections(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract Must Have / Requirements / Offer sections from the posting API payload.

    Args:
        payload: Raw posting detail payload.

    Returns:
        Dictionary with must_have, requirements_description, and offer_description.
    """
    must_have: list[str] = []
    requirements_description: str | None = None
    offer_description: str | None = None

    requirements = payload.get("requirements")
    if isinstance(requirements, dict):
        musts = requirements.get("musts", [])
        if isinstance(musts, list):
            for item in musts:
                if isinstance(item, dict):
                    value = item.get("value") or item.get("name")
                    if value:
                        must_have.append(str(value).strip())
                elif isinstance(item, str):
                    must_have.append(item.strip())

        req_desc = requirements.get("description")
        if isinstance(req_desc, str):
            requirements_description = _strip_html(req_desc)

    details = payload.get("details")
    if isinstance(details, dict):
        offer_desc_raw = details.get("description")
        if isinstance(offer_desc_raw, str):
            offer_description = _strip_html(offer_desc_raw)

    # Fallback to legacy section traversal if the new fields are missing.
    if not (must_have or requirements_description or offer_description):
        for sections in _walk_for_sections(payload):
            for section in sections:
                title = str(section.get("title", "")).strip().lower()
                items, content = _section_items_and_content(section)

                if "must have" in title or title == "must have":
                    must_have.extend(items or _split_content_lines(content))
                elif "requirements description" in title or "requirements" in title:
                    if not requirements_description:
                        requirements_description = content or "; ".join(items)
                elif (
                    "offer description" in title or "offer" in title or "we offer" in title
                ):
                    if not offer_description:
                        offer_description = content or "; ".join(items)

    return {
        "must_have": must_have,
        "requirements_description": requirements_description,
        "offer_description": offer_description,
    }


def _walk_for_sections(obj: Any) -> Iterable[list[dict[str, Any]]]:
    """Yield lists of section-like dicts found in a nested structure.

    Args:
        obj: Object to search through.

    Yields:
        Lists of section dictionaries.
    """
    stack = [obj]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for value in current.values():
                stack.append(value)
            if "sections" in current and isinstance(current["sections"], list):
                if all(isinstance(item, dict) for item in current["sections"]):
                    yield current["sections"]  # type: ignore[misc]
        elif isinstance(current, list):
            stack.extend(current)


def _section_items_and_content(section: dict[str, Any]) -> tuple[list[str], str | None]:
    """Return normalized list items and textual content for a section dict.

    Args:
        section: Section dictionary to extract from.

    Returns:
        Tuple of (items list, content string or None).
    """
    items_raw = section.get("items") or section.get("values") or []
    items: list[str] = []
    if isinstance(items_raw, list):
        for entry in items_raw:
            if isinstance(entry, dict):
                value = entry.get("value") or entry.get("text") or entry.get("label")
                if value:
                    items.append(str(value).strip())
            elif isinstance(entry, str):
                items.append(entry.strip())
    content = section.get("content")
    if isinstance(content, str):
        content = content.strip()
    else:
        content = None
    return items, content


def _split_content_lines(content: str | None) -> list[str]:
    """Split a blob of content into individual bullet-like lines.

    Args:
        content: Content string to split.

    Returns:
        List of non-empty lines.
    """
    if not content:
        return []
    return [line.strip() for line in content.splitlines() if line.strip()]


def _strip_html(raw: str) -> str:
    """Convert an HTML fragment to normalized plain text.

    Args:
        raw: HTML string to convert.

    Returns:
        Plain text with normalized whitespace.
    """
    soup = BeautifulSoup(raw, "html.parser")
    text = soup.get_text("\n")
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())
