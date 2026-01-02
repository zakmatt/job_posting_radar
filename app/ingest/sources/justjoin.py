"""HTTP client for fetching job postings from Just Join IT."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, List, Optional

import requests

from app.config import AppSettings

logger = logging.getLogger(__name__)


class JustJoinClient:
    """Lightweight client for the Just Join IT website."""

    def __init__(
        self,
        settings: Optional[AppSettings] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.settings = settings or AppSettings()
        self.session = session or requests.Session()
        self.api_base_url = self.settings.justjoin_api_base_url.rstrip("/")
        self._last_request_at: Optional[float] = None

    def fetch_page(self, cursor: int) -> Dict[str, Any]:
        """Fetch a batch of offers via the cursor endpoint.

        Args:
            cursor: Zero-based offset indicating where to start returning offers.

        Returns:
            Dict with:
                - postings: list of offers that include salary info.
                - raw_payload: full API response JSON.
                - next_cursor: next offset if provided by API (may be None).
                - items_count: number of items requested in this call.
        """
        if cursor < 0:
            raise ValueError("cursor must be >= 0")

        url = f"{self.api_base_url}/v2/user-panel/offers/by-cursor"
        items_count = min(self.settings.justjoin_page_size, 100)
        params = {
            "currency": "pln",
            "from": cursor,
            "itemsCount": items_count,
            "orderBy": "DESC",
        }
        response = self._request("GET", url, params=params)
        
        payload = response.json()
        data = payload.get("data") or []
        if not isinstance(data, list):
            raise RuntimeError("Unexpected Just Join response; expected data list")
        postings_with_salary = [p for p in data if _has_salary(p)]
        meta = payload.get("meta") or {}
        next_cursor = (meta.get("next") or {}).get("cursor")
        return {
            "postings": postings_with_salary,
            "raw_payload": payload,
            "next_cursor": next_cursor,
            "items_count": items_count,
        }

    def fetch_detail(self, slug: str) -> Dict[str, Any]:
        """Fetch offer detail by slug.

        Args:
            slug: Offer slug from the listing payload.

        Returns:
            Offer detail JSON as returned by the API.
        """
        if not slug:
            raise ValueError("slug must be provided")
        url = f"{self.api_base_url}/v1/offers/{slug}"
        response = self._request("GET", url)
        return response.json()

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """Perform an HTTP request with retry, backoff, and basic rate limiting."""
        headers = {"User-Agent": self.settings.user_agent}
        merged_headers = {**headers, **kwargs.pop("headers", {})}

        backoff = self.settings.backoff_initial_seconds
        attempts = self.settings.request_max_retries
        last_error: Optional[Exception] = None

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


def _has_salary(posting: Dict[str, Any]) -> bool:
    """Return True if a posting contains a salary range."""
    if not isinstance(posting, dict):
        return False
    # JJ payload uses employmentTypes list with from/to amounts
    employment_types = posting.get("employmentTypes")
    if isinstance(employment_types, list):
        for entry in employment_types:
            if not isinstance(entry, dict):
                continue
            if _has_money(entry.get("from")) or _has_money(entry.get("to")):
                return True
    # Fallback keys
    for key in ("salary_from", "salary_to", "salary"):
        if _has_money(posting.get(key)):
            return True
    return False


def _has_money(value: Any) -> bool:
    """Return True if value looks like a non-null numeric amount."""
    return isinstance(value, (int, float)) and value is not None

