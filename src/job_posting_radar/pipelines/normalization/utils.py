"""Utility functions for normalizing job postings."""

import html
import re
from datetime import datetime, timezone
from typing import Any

from job_posting_radar.pipelines.normalization.models import (
    EmploymentType,
    Seniority,
    WorkMode,
)

# ---------------------------------------------------------------------------
# Seniority mapping
# ---------------------------------------------------------------------------

_SENIORITY_MAP: dict[str, Seniority] = {
    # NoFluff
    "junior": Seniority.JUNIOR,
    "mid": Seniority.MID,
    "senior": Seniority.SENIOR,
    "expert": Seniority.SENIOR,  # Expert → Senior
    "lead": Seniority.STAFF,
    "principal": Seniority.STAFF,
    "staff": Seniority.STAFF,
    # JustJoin
    "trainee": Seniority.JUNIOR,
    "c-level": Seniority.STAFF,
}


def normalize_seniority(raw: str | None) -> Seniority:
    """Map raw seniority string to canonical Seniority enum.

    Args:
        raw: Source-specific seniority label (e.g., "Expert", "mid").

    Returns:
        Canonical Seniority enum value.
    """
    if not raw:
        return Seniority.UNKNOWN
    key = raw.strip().lower()
    return _SENIORITY_MAP.get(key, Seniority.UNKNOWN)


# ---------------------------------------------------------------------------
# Work mode mapping
# ---------------------------------------------------------------------------


def normalize_work_mode_nofluff(
    fully_remote: bool,
    remote_value: int | None,
    hybrid_desc: str | None,
) -> WorkMode:
    """Determine work mode from NoFluff location fields.

    Args:
        fully_remote: Boolean flag from listing.
        remote_value: Numeric remote level (0-4) from details.
        hybrid_desc: Hybrid description string.

    Returns:
        Canonical WorkMode enum value.
    """
    if fully_remote:
        return WorkMode.REMOTE
    # remote values: 0=onsite, 1-3=hybrid variants, 4=remote-friendly
    if remote_value is not None:
        if remote_value == 0:
            return WorkMode.ONSITE
        if remote_value >= 4:
            return WorkMode.REMOTE
        return WorkMode.HYBRID
    if hybrid_desc:
        return WorkMode.HYBRID
    return WorkMode.UNKNOWN


def normalize_work_mode_justjoin(workplace_type: str | None) -> WorkMode:
    """Determine work mode from JustJoin workplaceType.

    Args:
        workplace_type: Value like "remote", "hybrid", "office".

    Returns:
        Canonical WorkMode enum value.
    """
    if not workplace_type:
        return WorkMode.UNKNOWN
    key = workplace_type.strip().lower()
    if key == "remote":
        return WorkMode.REMOTE
    if key == "hybrid":
        return WorkMode.HYBRID
    if key in ("office", "onsite", "on-site"):
        return WorkMode.ONSITE
    return WorkMode.UNKNOWN


# ---------------------------------------------------------------------------
# Employment type mapping
# ---------------------------------------------------------------------------

_EMPLOYMENT_TYPE_MAP: dict[str, EmploymentType] = {
    "b2b": EmploymentType.B2B,
    "permanent": EmploymentType.PERMANENT,
    "uop": EmploymentType.PERMANENT,
    "contract": EmploymentType.CONTRACT,
    "mandate": EmploymentType.MANDATE,
    "mandate_contract": EmploymentType.MANDATE,
    "zlecenie": EmploymentType.MANDATE,
    "internship": EmploymentType.INTERNSHIP,
    "staz": EmploymentType.INTERNSHIP,
}


def normalize_employment_type(raw: str | None) -> EmploymentType:
    """Map raw employment type string to canonical enum.

    Args:
        raw: Source-specific employment type label.

    Returns:
        Canonical EmploymentType enum value.
    """
    if not raw:
        return EmploymentType.OTHER
    key = raw.strip().lower().replace(" ", "_")
    return _EMPLOYMENT_TYPE_MAP.get(key, EmploymentType.OTHER)


# ---------------------------------------------------------------------------
# HTML stripping
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def strip_html(text: str | None) -> str | None:
    """Remove HTML tags and normalize whitespace.

    Args:
        text: Raw HTML or plain text.

    Returns:
        Cleaned plain text or None if input was empty.
    """
    if not text:
        return None
    # Decode HTML entities
    text = html.unescape(text)
    # Remove tags
    text = _TAG_RE.sub(" ", text)
    # Normalize whitespace
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text if text else None


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------


def parse_timestamp(value: Any) -> datetime | None:
    """Parse various timestamp formats to UTC datetime.

    Args:
        value: Epoch milliseconds (int/float) or ISO string.

    Returns:
        UTC datetime or None if parsing fails.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value).astimezone(timezone.utc)
        except ValueError:
            return None
    return None
