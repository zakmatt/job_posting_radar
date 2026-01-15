"""Normalization functions for different job posting sources."""

from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from job_posting_radar.normalization.models import (
    EmploymentType,
    Location,
    NormalizedJobPosting,
    Salary,
    Seniority,
    WorkMode,
)

# ---------------------------------------------------------------------------
# Seniority mapping
# ---------------------------------------------------------------------------

_SENIORITY_MAP: Dict[str, Seniority] = {
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


def _normalize_seniority(raw: Optional[str]) -> Seniority:
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


def _normalize_work_mode_nofluff(
    fully_remote: bool,
    remote_value: Optional[int],
    hybrid_desc: Optional[str],
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


def _normalize_work_mode_justjoin(workplace_type: Optional[str]) -> WorkMode:
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

_EMPLOYMENT_TYPE_MAP: Dict[str, EmploymentType] = {
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


def _normalize_employment_type(raw: Optional[str]) -> EmploymentType:
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


def _strip_html(text: Optional[str]) -> Optional[str]:
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


def _parse_timestamp(value: Any) -> Optional[datetime]:
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


# ---------------------------------------------------------------------------
# NoFluff normalizer
# ---------------------------------------------------------------------------


def normalize_nofluff(raw: Dict[str, Any]) -> NormalizedJobPosting:
    """Normalize a NoFluff Jobs raw payload to canonical schema.

    Args:
        raw: Raw ingested payload dict with 'payload.listing' and optionally 'payload.details'.

    Returns:
        NormalizedJobPosting instance.
    """
    listing = raw.get("payload", {}).get("listing", {})
    details_wrapper = raw.get("payload", {}).get("details", {})
    details = details_wrapper.get("raw", {})
    sections = details_wrapper.get("sections", {})

    # Identity
    source_id = raw.get("source_id") or listing.get("reference") or listing.get("id", "")
    slug = listing.get("url") or details.get("postingUrl")
    job_url = f"https://nofluffjobs.com/job/{slug}" if slug else None
    ingested_at = _parse_timestamp(raw.get("ingested_at")) or datetime.now(timezone.utc)

    # Title & company
    title = listing.get("title") or details.get("title") or ""
    company = listing.get("name") or details.get("company", {}).get("name")

    # Locations
    locations: List[Location] = []
    places = listing.get("location", {}).get("places") or details.get("location", {}).get("places") or []
    for place in places:
        city = place.get("city")
        country = (place.get("country") or {}).get("name")
        if city or country:
            locations.append(Location(city=city, country=country))

    # Work mode
    fully_remote = listing.get("fullyRemote", False)
    remote_value = details.get("location", {}).get("remote")
    hybrid_desc = listing.get("location", {}).get("hybridDesc") or details.get("location", {}).get("hybridDesc")
    work_mode = _normalize_work_mode_nofluff(fully_remote, remote_value, hybrid_desc)

    # Seniority (take first if list)
    seniority_list = listing.get("seniority") or details.get("basics", {}).get("seniority") or []
    seniority_raw = seniority_list[0] if seniority_list else None
    seniority = _normalize_seniority(seniority_raw)

    # Employment types & salaries
    employment_types: List[EmploymentType] = []
    salaries: List[Salary] = []

    original_salary = details.get("essentials", {}).get("originalSalary", {})
    salary_types = original_salary.get("types", {})
    currency = original_salary.get("currency") or listing.get("salary", {}).get("currency")
    disclosed = original_salary.get("disclosedAt") or listing.get("salary", {}).get("disclosedAt")

    for type_key, type_data in salary_types.items():
        # Skip non-dict entries (e.g., boolean flags)
        if not isinstance(type_data, dict):
            continue

        emp_type = _normalize_employment_type(type_key)
        employment_types.append(emp_type)

        # Extract salary range
        range_values = type_data.get("range", [])
        from_amt = range_values[0] if len(range_values) > 0 else None
        to_amt = range_values[1] if len(range_values) > 1 else None
        period = (type_data.get("period") or "month").lower()

        # Only add salary if disclosed
        if disclosed == "VISIBLE" and (from_amt or to_amt):
            salaries.append(
                Salary(
                    employment_type=emp_type,
                    from_amount=from_amt,
                    to_amount=to_amt,
                    currency=currency,
                    period=period,
                    gross=True,  # NoFluff salaries are gross
                )
            )

    # Fallback: listing-level salary if no detailed salary found
    if not salaries and listing.get("salary", {}).get("from"):
        sal = listing["salary"]
        emp_type = _normalize_employment_type(sal.get("type"))
        if emp_type not in employment_types:
            employment_types.append(emp_type)
        salaries.append(
            Salary(
                employment_type=emp_type,
                from_amount=sal.get("from"),
                to_amount=sal.get("to"),
                currency=sal.get("currency"),
                period="month",
                gross=True,
            )
        )

    # Skills
    skills_required: List[str] = []
    skills_nice_to_have: List[str] = []

    musts = details.get("requirements", {}).get("musts", [])
    for item in musts:
        if isinstance(item, dict):
            skills_required.append(item.get("value", ""))
        elif isinstance(item, str):
            skills_required.append(item)

    nices = details.get("requirements", {}).get("nices", [])
    for item in nices:
        if isinstance(item, dict):
            skills_nice_to_have.append(item.get("value", ""))
        elif isinstance(item, str):
            skills_nice_to_have.append(item)

    # Fallback to sections.must_have if no musts found
    if not skills_required and sections.get("must_have"):
        skills_required = [s for s in sections["must_have"] if s]

    # Filter empty strings
    skills_required = [s for s in skills_required if s]
    skills_nice_to_have = [s for s in skills_nice_to_have if s]

    # Text content
    description = _strip_html(details.get("details", {}).get("description")) or _strip_html(
        sections.get("offer_description")
    )
    requirements_text = _strip_html(details.get("requirements", {}).get("description")) or _strip_html(
        sections.get("requirements_description")
    )
    offer_text = _strip_html(sections.get("offer_description"))

    # Timestamps
    posted_at = _parse_timestamp(listing.get("posted") or details.get("posted"))
    renewed_at = _parse_timestamp(listing.get("renewed"))
    expires_at = _parse_timestamp(details.get("expiresAt"))

    return NormalizedJobPosting(
        source="nofluff",
        source_id=source_id,
        slug=slug,
        job_url=job_url,
        ingested_at=ingested_at,
        title=title,
        company=company,
        locations=locations,
        work_mode=work_mode,
        seniority=seniority,
        employment_types=list(set(employment_types)),
        salaries=salaries,
        skills_required=skills_required,
        skills_nice_to_have=skills_nice_to_have,
        description=description,
        requirements_text=requirements_text,
        offer_text=offer_text,
        posted_at=posted_at,
        expires_at=expires_at,
        renewed_at=renewed_at,
    )


# ---------------------------------------------------------------------------
# JustJoin normalizer
# ---------------------------------------------------------------------------


def normalize_justjoin(raw: Dict[str, Any]) -> NormalizedJobPosting:
    """Normalize a JustJoin raw payload to canonical schema.

    Args:
        raw: Raw ingested payload dict with 'payload.listing' and optionally 'payload.details'.

    Returns:
        NormalizedJobPosting instance.
    """
    listing = raw.get("payload", {}).get("listing", {})
    details = raw.get("payload", {}).get("details", {})

    # Use details if available, otherwise fall back to listing
    data = details if details else listing

    # Identity
    source_id = raw.get("source_id") or data.get("slug") or data.get("guid") or ""
    slug = data.get("slug") or listing.get("slug")
    job_url = f"https://justjoin.it/offers/{slug}" if slug else None
    ingested_at = _parse_timestamp(raw.get("ingested_at")) or datetime.now(timezone.utc)

    # Title & company
    title = data.get("title") or ""
    company = data.get("companyName")

    # Locations
    locations: List[Location] = []
    multilocation = data.get("multilocation") or listing.get("multilocation") or []
    for loc in multilocation:
        city = loc.get("city")
        # JustJoin doesn't always have country; default to Poland
        country = data.get("countryCode") or "Poland"
        if city:
            locations.append(Location(city=city, country=country))
    # Fallback to top-level city
    if not locations and data.get("city"):
        locations.append(Location(city=data.get("city"), country=data.get("countryCode") or "Poland"))

    # Work mode
    workplace = data.get("workplaceType")
    if isinstance(workplace, dict):
        workplace = workplace.get("value")
    work_mode = _normalize_work_mode_justjoin(workplace)

    # Seniority
    exp_level = data.get("experienceLevel")
    if isinstance(exp_level, dict):
        exp_level = exp_level.get("value")
    seniority = _normalize_seniority(exp_level)

    # Employment types & salaries
    employment_types: List[EmploymentType] = []
    salaries: List[Salary] = []

    emp_types_list = data.get("employmentTypes") or listing.get("employmentTypes") or []
    for emp in emp_types_list:
        emp_type = _normalize_employment_type(emp.get("type"))
        employment_types.append(emp_type)

        from_amt = emp.get("from") or emp.get("fromPln")
        to_amt = emp.get("to") or emp.get("toPln")
        currency = (emp.get("currency") or "PLN").upper()
        period = (emp.get("unit") or "month").lower()
        gross = emp.get("gross")

        if from_amt or to_amt:
            salaries.append(
                Salary(
                    employment_type=emp_type,
                    from_amount=from_amt,
                    to_amount=to_amt,
                    currency=currency,
                    period=period,
                    gross=gross,
                )
            )

    # Skills
    skills_required: List[str] = []
    skills_nice_to_have: List[str] = []

    req_skills = data.get("requiredSkills") or listing.get("requiredSkills") or []
    for skill in req_skills:
        if isinstance(skill, dict):
            skills_required.append(skill.get("name", ""))
        elif isinstance(skill, str):
            skills_required.append(skill)

    nice_skills = data.get("niceToHaveSkills") or listing.get("niceToHaveSkills") or []
    for skill in nice_skills:
        if isinstance(skill, dict):
            skills_nice_to_have.append(skill.get("name", ""))
        elif isinstance(skill, str):
            skills_nice_to_have.append(skill)

    skills_required = [s for s in skills_required if s]
    skills_nice_to_have = [s for s in skills_nice_to_have if s]

    # Text content (body is HTML in details)
    description = _strip_html(data.get("body"))
    requirements_text = None  # JustJoin embeds requirements in body
    offer_text = None

    # Timestamps
    posted_at = _parse_timestamp(data.get("publishedAt") or listing.get("publishedAt"))
    expires_at = _parse_timestamp(data.get("expiredAt") or data.get("expiresAt") or listing.get("expiredAt"))
    renewed_at = _parse_timestamp(data.get("lastPublishedAt") or listing.get("lastPublishedAt"))

    return NormalizedJobPosting(
        source="justjoin",
        source_id=source_id,
        slug=slug,
        job_url=job_url,
        ingested_at=ingested_at,
        title=title,
        company=company,
        locations=locations,
        work_mode=work_mode,
        seniority=seniority,
        employment_types=list(set(employment_types)),
        salaries=salaries,
        skills_required=skills_required,
        skills_nice_to_have=skills_nice_to_have,
        description=description,
        requirements_text=requirements_text,
        offer_text=offer_text,
        posted_at=posted_at,
        expires_at=expires_at,
        renewed_at=renewed_at,
    )

