"""Unit tests for No Fluff Jobs helpers."""

from __future__ import annotations

import unittest

from app.ingest.sources.nofluff import (
    NoFluffJobsClient,
    _extract_detail_sections,
    _has_salary,
    _walk_for_postings,
)


class TestHasSalary(unittest.TestCase):
    """Validate salary detection heuristics."""

    def test_top_level_salary(self) -> None:
        posting = {"salary": {"from": 1000, "to": 2000}}
        self.assertTrue(_has_salary(posting))

    def test_employment_type_salary(self) -> None:
        posting = {
            "employmentTypes": [
                {"type": "b2b", "salary": {"from": 5000, "to": 7000}},
                {"type": "perm"},
            ]
        }
        self.assertTrue(_has_salary(posting))

    def test_missing_salary(self) -> None:
        posting = {"title": "Engineer", "employmentTypes": [{"type": "b2b"}]}
        self.assertFalse(_has_salary(posting))


class TestWalkForPostings(unittest.TestCase):
    """Ensure posting discovery finds nested lists of dicts."""

    def test_finds_nested_postings(self) -> None:
        payload = {"outer": {"postings": [{"id": "one"}, {"id": "two"}]}}
        result = list(_walk_for_postings(payload))
        self.assertIn([{"id": "one"}, {"id": "two"}], result)


class TestBuildSearchParams(unittest.TestCase):
    """Validate search parameter construction."""

    def test_defaults_and_criteria_merge(self) -> None:
        client = NoFluffJobsClient()
        params = client._build_search_params(page=3, criteria={"tech": "python"})
        self.assertEqual(params["pageTo"], 3)
        self.assertEqual(params["pageSize"], client.settings.nofluff_page_size)
        self.assertTrue(params["withSalaryMatch"])
        self.assertEqual(params["tech"], "python")


class TestExtractDetailSections(unittest.TestCase):
    """Validate section extraction from detail payload."""

    def test_extracts_expected_sections(self) -> None:
        payload = {
            "requirements": {
                "musts": [
                    {"value": "Python"},
                    {"value": "SQL"},
                ],
                "description": "<p>Build services</p>",
            },
            "details": {"description": "<p>Private healthcare</p>"},
        }
        extracted = _extract_detail_sections(payload)
        self.assertEqual(extracted["must_have"], ["Python", "SQL"])
        self.assertEqual(extracted["requirements_description"], "Build services")
        self.assertEqual(extracted["offer_description"], "Private healthcare")


if __name__ == "__main__":
    unittest.main()


