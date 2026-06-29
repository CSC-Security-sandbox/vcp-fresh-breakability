#!/usr/bin/env python3
"""Tests for generate_ai_comments.py"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_ai_comments import (
    _build_per_pr_prompt,
    _validate_comment,
    _fallback_comment,
    _ensure_marker,
    _extract_pr_data,
)


SAMPLE_PR = {
    "pr_num": "42",
    "package": "lodash",
    "from": "4.17.20",
    "to": "4.17.21",
    "bump": "patch",
    "dep_type": "production",
    "build": {"verdict": "pass", "pr_exit": 0},
    "test": {"ran": True, "exit": 0},
    "deterministic": {"api_changes": 0, "changelogSignal": "clean"},
}


class TestValidateComment(unittest.TestCase):
    def _make_comment(self, lines=120, has_table=True, has_subsection=True, has_footer=True):
        parts = ["<!-- breakability-check -->", "## SAFE — lodash"]
        if has_table:
            parts.append("| Layer | Signal | Detail |")
        if has_subsection:
            parts.append("### How we checked")
        body_lines = [f"Line {i}" for i in range(lines - len(parts) - (1 if has_footer else 0))]
        parts.extend(body_lines)
        if has_footer:
            parts.append("Mode: Deterministic + Behavioral Probe")
        return "\n".join(parts)

    def test_valid_comment_passes(self):
        comment = self._make_comment(lines=120)
        self.assertTrue(_validate_comment(comment, "42"))

    def test_too_short_fails(self):
        comment = self._make_comment(lines=50)
        self.assertFalse(_validate_comment(comment, "42"))

    def test_missing_table_fails(self):
        comment = self._make_comment(has_table=False)
        self.assertFalse(_validate_comment(comment, "42"))

    def test_missing_subsection_fails(self):
        comment = self._make_comment(has_subsection=False)
        self.assertFalse(_validate_comment(comment, "42"))

    def test_missing_footer_fails(self):
        comment = self._make_comment(has_footer=False)
        self.assertFalse(_validate_comment(comment, "42"))


class TestFallbackComment(unittest.TestCase):
    def test_fallback_includes_package(self):
        comment = _fallback_comment(SAMPLE_PR, "42", None, None, "claude-sonnet-4.5")
        self.assertIn("lodash", comment)
        self.assertIn("4.17.20", comment)
        self.assertIn("4.17.21", comment)

    def test_fallback_includes_marker(self):
        comment = _fallback_comment(SAMPLE_PR, "42", None, None, "claude-sonnet-4.5")
        self.assertIn("<!-- breakability-check -->", comment)

    def test_fallback_includes_run_url(self):
        comment = _fallback_comment(SAMPLE_PR, "42", "https://example.com/run/1", None, "claude-sonnet-4.5")
        self.assertIn("https://example.com/run/1", comment)

    def test_fallback_includes_merge_plan(self):
        comment = _fallback_comment(SAMPLE_PR, "42", None, "99", "claude-sonnet-4.5")
        self.assertIn("#99", comment)


class TestBuildPrompt(unittest.TestCase):
    def test_prompt_contains_pr_data(self):
        prompt = _build_per_pr_prompt(
            base_prompt="Base instructions here",
            pr=SAMPLE_PR, pr_num="42",
            metadata={"repo": "test/repo", "mode": "advisory"},
            run_url="https://example.com/run", merge_plan_issue="10",
            model_name="claude-sonnet-4.5", cross_deps=[], top_level={},
        )
        self.assertIn("PR #42", prompt)
        self.assertIn("lodash", prompt)
        self.assertIn("#10", prompt)
        self.assertIn("https://example.com/run", prompt)

    def test_prompt_includes_cross_deps(self):
        deps = [{"pr_a": "42", "pr_b": "43", "reason": "shared dep"}]
        prompt = _build_per_pr_prompt(
            base_prompt="Base", pr=SAMPLE_PR, pr_num="42",
            metadata={}, run_url=None, merge_plan_issue=None,
            model_name="test", cross_deps=deps, top_level={},
        )
        self.assertIn("Cross-PR Dependencies", prompt)
        self.assertIn("shared dep", prompt)


class TestEnsureMarker(unittest.TestCase):
    def test_adds_marker(self):
        result = _ensure_marker("## Some comment")
        self.assertTrue(result.startswith("<!-- breakability-check -->"))

    def test_preserves_existing_marker(self):
        text = "<!-- breakability-check -->\n## Comment"
        result = _ensure_marker(text)
        self.assertEqual(result.count("breakability-check"), 1)


class TestExtractPrData(unittest.TestCase):
    def test_serializes_pr(self):
        result = _extract_pr_data(SAMPLE_PR)
        data = json.loads(result)
        self.assertEqual(data["package"], "lodash")


if __name__ == "__main__":
    unittest.main()
