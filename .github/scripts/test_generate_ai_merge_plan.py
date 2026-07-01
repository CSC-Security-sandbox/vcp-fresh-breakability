#!/usr/bin/env python3
"""Tests for generate_ai_merge_plan.py"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generate_ai_merge_plan import _strip_preamble


class TestStripPreamble(unittest.TestCase):
    def test_strips_conversational_preamble(self):
        text = "Sure, here is the merge plan:\n# Breakability Merge Plan\n\nContent here"
        result = _strip_preamble(text)
        self.assertTrue(result.startswith("# Breakability Merge Plan"))

    def test_strips_code_fences(self):
        text = "```markdown\n# Breakability Merge Plan\n\nContent\n```"
        result = _strip_preamble(text)
        self.assertTrue(result.startswith("# Breakability Merge Plan"))
        self.assertNotIn("```", result)

    def test_preserves_clean_response(self):
        text = "# Breakability Merge Plan\n\nContent here"
        result = _strip_preamble(text)
        self.assertEqual(result, text)

    def test_strips_multiline_preamble(self):
        text = "I'll create the plan.\nHere it is:\n# Breakability Merge Plan\nLine 2"
        result = _strip_preamble(text)
        self.assertTrue(result.startswith("# Breakability Merge Plan"))
        self.assertIn("Line 2", result)

    def test_no_heading_returns_as_is(self):
        text = "This response has no heading at all"
        result = _strip_preamble(text)
        self.assertEqual(result, text)


if __name__ == "__main__":
    unittest.main()
