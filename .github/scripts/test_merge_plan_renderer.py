#!/usr/bin/env python3
"""Regression tests for the merge-plan renderer embedded in post-fallback-comments.sh."""
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("post-fallback-comments.sh")


def extract_merge_plan_python():
    body = SCRIPT.read_text()
    start = body.index("MERGE_PLAN_BODY=$(python3 << 'PYEOF'")
    start = body.index("\n", start) + 1
    end = body.index("\nPYEOF\n)", start)
    return body[start:end]


def base_pr(**overrides):
    pr = {
        "package": "example.com/lib",
        "ecosystem": "go",
        "from": "1.0.0",
        "to": "1.0.1",
        "bump": "patch",
        "dep_type": "production",
        "install_ok": True,
        "pkg_dir": "/",
        "verification_label": "L2_type_checked",
        "build": {"verdict": "pass", "new_errors": [], "main_exit": 0},
        "cves": [],
        "verdict_v2": {
            "verdict": "REVIEW",
            "severity": "medium",
            "confidence": "L2",
            "priority": "P2",
            "reason": "review needed",
            "residual": {"summary": "review needed", "check": "review:default"},
        },
    }
    pr.update(overrides)
    return pr


def render_plan(results):
    code = extract_merge_plan_python()
    with tempfile.TemporaryDirectory() as td:
        results_path = Path(td) / "build-results.json"
        results_path.write_text(json.dumps(results))
        code = code.replace('open("/tmp/build-results.json")', f'open({str(results_path)!r})')
        rendered = subprocess.run(
            [sys.executable, "-c", code],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={**os.environ, "GH_TOKEN": ""},
        )
        return rendered.stdout


def section(markdown, heading):
    _, rest = markdown.split(heading, 1)
    return rest.split("\n## ", 1)[0]


def has_pr(markdown, number):
    return re.search(rf"PR #{number}(?!\d)", markdown) is not None


class MergePlanRendererTests(unittest.TestCase):
    def test_glance_review_rows_move_out_of_manual_review(self):
        plan = render_plan({
            "prs": {
                "1": base_pr(
                    package="example.com/low",
                    verdict_v2={
                        "verdict": "REVIEW",
                        "severity": "low",
                        "confidence": "L3",
                        "priority": "P3",
                        "reason": "build, tests, and API diff are clean; changelog is unavailable",
                        "residual": {
                            "summary": "build, tests, and API diff are clean; changelog is unavailable",
                            "check": "glance:clean-missing-release-notes",
                        },
                    },
                ),
                "2": base_pr(
                    package="example.com/security",
                    cves=["CVE-2026-0001"],
                    verdict_v2={
                        "verdict": "REVIEW",
                        "severity": "low",
                        "confidence": "L3",
                        "priority": "P0",
                        "reason": "security-sensitive update requires human review",
                        "residual": {
                            "summary": "security-sensitive update requires human review",
                            "check": "glance:clean-missing-release-notes",
                        },
                    },
                ),
                "3": base_pr(package="example.com/medium"),
            },
            "metadata": {},
            "cross_pr_deps": [],
            "security_posture": {},
        })

        optional = section(plan, "## 🟡 Optional Glance — Low Breakability")
        manual = section(plan, "## ⚠️ Manual Review Needed")

        self.assertIn("GLANCE then MERGE — low breakability:** #1", plan)
        self.assertTrue(has_pr(optional, "1"))
        self.assertFalse(has_pr(manual, "1"))
        self.assertNotIn("Merge Risk: Medium", optional)

        self.assertFalse(has_pr(optional, "2"))
        self.assertTrue(has_pr(manual, "2"))
        self.assertTrue(has_pr(manual, "3"))


if __name__ == "__main__":
    unittest.main()
