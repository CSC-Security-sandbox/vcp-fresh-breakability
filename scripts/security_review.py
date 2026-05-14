"""
LLM-powered Security Review for GitHub Pull Requests.

Fetches the PR diff via `gh` CLI, sends it to Claude through the NetApp LLM
proxy, then posts the security analysis as PR comments (inline + summary).

Required environment variables
------------------------------
LLM_PROXY_API_KEY   – API key for https://llm-proxy-api.ai.eng.netapp.com
LLM_PROXY_USER      – User identifier required by the proxy (e.g. AD username)
GH_TOKEN            – GitHub token with `pull-requests: write` scope
GITHUB_REPOSITORY   – owner/repo  (set automatically by Actions)
PR_NUMBER           – Pull-request number to review
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from typing import Any

import anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LLM_PROXY_BASE_URL = "https://llm-proxy-api.ai.eng.netapp.com"
MODEL = "claude-opus-4.6"
MAX_TOKENS = 128000

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(name: str) -> str:
    """Return an env-var or abort with a clear message."""
    value = os.environ.get(name, "").strip()
    if not value:
        sys.exit(f"ERROR: Required environment variable {name!r} is not set.")
    return value


def _gh(*args: str) -> str:
    """Run a `gh` CLI command and return stdout."""
    result = subprocess.run(
        ["gh", *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(f"gh {' '.join(args)} failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# 1. Gather context
# ---------------------------------------------------------------------------

def get_pr_diff(repo: str, pr_number: str) -> str:
    """Fetch the unified diff for the PR."""
    return _gh("pr", "diff", pr_number, "--repo", repo)


def get_pr_info(repo: str, pr_number: str) -> dict[str, Any]:
    """Fetch PR metadata (title, body, head/base refs)."""
    raw = _gh(
        "pr", "view", pr_number,
        "--repo", repo,
        "--json", "title,body,headRefName,baseRefName,headRefOid,baseRefOid",
    )
    return json.loads(raw)


def get_changed_files(repo: str, pr_number: str) -> str:
    """List files changed in the PR."""
    return _gh("pr", "diff", pr_number, "--repo", repo, "--name-only")


def get_existing_review_threads(repo: str, pr_number: str) -> str:
    """Fetch existing review threads so the model can resolve/re-report."""
    return _gh(
        "pr", "view", pr_number,
        "--repo", repo,
        "--json", "comments,reviews",
    )


def read_bugbot_rules() -> str:
    """Read project-specific SAST rules if they exist."""
    for path in (".cursor/rules/BUGBOT.md", ".github/prompts/security-review.prompt.md"):
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as fh:
                return fh.read()
    return ""


# ---------------------------------------------------------------------------
# 2. Build the security review prompt (matches cursor-review.yml style)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Act as a senior cybersecurity expert and application security specialist.
Perform a comprehensive security audit of the provided PR diff with the
following requirements.

Analysis Scope — identify all vulnerabilities including OWASP Top 10 2021:
- A01 Broken Access Control, A02 Cryptographic Failures, A03 Injection,
  A04 Insecure Design, A05 Security Misconfiguration, A06 Vulnerable Components,
  A07 Identification and Authentication Failures, A08 Software and Data Integrity Failures,
  A09 Security Logging and Monitoring Failures, A10 SSRF
- Also review: input validation, output encoding, session management, crypto implementations,
  error handling and information disclosure, business logic flaws, race conditions,
  memory safety, API security, data protection, supply chain security

For each vulnerability found provide:
1. Exact location — file path and line number
2. Vulnerability type — e.g. SQL Injection, XSS, JWT Algorithm Confusion
3. Severity — Critical / High / Medium / Low with CVSS score where applicable
4. Impact — exploitation scenario and business impact
5. Root cause — why the vulnerability exists
6. Proof of concept — minimal example showing exploitability
7. Remediation — specific actionable fix with secure code snippet
8. Prevention — best practices to prevent recurrence

Quality standards:
- Minimise false positives; only report concrete, exploitable findings
- Prioritise by exploitability × impact
- Consider the application threat model and attack surface

Response rules:
- Do NOT comment on code style, spelling, formatting, or any non-security issues
- Only use severity labels: [CRITICAL], [HIGH], [MEDIUM], [LOW]
- Do NOT use vague language — state EXACTLY what the attacker does and gains
- Include specific payloads, URLs, and POST data in exploitation scenarios

You MUST respond using EXACTLY the format below with the EXACT delimiter lines shown.
Do NOT wrap the output in any extra code fences or JSON.

STEP 1 — Emit the inline comments as a JSON array between these delimiters:

---INLINE_START---
[
  {
    "path": "<file path relative to repo root>",
    "line": <diff line number (integer)>,
    "side": "RIGHT",
    "body": "[CRITICAL] **<Vuln Type>** -- <concise impact statement>"
  }
]
---INLINE_END---

If there are no inline findings, emit an empty array between the delimiters.
Use severity tags in the body: [CRITICAL], [HIGH], [MEDIUM], [LOW].

STEP 2 — Emit the summary as raw Markdown between these delimiters:

---SUMMARY_START---
## Security Analysis Report

### Executive Summary
[2-3 sentence overview: what the PR does, total finding count by severity, overall risk verdict]

| Severity | Count |
|---|---|
| Critical | N |
| High | N |
| Medium | N |
| Low | N |

---

### Critical Vulnerabilities
[List names only with file:line reference, or write "None identified."]

---

### Detailed Findings

#### Finding #N -- [Vulnerability Name]
- **Location**: /path/to/file.ext, line(s) N
- **Type**: [OWASP category] -- [Vulnerability Classification]
- **Severity**: [CRITICAL/HIGH/MEDIUM/LOW] (CVSS [score]: [vector string])
- **Impact**: [Exploitation scenario and business impact -- 2-3 sentences]
- **Root Cause**: [Technical explanation]
- **Vulnerable Code**: (show the vulnerable snippet)
- **Proof of Concept**: (show the exploit)
- **Fix**: (show corrected code)
- **Prevention**: [Best practice(s)]

---

### Security Recommendations
[Bullet list of 3-5 general hardening improvements]
---SUMMARY_END---

Severity tags for inline comments:
- [CRITICAL] -- RCE, auth bypass, data loss without prerequisites
- [HIGH]     -- exploitable with standard user access or specific conditions
- [MEDIUM]   -- incorrect behaviour, race condition, off-by-one
- [LOW]      -- non-blocking hardening suggestion

If no high-confidence vulnerability is found, return an empty inline array
and a summary stating the diff looks clean.
"""


def build_user_message(
    repo: str,
    pr_number: str,
    pr_info: dict[str, Any],
    diff: str,
    changed_files: str,
    existing_threads: str,
    project_rules: str,
) -> str:
    """Compose the user message sent to the model."""
    parts = [
        f"## Context",
        f"- Repo: {repo}",
        f"- PR #{pr_number}: {pr_info.get('title', '')}",
        f"- Base: {pr_info.get('baseRefName', '')} ({pr_info.get('baseRefOid', '')[:8]})",
        f"- Head: {pr_info.get('headRefName', '')} ({pr_info.get('headRefOid', '')[:8]})",
        "",
        "## Changed files",
        changed_files,
        "",
    ]

    if project_rules:
        parts += [
            "## Project-specific security rules",
            project_rules,
            "",
        ]

    if existing_threads and existing_threads != "{}":
        parts += [
            "## Existing review threads (validate & resolve or re-report)",
            existing_threads,
            "",
        ]

    parts += [
        "## PR Diff",
        "```diff",
        diff,
        "```",
    ]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# 3. Call the LLM via the NetApp proxy
# ---------------------------------------------------------------------------

def call_llm(system: str, user_msg: str) -> str:
    """Send the review request to Claude via the NetApp LLM proxy."""
    api_key = _env("LLM_PROXY_API_KEY")
    proxy_user = _env("LLM_PROXY_USER")

    client = anthropic.Anthropic(
        base_url=LLM_PROXY_BASE_URL,
        api_key=api_key,
        timeout=300.0,
    )

    message = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
        extra_body={"user": proxy_user},
    )
    return message.content[0].text


# ---------------------------------------------------------------------------
# 4. Parse the model response  (delimiter-based — simple & robust)
# ---------------------------------------------------------------------------

INLINE_START = "---INLINE_START---"
INLINE_END   = "---INLINE_END---"
SUMMARY_START = "---SUMMARY_START---"
SUMMARY_END   = "---SUMMARY_END---"


def _extract_between(text: str, start_tag: str, end_tag: str) -> str | None:
    """Return the text between *start_tag* and *end_tag*, or None."""
    s = text.find(start_tag)
    if s == -1:
        return None
    s += len(start_tag)
    e = text.find(end_tag, s)
    if e == -1:
        return None
    return text[s:e].strip()


def parse_response(text: str) -> tuple[list[dict], str]:
    """
    Extract inline comments (JSON array) and summary (raw Markdown)
    from the model's delimited response.

    Returns (inline_comments, summary_markdown).
    """
    inline_comments: list[dict] = []
    summary_md = ""

    # --- Inline comments (JSON array between delimiters) -----------------
    inline_raw = _extract_between(text, INLINE_START, INLINE_END)
    if inline_raw:
        # Strip optional code fences the model might still add
        inline_raw = re.sub(r"^```(?:json)?\s*\n?", "", inline_raw)
        inline_raw = re.sub(r"\n?```\s*$", "", inline_raw)
        try:
            parsed = json.loads(inline_raw)
            if isinstance(parsed, list):
                inline_comments = parsed
        except json.JSONDecodeError as exc:
            print(f"WARNING: Failed to parse inline JSON: {exc}", file=sys.stderr)

    # --- Summary (raw Markdown between delimiters) -----------------------
    summary_raw = _extract_between(text, SUMMARY_START, SUMMARY_END)
    if summary_raw:
        summary_md = summary_raw

    # --- Fallback: if delimiters were missing, salvage what we can -------
    if not summary_md:
        print("WARNING: Summary delimiters not found, using fallback.", file=sys.stderr)
        # Remove the inline JSON block if present
        cleaned = text
        if inline_raw:
            cleaned = cleaned.replace(inline_raw, "")
        # Strip stray delimiter lines and code fences
        for tag in (INLINE_START, INLINE_END, SUMMARY_START, SUMMARY_END):
            cleaned = cleaned.replace(tag, "")
        cleaned = re.sub(r"```json\s*\n?", "", cleaned)
        cleaned = re.sub(r"```\s*$", "", cleaned)
        cleaned = cleaned.strip()
        summary_md = cleaned if cleaned else "*(Security review produced no parseable output.)*"

    return inline_comments, summary_md


# ---------------------------------------------------------------------------
# 4b. Validate outputs before posting
# ---------------------------------------------------------------------------

def validate_inline_comments(comments: list[dict]) -> list[dict]:
    """Keep only well-formed inline comment dicts; drop the rest."""
    valid = []
    for c in comments:
        if not isinstance(c, dict):
            continue
        path = c.get("path")
        line = c.get("line")
        body = c.get("body")
        if not path or not isinstance(line, int) or not body:
            print(f"WARNING: Dropping malformed inline comment: {c!r}", file=sys.stderr)
            continue
        valid.append({
            "path": str(path),
            "line": int(line),
            "side": c.get("side", "RIGHT"),
            "body": str(body),
        })
    return valid


def validate_summary(md: str, max_len: int = 65536) -> str:
    """Ensure the summary is clean Markdown that GitHub will render."""
    if not md or not md.strip():
        return "*(Security review produced no output.)*"
    # Truncate if it exceeds GitHub comment size limit
    if len(md) > max_len:
        md = md[:max_len] + "\n\n*(truncated — exceeded GitHub comment size limit)*"
    return md


# ---------------------------------------------------------------------------
# 5. Post comments on the PR
# ---------------------------------------------------------------------------

BOT_MARKER = "<!-- llm-security-review-bot -->"


def post_summary_comment(repo: str, pr_number: str, body: str) -> None:
    """Post or update the summary comment on the PR (uses hidden marker)."""
    body_with_marker = f"{BOT_MARKER}\n{body}"

    # Search for an existing bot comment to update
    existing = subprocess.run(
        ["gh", "api", f"repos/{repo}/issues/{pr_number}/comments",
         "--jq", f'.[] | select(.body | contains("{BOT_MARKER}")) | .id'],
        capture_output=True, text=True, check=False,
    )
    comment_id = existing.stdout.strip().split("\n")[0] if existing.stdout.strip() else ""

    if comment_id:
        # Update existing comment
        subprocess.run(
            ["gh", "api", "--method", "PATCH",
             f"repos/{repo}/issues/comments/{comment_id}",
             "-f", f"body={body_with_marker}"],
            capture_output=True, text=True, check=False,
        )
        print(f"✅ Summary comment updated on PR #{pr_number}")
    else:
        # Create new comment
        _gh(
            "pr", "comment", pr_number,
            "--repo", repo,
            "--body", body_with_marker,
        )
        print(f"✅ Summary comment posted on PR #{pr_number}")


def post_inline_comments(
    repo: str,
    pr_number: str,
    comments: list[dict],
) -> None:
    """Post inline review comments on specific diff lines."""
    if not comments:
        print("ℹ️  No inline comments to post.")
        return

    # Build a single review with all inline comments via the GitHub API
    # gh api  --method POST  repos/{owner}/{repo}/pulls/{pr}/reviews
    review_body = json.dumps({
        "body": "🔍 **Automated Security Review** — inline findings attached.",
        "event": "COMMENT",
        "comments": [
            {
                "path": c["path"],
                "line": c.get("line", 1),
                "side": c.get("side", "RIGHT"),
                "body": c["body"],
            }
            for c in comments
        ],
    })

    result = subprocess.run(
        [
            "gh", "api",
            "--method", "POST",
            "-H", "Accept: application/vnd.github+json",
            f"repos/{repo}/pulls/{pr_number}/reviews",
            "--input", "-",
        ],
        input=review_body,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        # Inline comments can fail if line numbers don't map to the diff;
        # fall back to posting them as regular comments.
        print(f"⚠️  Inline review failed ({result.stderr.strip()}), "
              "falling back to summary-only.")
    else:
        print(f"✅ {len(comments)} inline comment(s) posted on PR #{pr_number}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    repo = _env("GITHUB_REPOSITORY")
    pr_number = _env("PR_NUMBER")

    print(f"🔍 Starting security review for {repo} PR #{pr_number} ...")

    # 1. Gather context
    pr_info = get_pr_info(repo, pr_number)
    diff = get_pr_diff(repo, pr_number)
    changed_files = get_changed_files(repo, pr_number)
    existing_threads = get_existing_review_threads(repo, pr_number)
    project_rules = read_bugbot_rules()

    if not diff:
        print("ℹ️  PR diff is empty — nothing to review.")
        post_summary_comment(
            repo, pr_number,
            "## Security Analysis Report\n\n"
            "No code changes detected in this PR. Nothing to review.",
        )
        return

    # Truncate very large diffs to stay within model context window
    max_diff_chars = 100_000
    if len(diff) > max_diff_chars:
        diff = diff[:max_diff_chars] + "\n\n... [diff truncated] ..."
        print(f"⚠️  Diff truncated to {max_diff_chars} chars.")

    # 2. Build prompt
    user_msg = build_user_message(
        repo, pr_number, pr_info,
        diff, changed_files, existing_threads, project_rules,
    )

    # 3. Call LLM via NetApp proxy
    print(f"📡 Calling {MODEL} via LLM proxy ...")
    raw_response = call_llm(SYSTEM_PROMPT, user_msg)
    print(f"📝 Received {len(raw_response)} chars from model.")

    # 4. Parse response
    inline_comments, summary_md = parse_response(raw_response)

    # 5. Validate before posting
    inline_comments = validate_inline_comments(inline_comments)
    summary_md = validate_summary(summary_md)

    # 6. Post to PR
    post_inline_comments(repo, pr_number, inline_comments)
    post_summary_comment(repo, pr_number, summary_md)

    print("✅ Security review completed successfully.")


if __name__ == "__main__":
    main()
