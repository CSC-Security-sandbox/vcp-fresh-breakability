#!/usr/bin/env python3
"""Generate rich AI-powered PR comments using breakability-prompt.md.

Reads the full breakability-prompt.md (domain knowledge, verdict rules, visual
templates) plus build-results.json and calls the AI backend per PR to generate
200-300 line rich comments with all 13 golden features.

Falls back to breakability_analyst.py template rendering if AI call fails.

Usage:
  generate_ai_comments.py <build-results.json> \
    --prompt .github/breakability-prompt.md \
    [--model claude-4.5-sonnet] \
    [--run-url URL] \
    [--merge-plan-issue NUMBER]
"""
import argparse
import json
import os
import re
import sys
from datetime import date
from typing import Dict, Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ai_backend import Backend
from verdict_contract import authoritative_verdict


def _read_prompt(prompt_path: str) -> str:
    with open(prompt_path) as f:
        return f.read()


def _extract_pr_data(pr: Dict[str, Any]) -> str:
    """Serialize a single PR's data as JSON for the AI prompt context."""
    return json.dumps(pr, indent=2, default=str)


def _build_per_pr_prompt(
    base_prompt: str,
    pr: Dict[str, Any],
    pr_num: str,
    metadata: Dict[str, Any],
    run_url: Optional[str],
    merge_plan_issue: Optional[str],
    model_name: str,
    cross_deps: list,
    top_level: Dict[str, Any],
) -> str:
    """Build the full prompt for one PR: base instructions + PR-specific data."""
    pr_json = _extract_pr_data(pr)

    relevant_cross_deps = [
        d for d in cross_deps
        if str(d.get("pr_a")) == pr_num or str(d.get("pr_b")) == pr_num
    ]

    plan_ref = f"#{merge_plan_issue}" if merge_plan_issue else "#ISSUE_NUMBER"

    sections = [
        base_prompt,
        "\n\n---\n\n## CONTEXT FOR THIS PR\n",
        f"You are generating a comment for **PR #{pr_num}**.\n",
        f"Replace `#ISSUE_NUMBER` with `{plan_ref}` in the merge plan link.\n",
        f"\n### PR Data (from build-results.json)\n```json\n{pr_json}\n```\n",
    ]

    if relevant_cross_deps:
        sections.append(
            f"\n### Cross-PR Dependencies\n```json\n"
            f"{json.dumps(relevant_cross_deps, indent=2)}\n```\n"
        )

    workspace_graph = top_level.get("workspace_graph")
    if workspace_graph:
        sections.append(
            f"\n### Workspace Graph (monorepo structure)\n```json\n"
            f"{json.dumps(workspace_graph, indent=2, default=str)}\n```\n"
        )

    nestjs_skew = top_level.get("nestjs_skew")
    if nestjs_skew:
        sections.append(
            f"\n### NestJS Version Skew\n```json\n"
            f"{json.dumps(nestjs_skew, indent=2)}\n```\n"
        )

    govulncheck = top_level.get("govulncheck")
    if govulncheck:
        sections.append(
            f"\n### govulncheck Summary\n```json\n"
            f"{json.dumps(govulncheck, indent=2)}\n```\n"
        )

    security_posture = top_level.get("security_posture")
    if security_posture:
        sections.append(
            f"\n### Security Posture\n```json\n"
            f"{json.dumps(security_posture, indent=2)}\n```\n"
        )

    if metadata:
        sections.append(
            f"\n### Metadata\n- Repo: {metadata.get('repo', 'unknown')}\n"
            f"- Mode: {metadata.get('mode', 'advisory')}\n"
            f"- Timestamp: {metadata.get('timestamp', 'unknown')}\n"
        )

    footer_parts = []
    if run_url:
        footer_parts.append(f"Analysis run: {run_url}")
        sections.append(f"\n### Run Link\nInclude this link in the footer: [{run_url}]({run_url})\n")

    sections.append(
        f"\n### Footer Requirements\n"
        f"End the comment with:\n"
        f"```\n"
        f"---\n"
        f"Mode: Deterministic + Behavioral Probe · Model: {model_name} · "
        f"Analyzed: {date.today().isoformat()}\n"
    )
    if run_url:
        sections.append(f"[Analysis run]({run_url})\n")
    sections.append("```\n")

    sections.append(
        "\n### OUTPUT INSTRUCTIONS\n"
        "Generate the COMPLETE PR comment in markdown. Start with `<!-- breakability-check -->` "
        "on the first line. Follow the visual format templates from Section 4/5 of the prompt.\n\n"
        "MANDATORY REQUIREMENTS:\n"
        "- The comment MUST be at least 150 lines long. Aim for 200-300 lines.\n"
        "- Include ALL sections: headline, signal summary table (7 rows), per-layer narrative "
        "(Build Analysis, Test Analysis, etc. with 'What we checked' bullets and actual "
        "stdout/stderr in code blocks), behavioral probe with SHA256 hashes, reachability "
        "with file:line references, policy decision pseudocode, final recommendation with "
        "numbered steps, and independent verification resources.\n"
        "- MUST include at least one ```bash code block with reproducible verification commands.\n"
        "- MUST include numbered action steps (1. 2. 3.) in the recommendation section.\n"
        "- Each per-layer section needs a confidence rating (HIGH/MEDIUM/LOW) with reasoning.\n"
        "- Output ONLY the markdown comment — no preamble, no explanation.\n"
    )

    return "\n".join(sections)


def _ensure_marker(comment: str) -> str:
    """Ensure the comment starts with the breakability marker."""
    marker = "<!-- breakability-check -->"
    stripped = comment.strip()
    if not stripped.startswith(marker):
        return f"{marker}\n{stripped}"
    return stripped


def _validate_comment(comment: str, pr_num: str) -> tuple:
    """Validate that the AI output meets golden standard quality bars.

    Returns (passed: bool, diagnostics: dict) where diagnostics maps each
    criterion to {passed: bool, value: any}.

    Checks 8 of 13 golden features: line count, H2 headers, signal table,
    H3 subsections, Mode footer, numbered recommendations, verification
    commands, and reachability section.
    """
    line_count = len(comment.strip().splitlines())
    comment_lower = comment.lower()

    diagnostics = {
        "line_count": {"passed": line_count >= 150, "value": line_count},
        "has_h2": {"passed": "##" in comment, "value": "##" in comment},
        "has_signal_table": {
            "passed": "| Layer " in comment or "| Check " in comment or "| Signal " in comment,
            "value": "| Layer " in comment or "| Check " in comment or "| Signal " in comment,
        },
        "has_h3": {"passed": "###" in comment, "value": "###" in comment},
        "has_mode_footer": {"passed": "Mode:" in comment, "value": "Mode:" in comment},
        "has_numbered_list": {
            "passed": bool(re.search(r'\d+[\.\)]\s', comment)),
            "value": bool(re.search(r'\d+[\.\)]\s', comment)),
        },
        "has_bash_block": {
            "passed": "```bash" in comment or "```shell" in comment,
            "value": "```bash" in comment or "```shell" in comment,
        },
        "has_reachability": {
            "passed": "reachab" in comment_lower or "import" in comment_lower,
            "value": "reachab" in comment_lower or "import" in comment_lower,
        },
    }

    all_passed = all(d["passed"] for d in diagnostics.values())

    if not all_passed:
        parts = []
        for name, d in diagnostics.items():
            val = d["value"]
            status = "FAIL" if not d["passed"] else "ok"
            parts.append(f"{name}={val}({status})")
        print(f"PR#{pr_num} validation: {', '.join(parts)}", file=sys.stderr)
    elif line_count < 200:
        print(f"PR#{pr_num}: AI comment is {line_count} lines (below 200-line golden target)", file=sys.stderr)

    return (all_passed, diagnostics)


def _near_valid(diagnostics: dict) -> bool:
    """Accept a comment without retry when it is long enough and nearly passes."""
    lc = diagnostics.get("line_count", {})
    if (lc.get("value") or 0) < 300:
        return False
    failures = sum(1 for d in diagnostics.values() if not d.get("passed"))
    return failures <= 1


def _fallback_comment(pr: Dict[str, Any], pr_num: str, run_url: Optional[str],
                      merge_plan_issue: Optional[str], model_name: str) -> str:
    """Generate a minimal fallback comment when AI fails for a single PR."""
    pkg = pr.get("package", "unknown")
    from_ver = pr.get("from", "?")
    to_ver = pr.get("to", "?")
    dep_type = pr.get("dep_type", "unknown")
    bump = pr.get("bump", "unknown")
    plan_ref = f"#{merge_plan_issue}" if merge_plan_issue else "#ISSUE_NUMBER"

    av = authoritative_verdict(pr)
    verdict = av.get("verdict", "REVIEW")
    emoji_map = {"SAFE": "✅", "BLOCKED": "🚫", "REVIEW": "⚠️"}
    emoji = emoji_map.get(verdict, "⚠️")

    lines = [
        "<!-- breakability-check -->",
        f"## {emoji} {verdict} — `{pkg}` {from_ver} → {to_ver} • {dep_type} • {bump}",
        "",
        "| Check | Result |",
        "|-------|--------|",
        "| Build | ⚙️ Analysis incomplete — AI comment generation failed |",
        "",
        "**Manual review required.** AI-powered analysis could not be completed for this PR.",
        "",
        f"📋 Merge plan: {plan_ref}",
        "",
        "---",
        f"Mode: Deterministic + Behavioral Probe · Model: {model_name} (fallback) · "
        f"Analyzed: {date.today().isoformat()}",
    ]
    if run_url:
        lines.append(f"[Analysis run]({run_url})")

    return "\n".join(lines)


def generate_comments(
    build_results: Dict[str, Any],
    prompt_path: str,
    model: str = "claude-4.5-sonnet",
    run_url: Optional[str] = None,
    merge_plan_issue: Optional[str] = None,
) -> Dict[str, str]:
    """Generate AI comments for all PRs. Returns {pr_num: comment_text}."""
    base_prompt = _read_prompt(prompt_path)
    metadata = build_results.get("metadata", {})
    cross_deps = build_results.get("cross_pr_deps", [])

    top_level = {
        k: build_results.get(k)
        for k in ("workspace_graph", "nestjs_skew", "govulncheck", "security_posture")
        if build_results.get(k)
    }

    prs = build_results.get("prs", {})
    results_list = build_results.get("results", [])

    pr_items = []
    if prs:
        for pr_num_str, pr_data in prs.items():
            if isinstance(pr_data, dict):
                pr_data.setdefault("pr_num", pr_num_str)
                pr_items.append((pr_num_str, pr_data))
    elif results_list:
        for pr_data in results_list:
            pr_num_str = str(pr_data.get("pr_num", pr_data.get("pr", "")))
            if pr_num_str:
                pr_items.append((pr_num_str, pr_data))

    if not pr_items:
        print("No PRs found in build-results.json", file=sys.stderr)
        return {}

    # Skip PRs with breakability:skip label
    pr_items = [
        (num, data) for num, data in pr_items
        if data.get("build", {}).get("verdict") != "skipped"
    ]

    backend = Backend.from_env(model=model)
    comments = {}

    for pr_num, pr_data in sorted(pr_items, key=lambda x: int(x[0]) if x[0].isdigit() else 0):
        print(f"PR#{pr_num}: Generating AI comment (model={backend.model})...", file=sys.stderr)

        prompt = _build_per_pr_prompt(
            base_prompt=base_prompt,
            pr=pr_data,
            pr_num=pr_num,
            metadata=metadata,
            run_url=run_url,
            merge_plan_issue=merge_plan_issue,
            model_name=model,
            cross_deps=cross_deps,
            top_level=top_level,
        )

        comment = None
        for attempt in range(2):
            response = backend.invoke(
                prompt,
                namespace="breakability-comment",
                key=f"comment-pr-{pr_num}" if attempt == 0 else f"comment-pr-{pr_num}-retry",
            )

            if response:
                valid, diag = _validate_comment(response, pr_num)
                if valid:
                    comment = _ensure_marker(response)
                    line_count = len(comment.splitlines())
                    print(f"PR#{pr_num}: AI comment generated ({line_count} lines)", file=sys.stderr)
                    break
                if _near_valid(diag):
                    comment = _ensure_marker(response)
                    line_count = len(comment.splitlines())
                    print(f"PR#{pr_num}: AI comment near-valid, accepted ({line_count} lines)", file=sys.stderr)
                    break
                reason = "validation failed"
                preview = response[:200].replace('\n', '\\n')
                print(f"PR#{pr_num}: response preview ({len(response)} chars): {preview}", file=sys.stderr)
            else:
                reason = f"empty response (0 chars)"
                print(f"PR#{pr_num}: {reason}", file=sys.stderr)
            if attempt == 0:
                print(f"PR#{pr_num}: AI call {reason}, retrying once...", file=sys.stderr)

        if comment:
            comments[pr_num] = comment
        else:
            print(f"PR#{pr_num}: AI failed after retry, using fallback", file=sys.stderr)
            comments[pr_num] = _fallback_comment(
                pr_data, pr_num, run_url, merge_plan_issue, model
            )

    return comments


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("build_results", help="Path to build-results.json")
    ap.add_argument(
        "--prompt",
        default=".github/breakability-prompt.md",
        help="Path to breakability-prompt.md",
    )
    ap.add_argument("--model", default="claude-4.5-sonnet", help="AI model to use")
    ap.add_argument("--run-url", default=None, help="GitHub Actions run URL")
    ap.add_argument("--merge-plan-issue", default=None, help="Merge plan issue number")
    ap.add_argument("--pr", type=str, help="Generate for a single PR only")
    ap.add_argument("--stdout", action="store_true", help="Write to stdout instead of files")
    args = ap.parse_args()

    if not os.path.exists(args.prompt):
        print(f"Prompt file not found: {args.prompt}", file=sys.stderr)
        print("Falling back to breakability_analyst.py", file=sys.stderr)
        sys.exit(2)

    with open(args.build_results) as f:
        build_results = json.load(f)

    run_url = args.run_url or os.environ.get("ANALYSIS_RUN_URL")

    comments = generate_comments(
        build_results=build_results,
        prompt_path=args.prompt,
        model=args.model,
        run_url=run_url,
        merge_plan_issue=args.merge_plan_issue,
    )

    if args.pr:
        comments = {k: v for k, v in comments.items() if k == args.pr}

    stub_count = 0
    real_count = 0
    written = 0
    for pr_num, comment in comments.items():
        is_stub = "AI comment generation failed" in comment or len(comment.strip().splitlines()) < 30
        if is_stub:
            stub_count += 1
        else:
            real_count += 1
        if args.stdout:
            print(f"\n{'='*60}\nPR #{pr_num}\n{'='*60}")
            print(comment)
        else:
            output_file = f"/tmp/pr-{pr_num}-comment.md"
            with open(output_file, "w") as f:
                f.write(comment)
            print(f"✅ PR #{pr_num} → {output_file}", file=sys.stderr)
        written += 1

    print(f"\n✅ Generated {written} AI comments ({real_count} AI, {stub_count} stubs)", file=sys.stderr)

    if written > 0 and stub_count == written:
        print(
            f"⚠️ All {stub_count} comments are fallback stubs (AI backend unavailable). "
            "Exiting non-zero so workflow falls back to breakability_analyst.py.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
