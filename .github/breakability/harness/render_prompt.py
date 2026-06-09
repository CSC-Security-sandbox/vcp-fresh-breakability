#!/usr/bin/env python3
"""Render the per-PR AI adjudicator prompt from build-results.json.

Usage: render_prompt.py <build-results.json> <pr_id> <prompt_template.md>
Prints the rendered prompt to stdout (ready to pipe into the `agent` CLI).
Substitutes the {{...}} fields the template declares, scoped to the bumped module.
"""
import json
import sys


def module_dir(pr):
    d = (pr.get("pkg_dir") or "/").strip()
    return "" if d in ("/", ".", "") else d.strip("/")


def in_module(fp, mod):
    f = fp.lstrip("./")
    if mod == "":
        return not (f.startswith("cicd/") or f.startswith("automations/"))
    return f.startswith(mod + "/") or f == mod


def main():
    results_path, pr_id, tmpl_path = sys.argv[1], sys.argv[2], sys.argv[3]
    prs = json.load(open(results_path)).get("prs", {})
    pr = prs.get(str(pr_id))
    if not pr:
        sys.exit(1)
    det = pr.get("deterministic") or {}
    mod = module_dir(pr)

    bullets = "\n".join(
        f"- {b}" for b in ((det.get("changelogSignal") or {}).get("bullets") or [])
    ) or "(none recorded)"

    apidiff = "\n".join(
        f"- {c.get('symbol')}: {c.get('changeType')} "
        f"({'HARD' if c.get('isHardBreak') else 'soft'})"
        for c in (det.get("api_changes_detail") or [])
    ) or "(none)"

    in_mod_sites = [
        f"- {u.get('file')}:{u.get('line')} {u.get('symbol')} ({u.get('context')})"
        for u in (det.get("usages") or [])
        if in_module(u.get("file", ""), mod)
    ]
    sites = "\n".join(in_mod_sites) or "(no recorded usages inside the bumped module)"

    sub = {
        "pr_id": str(pr_id),
        "package": pr.get("package", "?"),
        "from": pr.get("from", "?"),
        "to": pr.get("to", "?"),
        "bump": pr.get("bump", "?"),
        "changelog_breaking_bullets": bullets,
        "apidiff_symbols": apidiff,
        "our_call_sites_in_bumped_module": sites,
        "build_verdict": (pr.get("build") or {}).get("verdict", "?"),
        "test_verdict": (pr.get("test") or {}).get("verdict", "n/a"),
    }
    text = open(tmpl_path).read()
    for k, v in sub.items():
        text = text.replace("{{" + k + "}}", str(v))
    print(text)


if __name__ == "__main__":
    main()
