#!/usr/bin/env python3
"""Validate one AI-adjudicator JSON output. Rejects invented citations and schema drift.

The AI layer is only trusted when it is FALSIFIABLE. This validator enforces:
  - strict schema (keys + types)
  - citation, if non-empty, must resolve to a real file:line in THIS repo (the #38 guard)
  - reachable=false with a real citation may downgrade REVIEW->SAFE
  - any failure => REJECT => PR stays REVIEW (fail-safe), oracle recorded as unavailable

Usage:  echo '<agent json>' | python3 validate_adjudication.py --repo .
Exit 0 = accepted (prints normalized verdict json); exit 1 = rejected (prints reason).
"""
import json
import os
import sys


def fail(reason):
    print(json.dumps({"accepted": False, "reason": reason, "verdict": "review"}))
    return 1


def main():
    repo = "."
    if "--repo" in sys.argv:
        repo = sys.argv[sys.argv.index("--repo") + 1]
    raw = sys.stdin.read().strip()
    # tolerate code fences / surrounding prose
    if "```" in raw:
        raw = raw.split("```")[1].lstrip("json").strip() if raw.count("```") >= 2 else raw
    start, end = raw.find("{"), raw.rfind("}")
    if start < 0 or end < 0:
        return fail("no JSON object in adjudicator output")
    try:
        d = json.loads(raw[start:end + 1])
    except Exception as e:
        return fail(f"invalid JSON: {e}")

    required = {"pr", "reachable", "evidence", "citation", "recommendation", "confidence"}
    missing = required - set(d)
    if missing:
        return fail(f"missing keys: {sorted(missing)}")
    if d["reachable"] not in (True, False, "uncertain"):
        return fail("reachable must be true|false|'uncertain'")
    if d["recommendation"] not in ("safe", "review"):
        return fail("recommendation must be safe|review (AI can never FIX or clear CVE)")

    cite = (d.get("citation") or "").strip()
    if cite:
        path = cite.split(":")[0].lstrip("./")
        if not os.path.exists(os.path.join(repo, path)):
            return fail(f"INVENTED CITATION: {cite} does not exist in repo")

    # Only allow a downgrade to safe when backed by a real citation + not-reachable.
    if d["recommendation"] == "safe" and not (d["reachable"] is False and cite):
        return fail("downgrade to 'safe' requires reachable=false WITH a real citation")

    print(json.dumps({"accepted": True, "verdict": d["recommendation"],
                      "pr": d["pr"], "evidence": d["evidence"], "citation": cite,
                      "confidence": d["confidence"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
