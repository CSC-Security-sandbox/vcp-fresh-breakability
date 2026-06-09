#!/usr/bin/env python3
"""Breakability acceptance gate — the deterministic fitness function for the loop.

Replaces the LLM "SCORE: X.X" vibe oracle in loop.sh. Run in seconds, no CI.

Pipeline:
  1. Load build-results.json (deterministic tool output).
  2. Derive a prediction per PR (auto_clear/review/fix) from build verdict + merge_risk.
  3. Score predictions vs corpus.json (verified labels) using breakability_eval.Scorer.
  4. INVENTED-CITATION GUARD: any PR whose verdict claims break-reachability but whose
     files_importing is empty (or points to files that don't exist) is a fabricated claim
     -> HARD FAIL. This is the #38 failure mode (claimed Error.Code reachable in a module
     that does not import lib/pq).
  5. GOLDEN GUARD (optional): if golden_predictions.json exists, any categorization drift
     for a previously-correct PR is flagged.
  6. Emit machine-parseable SCORE + GATE lines for loop.sh.

ACCEPTED iff: zero_false_green AND zero_invented_citations AND no_golden_regression.

Usage:
  python3 run_gate.py <build-results.json> <corpus.json> [--repo <root>] [--golden <file>]
Exit code 0 = ACCEPTED, 1 = REJECTED.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "scripts"))
from breakability_eval import CorpusCase, Scorer  # noqa: E402


def derive_prediction(pr):
    """Map deterministic tool output -> {auto_clear|review|fix}. Mirrors SPEC buckets."""
    build = pr.get("build") or {}
    if build.get("verdict") == "fail":
        return "fix"
    tag = ((pr.get("merge_risk") or {}).get("tag") or "").lower()
    if tag in ("low", "none", ""):
        return "auto_clear"
    return "review"  # medium / high -> needs a look


def claims_reachability(pr):
    reason = ((pr.get("merge_risk") or {}).get("reason") or "").lower()
    return ("break-reachable" in reason or "reached in" in reason
            or "reachable api" in reason)


def invented_citation(pr, repo_root):
    """True if the PR claims reachability but has no real importing file to back it."""
    if not claims_reachability(pr):
        return False, ""
    importers = pr.get("files_importing") or []
    if not importers:
        return True, "claims break-reachability but files_importing is empty"
    missing = [f for f in importers
               if not os.path.exists(os.path.join(repo_root, f.lstrip("./")))]
    if missing:
        return True, f"cites importing files that do not exist: {missing}"
    return False, ""


def _validate_ai(v, repo_root):
    """Same falsifiability contract as validate_adjudication.py: reject invented citations,
    reject FIX/CVE attempts, require citation for a downgrade-to-safe."""
    need = {"reachable", "recommendation", "citation"}
    if not need <= set(v):
        return False, f"missing keys {sorted(need - set(v))}"
    if v["recommendation"] not in ("safe", "review"):
        return False, "AI can only say safe|review (never fix/clear-CVE)"
    cite = (v.get("citation") or "").strip()
    if cite:
        path = cite.split(":")[0].lstrip("./")
        if not os.path.exists(os.path.join(repo_root, path)):
            return False, f"INVENTED CITATION {cite}"
    if v["recommendation"] == "safe" and not (v["reachable"] is False and cite):
        return False, "downgrade to safe needs reachable=false WITH real citation"
    return True, ""


def main():
    if len(sys.argv) < 3:
        print("Usage: run_gate.py <build-results.json> <corpus.json> [--repo R] [--golden G]")
        return 2
    results_path, corpus_path = sys.argv[1], sys.argv[2]
    repo_root = "."
    golden_path = None
    ai_path = None
    args = sys.argv[3:]
    for i, a in enumerate(args):
        if a == "--repo" and i + 1 < len(args):
            repo_root = args[i + 1]
        if a == "--golden" and i + 1 < len(args):
            golden_path = args[i + 1]
        if a == "--ai" and i + 1 < len(args):
            ai_path = args[i + 1]

    results = json.load(open(results_path))
    corpus = json.load(open(corpus_path))
    prs = results.get("prs", {})
    if isinstance(prs, list):
        prs = {str(p.get("pr_id") or p.get("number")): p for p in prs}

    cases = [CorpusCase(c) for c in corpus["cases"]]

    # 1. deterministic predictions for corpus PRs (AI-off baseline)
    predictions = {}
    for c in cases:
        pid = str(c.pr_id)
        if pid in prs:
            predictions[pid] = derive_prediction(prs[pid])
        # else -> Scorer defaults to abstain (counts as false_none for review/fix)

    base_res = Scorer(cases).score(dict(predictions))
    base_ac = base_res["metrics"]["auto_clear_pct"]
    base_fb = base_res["errors"]["false_block_count"]

    # 1b. apply VALIDATED AI verdicts on top (the differentiator, measurable).
    #     AI may only downgrade REVIEW->auto_clear with a real citation; never touch FIX/CVE.
    ai_applied, ai_proof_added, ai_rejected = [], [], []
    if ai_path and os.path.exists(ai_path):
        ai = json.load(open(ai_path))
        for pid, v in ai.items():
            if predictions.get(pid) != "review":
                continue  # AI only adjudicates the REVIEW bucket
            ok, why = _validate_ai(v, repo_root)
            if not ok:
                ai_rejected.append((pid, why))
                continue
            if v.get("recommendation") == "safe":
                predictions[pid] = "auto_clear"
                ai_applied.append((pid, v.get("citation", "")))
            else:  # review, but now PROOF-backed (citation) instead of generic caution
                ai_proof_added.append((pid, v.get("citation", "")))

    score_res = Scorer(cases).score(predictions)

    # 2. invented-citation guard (over ALL prs, not just corpus)
    invented = []
    for pid, pr in prs.items():
        bad, why = invented_citation(pr, repo_root)
        if bad:
            invented.append((pid, pr.get("package", "?"), why))

    # 3. golden regression (optional)
    golden_regressions = []
    if golden_path and os.path.exists(golden_path):
        golden = json.load(open(golden_path))
        for pid, want in golden.items():
            got = predictions.get(pid)
            if got and got != want:
                golden_regressions.append((pid, want, got))

    fg = score_res["errors"]["false_green_count"]
    fb = score_res["errors"]["false_block_count"]
    fn = score_res["errors"]["false_none_count"]
    ac = score_res["metrics"]["auto_clear_pct"]

    zero_fg = fg == 0
    zero_invented = len(invented) == 0
    no_golden_reg = len(golden_regressions) == 0
    accepted = zero_fg and zero_invented and no_golden_reg

    # composite 0-10 score: start 10, subtract heavy for hard fails, light for noise
    score = 10.0
    score -= fg * 4.0            # false-green is catastrophic
    score -= len(invented) * 3.0  # fabricated evidence destroys trust
    score -= len(golden_regressions) * 2.0
    score -= fb * 1.0            # over-flagging (noise)
    score -= fn * 1.0
    score = max(0.0, round(score, 1))

    print(f"SCORE: {score}")
    print(f"ACCEPTED: {accepted}")
    print(f"FALSE_GREEN: {fg}")
    print(f"FALSE_BLOCK: {fb}")
    print(f"FALSE_NONE: {fn}")
    print(f"INVENTED_CITATIONS: {len(invented)}")
    print(f"GOLDEN_REGRESSIONS: {len(golden_regressions)}")
    print(f"AUTO_CLEAR_PCT: {ac:.1f}")
    if ai_path:
        print(f"AI_OFF_AUTO_CLEAR_PCT: {base_ac:.1f}")
        print(f"AI_ON_AUTO_CLEAR_PCT: {ac:.1f}")
        print(f"AI_OFF_FALSE_BLOCK: {base_fb}")
        print(f"AI_ON_FALSE_BLOCK: {fb}")
        print(f"AI_DOWNGRADES_APPLIED: {len(ai_applied)}")
        print(f"AI_PROOF_ADDED: {len(ai_proof_added)}")
        print(f"AI_REJECTED: {len(ai_rejected)}")
    print("FINDINGS:")
    sev = {"false_green": "P0", "false_none": "P1", "false_block": "P2"}
    for c in score_res["per_case"]:
        if c["error"]:
            p = sev.get(c["error"], "P2")
            print(f"- [{p}] PR#{c['pr_id']} | {c['error']} | expected={c['expected']} predicted={c['predicted']}")
    for pid, pkg, why in invented:
        print(f"- [P0] PR#{pid} {pkg} | INVENTED CITATION | {why}")
    for pid, want, got in golden_regressions:
        print(f"- [P1] PR#{pid} | GOLDEN REGRESSION want={want} got={got}")
    print("END_FINDINGS")

    json.dump({"score": score, "accepted": accepted, "metrics": score_res["metrics"],
               "errors": score_res["errors"], "invented": invented,
               "golden_regressions": golden_regressions, "predictions": predictions},
              open("gate-result.json", "w"), indent=2)
    return 0 if accepted else 1


def _err_of(score_res, pid):
    for c in score_res["per_case"]:
        if c["pr_id"] == pid:
            return c["error"]
    return None


if __name__ == "__main__":
    sys.exit(main())
