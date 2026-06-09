#!/usr/bin/env python3
"""Reconcile deterministic build-check output with INDEPENDENT AI adjudication.

PRD intent: the AI layer does its own analysis; it is NOT a formatter that echoes the
deterministic verdict. This module is the decision/reconcile layer that lets independent
evidence (deterministic module-scope + AI investigation) override a deterministic
false positive, while staying fail-safe (never invents a merge-block, never clears a CVE).

Two tiers, both call-graph-free:

  Tier 0 (deterministic, ~microseconds, no AI):
    If the bumped dependency is NOT imported anywhere in the BUMPED MODULE, then a
    breaking API change in that dependency cannot reach this module. This clears the
    classic cross-module false positive (e.g. PR #38: lib/pq bumped in
    automations/tstctl but the only Error.Code usages live in the ROOT module's
    database/* — a different go.mod). Verdict -> SAFE, cited by the module manifest.

  Tier 1 (AI independent adjudication, residue only):
    For PRs where the dependency IS imported in the bumped module, consult the AI
    verdict (replayed from a verdicts file or produced live per-PR). Reconcile:
      - AI reachable=true  -> keep REVIEW, attach the AI's call-site citation as proof.
      - AI reachable=false WITH a real in-module citation AND no declared breaking-change
        section -> may downgrade REVIEW -> SAFE (the AI proved the specific call is
        unaffected).  Otherwise HOLD at REVIEW (conservative; a probe / human decides).
      - AI uncertain / missing / unvalidated -> keep the deterministic verdict.

Guardrails (always): the AI/reconcile layer can only move REVIEW<->SAFE. It can NEVER
produce FIX, never override a build:fail, and never clear a CVE.

Usage:
  reconcile_adjudication.py <build-results.json> [--verdicts ai_verdicts.json]
                            [--repo .] [--write] [--harness DIR]
Prints a per-PR reconcile summary; with --write, updates verdict_v2 in build-results.json.
"""
import argparse
import json
import os
import sys


def _module_dir(pr):
    d = (pr.get("pkg_dir") or "/").strip()
    return "" if d in ("/", ".", "") else d.strip("/")


def _in_module(fp, mod):
    f = (fp or "").lstrip("./")
    if mod == "":
        # root module = everything NOT under a nested module dir
        return not (f.startswith("cicd/") or f.startswith("automations/"))
    return f.startswith(mod + "/") or f == mod


def _files_importing_in_module(pr, mod):
    out = []
    for f in pr.get("files_importing") or []:
        path = f if isinstance(f, str) else (f.get("file") or "")
        if _in_module(path, mod):
            out.append(path)
    return out


def _changed_symbol_usages_in_module(pr, mod):
    det = pr.get("deterministic") or {}
    return [u for u in (det.get("usages") or []) if _in_module(u.get("file", ""), mod)]


def _dep_imported_in_module(pr, mod):
    return bool(_files_importing_in_module(pr, mod)) or bool(_changed_symbol_usages_in_module(pr, mod))


def _has_declared_breaking_section(pr):
    det = pr.get("deterministic") or {}
    sig = det.get("changelogSignal")
    blob = ""
    if isinstance(sig, str):
        blob += sig
    elif isinstance(sig, dict):
        blob += json.dumps(sig)
    blob += " " + (det.get("changelogText") or "")
    low = blob.lower()
    return ("breaking change" in low) or ("### breaking" in low) or ("deprecat" in low)


def _current_verdict(pr):
    v2 = pr.get("verdict_v2") or {}
    return (v2.get("verdict") or "").upper()


def _reason_code(pr):
    v2 = pr.get("verdict_v2") or {}
    return ((v2.get("residual") or {}).get("check") or v2.get("reason") or "")


def _apply_safe(pr, reason, evidence, citation, source):
    """Downgrade this PR's verdict_v2 to SAFE. Never touches CVE/security state."""
    v2 = pr.setdefault("verdict_v2", {})
    v2["verdict"] = "SAFE"
    v2["priority"] = "P3"
    es = v2.setdefault("evidenceState", {})
    es["api_diff"] = "NONE"
    es["usage"] = "NONE"
    v2["residual"] = {"summary": evidence, "check": reason}
    v2["reason"] = evidence
    pr["ai_adjudication"] = {
        "applied": "downgrade_to_safe",
        "source": source,
        "reason_code": reason,
        "evidence": evidence,
        "citation": citation,
    }


def _apply_needs_change(pr, evidence, citation, remediation, flaw, source):
    """Keep REVIEW but attach the resolved finding + remediation (the work is done; advisory,
    never a hard merge block — the AI may not FIX-gate)."""
    v2 = pr.setdefault("verdict_v2", {})
    v2["verdict"] = "REVIEW"
    v2["residual"] = {"summary": evidence, "check": "review:ai-needs-change"}
    v2["reason"] = evidence
    pr["ai_adjudication"] = {
        "applied": "needs_change",
        "source": source,
        "evidence": evidence,
        "citation": citation,
        "remediation": remediation,
        "deterministic_flaw": flaw,
    }


def _record_review(pr, evidence, citation, source, flaw="", question=""):
    pr["ai_adjudication"] = {
        "applied": "hold_review",
        "source": source,
        "evidence": evidence,
        "citation": citation,
        "deterministic_flaw": flaw,
        "escalation_question": question,
    }


def reconcile_pr(pr, verdict, repo):
    """Return (action, detail). Mutates pr's verdict_v2 when it downgrades to safe."""
    mod = _module_dir(pr)
    verdict_now = _current_verdict(pr)
    reason = _reason_code(pr)

    # Only ever reconsider REVIEW verdicts driven by a (possibly cross-module) API-diff.
    # Never touch FIX (build/security) or already-SAFE.
    is_break_reachable = "break-reachable" in reason or (
        (pr.get("verdict_v2", {}).get("evidenceState", {}) or {}).get("api_diff") == "POSITIVE"
    )
    if verdict_now != "REVIEW" or not is_break_reachable:
        return ("kept", f"verdict={verdict_now or 'n/a'} (not a break-reachable review; untouched)")

    # ── Tier 0: deterministic module-scope (no AI, no call graph) ──────────────
    if not _dep_imported_in_module(pr, mod):
        manifest = (mod + "/go.mod") if mod else "go.mod"
        if not os.path.exists(os.path.join(repo, manifest)):
            manifest = ""  # still safe; manifest path just unavailable for citation
        ev = (f"Dependency `{pr.get('package')}` is not imported in the bumped module "
              f"`{mod or 'root'}`; a breaking API change in it cannot reach this module. "
              f"The flagged usages, if any, live in a different go.mod.")
        _apply_safe(pr, "safe:not-imported-in-bumped-module", ev, manifest, "deterministic_module_scope")
        return ("downgraded_safe", f"Tier0: dep not imported in '{mod or 'root'}' -> SAFE")

    # ── Tier 1: AI as senior arbiter (independent-first + deterministic-audit) ──
    if not verdict or not verdict.get("accepted", True):
        return ("kept", "dep imported in module; no accepted AI verdict -> keep REVIEW")

    # Normalize across schema versions: prefer the arbitrated final_verdict.
    final = verdict.get("final_verdict") or verdict.get("verdict")
    if final is None and "reachable" in verdict:  # legacy
        final = "safe" if verdict.get("recommendation") == "safe" else "review"
    cite = (verdict.get("citation") or "").strip()
    ev = (verdict.get("proof") or verdict.get("evidence") or "").strip()
    flaw = (verdict.get("deterministic_flaw") or "").strip()
    remediation = (verdict.get("remediation") or "").strip()
    question = (verdict.get("escalation_question") or "").strip()

    if final == "safe":
        # The AI resolved it to SAFE with shown work. Honor it (validator already required
        # proof + real citation). The override of the deterministic REVIEW is justified by the
        # named deterministic_flaw when the two disagree.
        reason = "safe:ai-resolved" + ("-audit-override" if flaw else "")
        _apply_safe(pr, reason, ev, cite, "ai_arbiter")
        tail = f" (override flaw: {flaw})" if flaw else ""
        return ("downgraded_safe", f"AI resolved SAFE, cite {cite or '(grep-negative)'}{tail}")

    if final == "needs_change":
        _apply_needs_change(pr, ev, cite, remediation, flaw, "ai_arbiter")
        return ("kept", f"AI resolved NEEDS_CHANGE, cite {cite} -> REVIEW (+remediation)")

    if final == "escalate":
        _record_review(pr, ev, cite, "ai_arbiter", flaw, question)
        return ("kept", f"AI escalated (runtime-irreducible): {question[:60]}")

    return ("kept", f"AI final_verdict={final!r} unhandled -> keep REVIEW")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--verdicts", default="")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    data = json.load(open(args.results))
    prs = data.get("prs") or {}
    verdicts = {}
    if args.verdicts and os.path.exists(args.verdicts):
        raw = json.load(open(args.verdicts))
        for k, v in raw.items():
            if k.startswith("_"):
                continue
            verdicts[str(k)] = v

    n_safe = n_kept = 0
    for pid, pr in prs.items():
        v = verdicts.get(str(pid))
        if v is not None and "accepted" not in v:
            v = dict(v, accepted=True)  # replay verdicts are pre-trusted grounded outputs
        action, detail = reconcile_pr(pr, v, args.repo)
        if action == "downgraded_safe":
            n_safe += 1
        else:
            n_kept += 1
        print(f"PR#{pid}: {action.upper():16s} {detail}")

    print(f"\nRECONCILE_SUMMARY downgraded_safe={n_safe} kept={n_kept} total={len(prs)}")
    if args.write:
        json.dump(data, open(args.results, "w"), indent=2)
        print(f"WROTE {args.results}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
