#!/usr/bin/env python3
"""Lightweight deterministic reachability analyzer for the breakability pipeline.

CALLGRAPH POLICY
================
This tool embodies a three-tier analysis strategy for the breakability pipeline:

1. LITE (this tool)
   - Fast, deterministic, no source parsing
   - Consumes pre-computed fields from build-results: declared_break_reachability,
     deterministic.usages, files_importing
   - Answers: "Does the PR record contain enough evidence (surface-level) to
     confirm or rule out production reachability?"
   - Typical use: all PRs, default verdict strategy
   - Scalable: O(PR record size), not O(codebase)

2. DEEP (deep.go)
   - Full-repo static callgraph analysis via golang.org/x/tools
   - Mirrors govulncheck's proven pipeline: SSA → CHA → VTA → forward-reachable
   - Answers: "From this repo's entrypoints, is the changed symbol transitively
     reachable in the static call graph?"
   - Use when: LITE yields UNCERTAIN and whole-repo analysis justified (targeted
     high-risk PRs, policy validation, deep review)
   - Cost: 30-300s per repo (compute-heavy), not typical dev workflow
   - CRITICAL: Never asserts SAFE. Unreachable downgraded to POTENTIALLY_REACHABLE
     when dynamic constructs (reflect/unsafe/cgo/plugin) exist.

3. DYNAMIC PROBES (future/targeted)
   - Runtime instrumentation, targeted to unresolved callsites from LITE+DEEP
   - Probes specific high-risk function entry points
   - Answers: "At runtime, does this function get called with a changed symbol?"
   - Use when: static analysis insufficient, execution path required

ABSENT IS STRICT OPT-IN
-----------------------
The ABSENT verdict is deliberately conservative. It is only returned when:
  - The deterministic layer explicitly confirms: checked=True, kind='not_imported'
  - AND no production imports exist in the PR record
  - A "missing callsite" alone never triggers ABSENT; absence of evidence is NOT
    evidence of absence. The merge gate can depend on ABSENT; false positives would
    be catastrophic for security.

Three verdicts
--------------
PRESENT   — at least one production callsite for a named changed symbol was located
            deterministically from surface_evidence or deterministic.usages.

ABSENT    — the package is provably not imported by this repository. Only declared when
            the deterministic layer set checked=True, reachability_kind='not_imported'
            AND no files_importing / evidence entries exist. Never inferred from a
            missing callsite alone — absence of evidence ≠ evidence of absence.

UNCERTAIN — everything else: imports exist but no named callsite for the specific
            changed symbols; dynamic hazards (reflect/unsafe/plugin/go:generate/
            go:linkname) alongside import evidence; or reachability data incomplete.

Evidence contract (returned dict)
----------------------------------
  verdict           PRESENT | ABSENT | UNCERTAIN
  confidence        high | medium | low
  callsites         list[{file, line, symbol, is_test}] — located callsites
  import_sites      list[{file, line, is_test}] — import-level evidence
  dynamic_hazards   list[str] — reasons static absence is unsound
  absent_reason     str | None
  uncertain_reason  str | None
  searched_symbols  list[str] — the changed symbols we searched for
  checked           bool — whether a deterministic reachability check was present
  sources_used      list[str] — which PR fields contributed evidence

Limitations
-----------
- Does NOT parse Go source files; relies on pre-computed fields in build-results.
- ABSENT is only asserted when declared_break_reachability.checked == True and
  reachability_kind == 'not_imported'; a silent absence (unchecked) stays UNCERTAIN.
- Dynamic hazard detection from snippets is best-effort (text scan, not AST).
- Symbol matching is by leaf name only; shadowing / aliased imports are not resolved.
- Indirect (transitive) reachability is not computed here; use deep.go for that.

How it feeds the evidence contract
-----------------------------------
The output dict is designed to be merged into declared_break_reachability or stored
as a new field `lite_reachability` alongside it. Downstream consumers (differential-
probe, behavioral-probe) should:
  - Treat PRESENT as confirming prod_reachable=True, supplying the callsite.
  - Treat ABSENT as confirming checked=True / reachability_kind='not_imported'.
  - Treat UNCERTAIN as "cannot lower verdict below Medium; supply uncertain_reason".
"""
from __future__ import annotations

import json
import sys
from typing import Any

# ---------------------------------------------------------------------------
# Dynamic-dispatch markers that make "not found → ABSENT" unsound
# ---------------------------------------------------------------------------

_GO_DYNAMIC_MARKERS: dict[str, list[str]] = {
    "reflect":     ['"reflect"'],
    "unsafe":      ['"unsafe"'],
    "cgo":         ['"C"', 'import "C"'],
    "plugin":      ['"plugin"'],
    "go:generate": ["//go:generate"],
    "go:linkname": ["//go:linkname"],
}

_PY_DYNAMIC_MARKERS: dict[str, list[str]] = {
    "importlib":     ["importlib.import_module", "__import__"],
    "getattr_module": ["getattr("],
    "eval":           ["eval("],
}

_JS_DYNAMIC_MARKERS: dict[str, list[str]] = {
    "dynamic_require": ["require("],
    "dynamic_import":  ["import("],
    "eval":            ["eval("],
}

_MARKERS_BY_ECOSYSTEM: dict[str, dict[str, list[str]]] = {
    "go":         _GO_DYNAMIC_MARKERS,
    "python":     _PY_DYNAMIC_MARKERS,
    "pip":        _PY_DYNAMIC_MARKERS,
    "npm":        _JS_DYNAMIC_MARKERS,
    "javascript": _JS_DYNAMIC_MARKERS,
    "typescript": _JS_DYNAMIC_MARKERS,
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_int(v: Any) -> int | None:
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _is_test_file(path: str) -> bool:
    if not path:
        return False
    name = path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    return (
        name.endswith("_test.go")
        or name.startswith("test_")
        or name.endswith("_test.py")
        or "/test/" in path
        or "/tests/" in path
        or "/__tests__/" in path
        or ".test.ts" in path
        or ".spec.ts" in path
        or ".test.js" in path
        or ".spec.js" in path
    )


def _scan_snippets_for_hazards(snippets: list[str], ecosystem: str) -> list[str]:
    """Text-scan source snippets for dynamic-dispatch markers (best-effort)."""
    markers = _MARKERS_BY_ECOSYSTEM.get(ecosystem, {})
    found: set[str] = set()
    for snippet in snippets:
        for reason, needles in markers.items():
            for needle in needles:
                if needle in snippet:
                    found.add(reason)
    return sorted(found)


def _extract_import_sites(pr: dict) -> list[dict]:
    """Collect import-level evidence from files_importing and dbr.evidence."""
    sites: list[dict] = []
    seen: set[tuple] = set()

    def _add(file: str, line: Any, is_test: bool) -> None:
        key = (file, _safe_int(line))
        if key not in seen:
            seen.add(key)
            sites.append({"file": file, "line": _safe_int(line), "is_test": is_test})

    # files_importing: ["path/file.go:42", "path/file.go", ...]
    for entry in pr.get("files_importing") or []:
        if not isinstance(entry, str) or not entry:
            continue
        parts = entry.rsplit(":", 1)
        file = parts[0]
        line = parts[1] if len(parts) == 2 and parts[1].isdigit() else None
        _add(file, line, _is_test_file(file))

    # declared_break_reachability.evidence
    dbr = pr.get("declared_break_reachability") or {}
    for e in dbr.get("evidence") or []:
        if not isinstance(e, dict):
            continue
        file = e.get("file") or ""
        if not file:
            continue
        is_test = bool(e.get("is_test", _is_test_file(file)))
        _add(file, e.get("line"), is_test)

    return sites


def _extract_callsites(pr: dict, sym_set: set[str]) -> list[dict]:
    """Collect symbol-level callsite evidence from surface_evidence and usages."""
    callsites: list[dict] = []
    seen: set[tuple] = set()

    def _add(file: str, line: Any, symbol: str, is_test: bool) -> None:
        key = (file, _safe_int(line), symbol)
        if key not in seen:
            seen.add(key)
            callsites.append(
                {"file": file, "line": _safe_int(line), "symbol": symbol, "is_test": is_test}
            )

    # declared_break_reachability.surface_evidence — direct call-site records
    dbr = pr.get("declared_break_reachability") or {}
    for e in dbr.get("surface_evidence") or []:
        if not isinstance(e, dict):
            continue
        file = e.get("file") or ""
        if not file:
            continue
        sym = e.get("symbol") or ""
        is_test = bool(e.get("is_test", _is_test_file(file)))
        named = bool(e.get("named", False))
        # Accept if: no symbol filter, OR symbol matches, OR entry is explicitly "named"
        if sym_set and sym and sym not in sym_set and not named:
            continue
        _add(file, e.get("line"), sym, is_test)

    # deterministic.usages — broader usage scan from the TS pipeline
    det = pr.get("deterministic") or {}
    for u in det.get("usages") or []:
        if not isinstance(u, dict):
            continue
        file = u.get("file") or ""
        if not file:
            continue
        sym = u.get("symbol") or ""
        ctx = str(u.get("context", "")).lower()
        is_test = ctx == "test" or _is_test_file(file)
        if sym_set and sym and sym not in sym_set:
            continue
        _add(file, u.get("line"), sym, is_test)

    return callsites


def _collect_snippets(pr: dict) -> list[str]:
    dbr = pr.get("declared_break_reachability") or {}
    snippets: list[str] = []
    for e in dbr.get("surface_evidence") or []:
        if isinstance(e, dict) and e.get("snippet"):
            snippets.append(str(e["snippet"]))
    det = pr.get("deterministic") or {}
    for u in det.get("usages") or []:
        if isinstance(u, dict) and u.get("snippet"):
            snippets.append(str(u["snippet"]))
    return snippets


def _collect_dynamic_hazards(pr: dict, ecosystem: str) -> list[str]:
    """Merge pre-computed dynamic reasons from deep.go output with snippet-scan results."""
    hazards: set[str] = set()

    # Carry through hazards already computed by deep.go (stored in the PR record
    # as dynamic_reasons when the callgraph tool has been run on this repo).
    if isinstance(pr.get("dynamic_reasons"), list):
        for r in pr["dynamic_reasons"]:
            if isinstance(r, str) and r:
                hazards.add(r)
    elif pr.get("dynamic_present") is True:
        hazards.add("unknown_dynamic")

    # Best-effort text scan of embedded source snippets.
    snippets = _collect_snippets(pr)
    if snippets:
        for h in _scan_snippets_for_hazards(snippets, ecosystem):
            hazards.add(h)

    return sorted(hazards)


def _sources_used(pr: dict) -> list[str]:
    sources: list[str] = []
    if pr.get("files_importing"):
        sources.append("files_importing")
    dbr = pr.get("declared_break_reachability") or {}
    if dbr.get("evidence"):
        sources.append("declared_break_reachability.evidence")
    if dbr.get("surface_evidence"):
        sources.append("declared_break_reachability.surface_evidence")
    det = pr.get("deterministic") or {}
    if det.get("usages"):
        sources.append("deterministic.usages")
    return sources


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze(pr: dict, changed_symbols: list[str] | None = None) -> dict:
    """Analyze a single PR record and return reachability evidence.

    Args:
        pr: A single PR record from build-results JSON (the value under prs[N]).
        changed_symbols: Optional list of changed exported symbol leaf names,
            e.g. ["NewClient", "WithTimeout"]. When omitted any callsite evidence
            for the affected package is accepted.

    Returns:
        Evidence dict — see module docstring for the full schema.
    """
    ecosystem = str(pr.get("ecosystem") or "go").lower()
    sym_set: set[str] = set(changed_symbols) if changed_symbols else set()

    import_sites = _extract_import_sites(pr)
    callsites = _extract_callsites(pr, sym_set)
    prod_imports = [s for s in import_sites if not s["is_test"]]
    prod_callsites = [c for c in callsites if not c["is_test"]]

    dynamic_hazards = _collect_dynamic_hazards(pr, ecosystem)
    sources = _sources_used(pr)

    dbr = pr.get("declared_break_reachability") or {}
    checked = bool(dbr.get("checked", False))

    # ── ABSENT ────────────────────────────────────────────────────────────────
    # Only assert ABSENT when the deterministic layer explicitly confirmed it.
    if (
        checked
        and dbr.get("reachability_kind") == "not_imported"
        and dbr.get("prod_reachable") is not True
        and not prod_imports
    ):
        return {
            "verdict": "ABSENT",
            "confidence": "high",
            "callsites": [],
            "import_sites": import_sites,   # may contain test-only imports
            "dynamic_hazards": dynamic_hazards,
            "absent_reason": "deterministic_layer_confirmed_not_imported",
            "uncertain_reason": None,
            "searched_symbols": sorted(sym_set),
            "checked": checked,
            "sources_used": sources,
        }

    # ── PRESENT ───────────────────────────────────────────────────────────────
    if prod_callsites:
        if sym_set:
            named_hits = [c for c in prod_callsites if c["symbol"] in sym_set]
            if named_hits:
                confidence = "medium" if dynamic_hazards else "high"
                return {
                    "verdict": "PRESENT",
                    "confidence": confidence,
                    "callsites": prod_callsites,
                    "import_sites": import_sites,
                    "dynamic_hazards": dynamic_hazards,
                    "absent_reason": None,
                    "uncertain_reason": None,
                    "searched_symbols": sorted(sym_set),
                    "checked": checked,
                    "sources_used": sources,
                }
        else:
            # No symbol filter: any production callsite is sufficient.
            confidence = "medium" if dynamic_hazards else "high"
            return {
                "verdict": "PRESENT",
                "confidence": confidence,
                "callsites": prod_callsites,
                "import_sites": import_sites,
                "dynamic_hazards": dynamic_hazards,
                "absent_reason": None,
                "uncertain_reason": None,
                "searched_symbols": sorted(sym_set),
                "checked": checked,
                "sources_used": sources,
            }

    # ── UNCERTAIN ─────────────────────────────────────────────────────────────
    if not prod_imports:
        if checked:
            # checked=True but kind is not 'not_imported' — ambiguous declaration
            uncertain_reason = "checked_but_reachability_kind_ambiguous"
        else:
            uncertain_reason = "no_production_import_evidence_unchecked"
        confidence = "low"
    elif sym_set and not prod_callsites:
        if dynamic_hazards:
            uncertain_reason = (
                "imports_present_dynamic_hazards:" + ",".join(dynamic_hazards)
            )
            confidence = "low"
        else:
            uncertain_reason = "imports_present_no_named_callsite_for_changed_symbols"
            confidence = "medium"
    elif dynamic_hazards:
        uncertain_reason = (
            "imports_present_dynamic_hazards:" + ",".join(dynamic_hazards)
        )
        confidence = "low"
    else:
        uncertain_reason = "insufficient_callsite_data"
        confidence = "low"

    return {
        "verdict": "UNCERTAIN",
        "confidence": confidence,
        "callsites": callsites,  # may contain test callsites as partial evidence
        "import_sites": import_sites,
        "dynamic_hazards": dynamic_hazards,
        "absent_reason": None,
        "uncertain_reason": uncertain_reason,
        "searched_symbols": sorted(sym_set),
        "checked": checked,
        "sources_used": sources,
    }


def analyze_build_results(
    results: dict,
    changed_symbols_by_pr: dict[str, list[str]] | None = None,
) -> dict[str, dict]:
    """Analyze every PR in a build-results JSON file.

    Args:
        results: Full build-results JSON dict (top level, with "prs" key).
        changed_symbols_by_pr: Optional {pr_number_str: [symbol, ...]} mapping.

    Returns:
        {pr_number_str: evidence_dict}
    """
    prs = results.get("prs") or {}
    out: dict[str, dict] = {}
    for pr_num, pr_record in prs.items():
        if not isinstance(pr_record, dict):
            continue
        symbols = (changed_symbols_by_pr or {}).get(str(pr_num))
        out[str(pr_num)] = analyze(pr_record, symbols)
    return out


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _cli() -> int:
    """Minimal CLI: reads a build-results JSON from stdin or a file argument,
    optionally filtered to a single PR + changed symbols, emits JSON to stdout.

    This is the FIRST-TIER reachability check for the breakability pipeline
    (see module docstring: CALLGRAPH POLICY). It performs lightweight deterministic
    analysis without source parsing. If the verdict is UNCERTAIN and deeper
    analysis is needed, consider running deep.go (the full-repo callgraph tool).

    Usage:
      python3 lite.py [build-results.json] [--pr N] [--symbols Foo,Bar]

    Returns JSON dict with verdict (PRESENT|ABSENT|UNCERTAIN), confidence
    (high|medium|low), and detailed evidence chain.
    """
    import argparse

    p = argparse.ArgumentParser(
        description="Lightweight deterministic reachability analyzer"
    )
    p.add_argument(
        "results_file",
        nargs="?",
        default="-",
        help="Path to build-results JSON, or '-' for stdin (default: stdin)",
    )
    p.add_argument("--pr", default=None, help="Analyze only this PR number")
    p.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated list of changed symbol names",
    )
    args = p.parse_args()

    if args.results_file == "-":
        raw = sys.stdin.read()
    else:
        with open(args.results_file) as f:
            raw = f.read()

    data = json.loads(raw)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None

    if args.pr:
        prs = data.get("prs") or {}
        pr_record = prs.get(str(args.pr)) or prs.get(args.pr)
        if pr_record is None:
            print(json.dumps({"error": f"PR {args.pr!r} not found"}), file=sys.stderr)
            return 1
        result = {str(args.pr): analyze(pr_record, symbols)}
    else:
        sym_map = {str(args.pr): symbols} if args.pr and symbols else None
        result = analyze_build_results(data, sym_map)

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
