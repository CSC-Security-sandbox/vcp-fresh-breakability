#!/usr/bin/env python3
"""Tests for the lightweight deterministic reachability analyzer (lite.py).

Run: python3 .github/tools/reachability/test_lite.py
Exits non-zero on any failure.

No pytest dependency — uses only stdlib assertions so it runs in any CI environment.

CALLGRAPH POLICY VALIDATION
----------------------------
These tests validate the three-tier reachability strategy:
  1. LITE (lite.py):    Fast, deterministic, no parsing → all PRs, default
  2. DEEP (deep.go):    Full-repo callgraph → UNCERTAIN PRs, high-risk, policy
  3. DYNAMIC (future):  Runtime probes → unresolved high-risk callsites

lite.py is the FIRST TIER: lightweight deterministic analysis. When verdict is
UNCERTAIN, DEEP can be invoked (expensive, targeted). ABSENT is deliberately
conservative—only returned when deterministic layer explicitly confirms it
(checked=True, kind='not_imported', no prod imports). Never inferred from
missing evidence alone.

Test matrix
-----------
1.  PRESENT   — surface_evidence has a named production callsite for a changed symbol.
2.  PRESENT   — deterministic.usages has a production-context entry for the symbol.
3.  PRESENT   — no symbol filter; any production callsite qualifies.
4.  PRESENT   — confidence degrades to 'medium' when dynamic_hazards are present.
5.  ABSENT    — checked=True, kind='not_imported', no imports, no prod_reachable.
6.  ABSENT    — test-only imports do not block ABSENT verdict.
7.  NOT ABSENT — checked=True but kind != 'not_imported' → UNCERTAIN, not ABSENT.
8.  NOT ABSENT — unchecked, no imports → UNCERTAIN (not ABSENT; cannot assert).
9.  UNCERTAIN  — imports present but no named callsite for specific changed symbols.
10. UNCERTAIN  — imports + dynamic hazards from pre-computed dynamic_reasons.
11. UNCERTAIN  — imports + dynamic hazards detected from embedded snippet text.
12. UNCERTAIN  — no callsite data at all; imports only via files_importing.
13. UNCERTAIN  — checked=True but reachability_kind is ambiguous (not 'not_imported').
14. PRESENT    — symbol in deterministic.usages but NOT in surface_evidence (fallback).
15. PRESENT    — files_importing "file:line" format is parsed correctly.
16. ABSENT     — prod_reachable=True blocks ABSENT even when kind='not_imported'.
17. analyze_build_results — round-trip over a multi-PR results dict.
18. UNCERTAIN  — missing data / empty PR record does not crash.

Policy highlights
-----------------
• ABSENT only when deterministic layer explicitly says "not_imported" (strict opt-in).
• UNCERTAIN with dynamic hazards + imports = cannot trust static analysis alone;
  merge gate should require DEEP or manual review.
• PRESENT with dynamic hazards = medium confidence (presence is high-confidence,
  but dynamic could hide additional paths we didn't find).
• lite.py always returns a verdict; never crashes; always includes uncertain_reason
  or absent_reason to guide downstream decision-making.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lite import analyze, analyze_build_results  # noqa: E402

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _pr(
    *,
    ecosystem: str = "go",
    files_importing: list | None = None,
    dbr: dict | None = None,
    deterministic: dict | None = None,
    dynamic_reasons: list | None = None,
    dynamic_present: bool = False,
) -> dict:
    """Build a minimal PR record."""
    record: dict = {"ecosystem": ecosystem}
    if files_importing is not None:
        record["files_importing"] = files_importing
    if dbr is not None:
        record["declared_break_reachability"] = dbr
    if deterministic is not None:
        record["deterministic"] = deterministic
    if dynamic_reasons is not None:
        record["dynamic_reasons"] = dynamic_reasons
    elif dynamic_present:
        record["dynamic_present"] = True
    return record


def _site(
    file: str,
    line: int | None = 10,
    symbol: str = "NewClient",
    is_test: bool = False,
    named: bool = True,
    snippet: str | None = None,
) -> dict:
    d: dict = {
        "file": file,
        "line": line,
        "symbol": symbol,
        "is_test": is_test,
        "named": named,
    }
    if snippet is not None:
        d["snippet"] = snippet
    return d


def _usage(
    file: str,
    line: int = 5,
    symbol: str = "NewClient",
    context: str = "production",
) -> dict:
    return {"file": file, "line": line, "symbol": symbol, "context": context}


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

CASES: list[tuple[str, dict, list[str] | None, str, str]] = []
# (name, pr_record, changed_symbols, expected_verdict, expected_confidence)


def _case(name: str, pr: dict, syms: list[str] | None, verdict: str, conf: str):
    CASES.append((name, pr, syms, verdict, conf))


# 1. PRESENT — surface_evidence named production callsite
_case(
    "present_named_surface_evidence",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "import",
            "prod_reachable": True,
            "surface_evidence": [
                _site("core/client/client.go", line=22, symbol="NewClient", is_test=False)
            ],
        }
    ),
    ["NewClient"],
    "PRESENT",
    "high",
)

# 2. PRESENT — deterministic.usages production-context
_case(
    "present_via_deterministic_usages",
    _pr(
        files_importing=["core/client/client.go:1"],
        deterministic={"usages": [_usage("core/client/client.go", 22, "WithTimeout")]},
    ),
    ["WithTimeout"],
    "PRESENT",
    "high",
)

# 3. PRESENT — no symbol filter; any production callsite
_case(
    "present_no_symbol_filter",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "import",
            "prod_reachable": True,
            "surface_evidence": [_site("core/svc/handler.go", symbol="Dial")],
        }
    ),
    None,  # no filter
    "PRESENT",
    "high",
)

# 4. PRESENT — confidence degrades to 'medium' with dynamic_hazards
_case(
    "present_degrades_confidence_with_dynamics",
    _pr(
        dynamic_reasons=["reflect", "unsafe"],
        dbr={
            "checked": True,
            "prod_reachable": True,
            "surface_evidence": [_site("core/svc/handler.go", symbol="NewClient")],
        },
    ),
    ["NewClient"],
    "PRESENT",
    "medium",
)

# 5. ABSENT — fully clean: checked, kind=not_imported, no imports
# Policy: ABSENT is STRICT OPT-IN. Only when:
#   - deterministic layer explicitly checked=True AND kind='not_imported'
#   - AND zero production imports/evidence found
#   - NEVER inferred from missing evidence (absence ≠ evidence of absence)
_case(
    "absent_clean",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "not_imported",
            "prod_reachable": False,
        }
    ),
    ["NewClient", "Dial"],
    "ABSENT",
    "high",
)

# 6. ABSENT — test-only imports do not block ABSENT
_case(
    "absent_test_only_imports_allowed",
    _pr(
        files_importing=["core/client/client_test.go:1"],
        dbr={
            "checked": True,
            "reachability_kind": "not_imported",
            "prod_reachable": False,
            "evidence": [
                {
                    "file": "core/client/client_test.go",
                    "line": 1,
                    "is_test": True,
                }
            ],
        },
    ),
    None,
    "ABSENT",
    "high",
)

# 7. NOT ABSENT (UNCERTAIN) — checked=True but kind != 'not_imported'
# Policy: kind='not_imported' is required for ABSENT. Any other kind (e.g., 'import',
# 'indirect', 'ambiguous') means reachability is not definitively ruled out → UNCERTAIN.
_case(
    "uncertain_checked_kind_not_not_imported",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "import",   # not 'not_imported'
            "prod_reachable": False,
        }
    ),
    ["Foo"],
    "UNCERTAIN",
    "low",
)

# 8. NOT ABSENT (UNCERTAIN) — unchecked, no evidence at all
# Policy: checked=False means deterministic layer didn't run or didn't confirm.
# Absence of evidence is NOT evidence of absence → stay UNCERTAIN. ABSENT only
# when deterministic layer explicitly commits (checked=True, kind='not_imported').
_case(
    "uncertain_unchecked_empty",
    _pr(),
    ["Bar"],
    "UNCERTAIN",
    "low",
)

# 9. UNCERTAIN — imports exist but no named callsite for changed symbols
_case(
    "uncertain_imports_no_named_callsite",
    _pr(
        files_importing=["core/auth/auth.go:3"],
        dbr={
            "checked": True,
            "reachability_kind": "import",
            "prod_reachable": True,
            # surface_evidence only carries an unnamed symbol
            "surface_evidence": [
                _site("core/auth/auth.go", symbol="OldFunc", named=False)
            ],
        },
    ),
    ["NewFunc"],  # different from what surface_evidence has
    "UNCERTAIN",
    "medium",
)

# 10. UNCERTAIN — imports + pre-computed dynamic hazards
_case(
    "uncertain_dynamic_hazards_precomputed",
    _pr(
        files_importing=["core/plugin/plugin.go:5"],
        dynamic_reasons=["reflect", "plugin"],
    ),
    ["Register"],
    "UNCERTAIN",
    "low",
)

# 11. UNCERTAIN — dynamic hazard detected from snippet text
_case(
    "uncertain_dynamic_hazard_from_snippet",
    _pr(
        files_importing=["core/dyn/dyn.go:7"],
        dbr={
            "checked": False,
            "surface_evidence": [
                _site(
                    "core/dyn/dyn.go",
                    symbol="",
                    named=False,
                    snippet='import "reflect"\nval := reflect.ValueOf(x)',
                )
            ],
        },
    ),
    ["MarshalJSON"],
    "UNCERTAIN",
    "low",
)

# 12. UNCERTAIN — files_importing only, no usages data
_case(
    "uncertain_files_importing_only",
    _pr(
        files_importing=["core/metrics/metrics.go:1"],
    ),
    ["NewCounter"],
    "UNCERTAIN",
    "medium",
)

# 13. UNCERTAIN — checked=True but ambiguous kind
_case(
    "uncertain_checked_ambiguous_kind",
    _pr(
        dbr={"checked": True, "reachability_kind": "indirect"},
    ),
    ["Foo"],
    "UNCERTAIN",
    "low",
)

# 14. PRESENT — symbol only in usages, not surface_evidence
_case(
    "present_symbol_only_in_usages",
    _pr(
        deterministic={
            "usages": [
                _usage("core/store/store.go", line=44, symbol="OpenDB", context="production")
            ]
        },
    ),
    ["OpenDB"],
    "PRESENT",
    "high",
)

# 15. PRESENT — files_importing "file:line" parsed correctly, usages match
_case(
    "present_files_importing_colon_format",
    _pr(
        files_importing=["core/http/server.go:12"],
        deterministic={
            "usages": [
                _usage("core/http/server.go", line=12, symbol="ListenAndServe", context="production")
            ]
        },
    ),
    ["ListenAndServe"],
    "PRESENT",
    "high",
)

# 16. NOT ABSENT — prod_reachable=True blocks ABSENT even with kind='not_imported'
_case(
    "not_absent_when_prod_reachable_true",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "not_imported",
            "prod_reachable": True,   # contradictory but prod_reachable wins
        }
    ),
    ["Foo"],
    "UNCERTAIN",
    "low",
)

# 18. UNCERTAIN — empty PR record does not crash
_case(
    "uncertain_empty_pr_record",
    {},
    None,
    "UNCERTAIN",
    "low",
)


# ---------------------------------------------------------------------------
# Extra structural assertions (beyond verdict+confidence)
# ---------------------------------------------------------------------------

STRUCTURAL: list[tuple[str, dict, list[str] | None, str, str]] = []
# (name, pr, syms, field_name, expected_value_repr)


def _struct(name: str, pr: dict, syms: list[str] | None, field: str, expected):
    STRUCTURAL.append((name, pr, syms, field, expected))


# PRESENT result must carry callsites
_struct(
    "present_callsites_non_empty",
    _pr(
        dbr={
            "surface_evidence": [_site("svc/main.go", symbol="Connect")]
        }
    ),
    None,
    "callsites_nonempty",
    True,
)

# ABSENT result must have empty callsites
_struct(
    "absent_callsites_empty",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "not_imported",
            "prod_reachable": False,
        }
    ),
    None,
    "callsites_empty",
    True,
)

# ABSENT must carry absent_reason
_struct(
    "absent_has_absent_reason",
    _pr(
        dbr={
            "checked": True,
            "reachability_kind": "not_imported",
            "prod_reachable": False,
        }
    ),
    None,
    "absent_reason_nonempty",
    True,
)

# UNCERTAIN must carry uncertain_reason
_struct(
    "uncertain_has_uncertain_reason",
    _pr(files_importing=["core/x/x.go:1"]),
    ["Foo"],
    "uncertain_reason_nonempty",
    True,
)

# PRESENT does NOT carry absent_reason or uncertain_reason
_struct(
    "present_has_no_absent_or_uncertain_reason",
    _pr(
        dbr={
            "surface_evidence": [_site("svc/main.go", symbol="Connect")]
        }
    ),
    None,
    "present_no_extra_reasons",
    True,
)

# searched_symbols must be populated when passed
_struct(
    "searched_symbols_returned",
    _pr(
        dbr={
            "surface_evidence": [_site("svc/main.go", symbol="Foo")]
        }
    ),
    ["Foo", "Bar"],
    "searched_symbols",
    ["Bar", "Foo"],  # sorted
)

# dynamic_hazards forwarded when present
_struct(
    "dynamic_hazards_forwarded",
    _pr(
        files_importing=["core/x/x.go:1"],
        dynamic_reasons=["reflect", "unsafe"],
    ),
    ["Foo"],
    "dynamic_hazards",
    ["reflect", "unsafe"],
)


# ---------------------------------------------------------------------------
# analyze_build_results round-trip test (test 17)
# ---------------------------------------------------------------------------

def _test_analyze_build_results():
    results = {
        "prs": {
            "42": _pr(
                dbr={
                    "checked": True,
                    "reachability_kind": "not_imported",
                    "prod_reachable": False,
                }
            ),
            "99": _pr(
                dbr={
                    "surface_evidence": [_site("svc/main.go", symbol="Foo")]
                }
            ),
        }
    }
    out = analyze_build_results(results, {"42": ["Foo"], "99": ["Foo"]})
    assert "42" in out and "99" in out, "both PRs present in output"
    assert out["42"]["verdict"] == "ABSENT", f"PR 42 expected ABSENT, got {out['42']['verdict']}"
    assert out["99"]["verdict"] == "PRESENT", f"PR 99 expected PRESENT, got {out['99']['verdict']}"
    return True


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _run_field_check(name: str, result: dict, field: str, expected) -> bool:
    if field == "callsites_nonempty":
        return bool(result.get("callsites"))
    if field == "callsites_empty":
        return result.get("callsites") == []
    if field == "absent_reason_nonempty":
        return bool(result.get("absent_reason"))
    if field == "uncertain_reason_nonempty":
        return bool(result.get("uncertain_reason"))
    if field == "present_no_extra_reasons":
        return result.get("absent_reason") is None and result.get("uncertain_reason") is None
    return result.get(field) == expected


def main() -> int:
    fails = 0
    total = 0

    # Verdict + confidence cases
    for name, pr, syms, exp_verdict, exp_conf in CASES:
        total += 1
        result = analyze(pr, syms)
        v_ok = result["verdict"] == exp_verdict
        c_ok = result["confidence"] == exp_conf
        if not (v_ok and c_ok):
            fails += 1
            print(
                f"FAIL [{name}]:\n"
                f"  verdict  got={result['verdict']!r} exp={exp_verdict!r}\n"
                f"  confidence got={result['confidence']!r} exp={exp_conf!r}\n"
                f"  uncertain_reason={result.get('uncertain_reason')}\n"
                f"  absent_reason={result.get('absent_reason')}"
            )

    # Structural assertions
    for name, pr, syms, field, expected in STRUCTURAL:
        total += 1
        result = analyze(pr, syms)
        ok = _run_field_check(name, result, field, expected)
        if not ok:
            fails += 1
            print(
                f"FAIL structural [{name}]: field={field!r} "
                f"got={result.get(field)!r} exp={expected!r}"
            )

    # Round-trip test
    total += 1
    try:
        ok = _test_analyze_build_results()
        if not ok:
            fails += 1
            print("FAIL [analyze_build_results round-trip]")
    except AssertionError as e:
        fails += 1
        print(f"FAIL [analyze_build_results round-trip]: {e}")
    except Exception as e:
        fails += 1
        print(f"ERROR [analyze_build_results round-trip]: {e}")

    if fails:
        print(f"\n{fails}/{total} FAILED")
        return 1
    print(f"OK: all {total} reachability-lite cases passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
