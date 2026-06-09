"""Unit tests for the sound call-graph reachability evidence rendered into the
AI adjudicator prompt (render_prompt.render_reach).

The contract that keeps the layer SAFE:
  - no call-graph result  -> explicitly weak/uncertain (never implies safe)
  - analysis FAILED        -> explicitly UNKNOWN (never implies safe)
  - analyzed               -> per-symbol direct/transitive verdicts surfaced
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import render_prompt as rp


def test_no_reach_is_weak_not_safe():
    out = rp.render_reach(None)
    assert "not run" in out.lower()
    assert "miss" in out.lower()  # warns grep can miss indirect dispatch
    assert "safe" not in out.lower().split("treat")[0]  # never asserts safe


def test_failed_analysis_is_unknown_not_safe():
    out = rp.render_reach({"analyzed": False, "error": "build failed"})
    assert "failed" in out.lower()
    assert "unknown" in out.lower()
    assert "do not assume safe" in out.lower()


def test_analyzed_surfaces_direct_and_transitive():
    reach = {
        "analyzed": True,
        "roots": 100,
        "any_direct_in_module": True,
        "any_transitively_reachable": True,
        "results": [
            {"symbol": "Search", "direct_in_module": True,
             "transitively_reachable": True, "direct_sites": ["a/b.go:42"]},
            {"symbol": "PlanScan", "direct_in_module": False,
             "transitively_reachable": True, "direct_sites": []},
            {"symbol": "Unused", "direct_in_module": False,
             "transitively_reachable": False, "direct_sites": []},
        ],
    }
    out = rp.render_reach(reach)
    assert "any_direct_in_module=True" in out
    assert "Search" in out and "a/b.go:42" in out
    assert "compile/signature break" in out          # direct call → compile-break framing
    assert "behavioral-change exposure only" in out    # transitive-only → behavioral framing
    assert "NOT reachable at all" in out               # unused symbol
