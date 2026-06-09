# Breakability MVP Harness

The **fitness function** that was missing for 60+ iterations. The loop now optimizes toward a
deterministic number, not an LLM's mood.

| File | Role |
|------|------|
| `../SPEC.md` | Frozen acceptance contract (Go-only, 4 signals + narrow AI adjudicator, #121 output). Do not renegotiate mid-loop. |
| `corpus.json` | 15 PRs with **verified** ground-truth labels (checked against build-results.json + repo source). The oracle. |
| `golden_predictions.json` | The ideal prediction per PR. The loop must not regress these. |
| `run_gate.py` | Grades a `build-results.json` vs corpus + golden. HARD gates: zero false-green, zero invented citations, no golden regression. Emits `SCORE`/`ACCEPTED`. |
| `ai_adjudicator_prompt.md` | Narrow per-PR AI prompt (REVIEW bucket only). Replaces the dormant 2-hour mega-call. |
| `validate_adjudication.py` | Schema + invented-citation guard for AI output. AI can downgrade REVIEW→SAFE only with a real citation; never FIX, never clear CVE. |
| `gated_loop.sh` | Closed-loop: grade → cheap fix-agent in worktree → fast re-grade (re-run classifier over cached raw signals, no rebuild) → merge **only if score strictly improves**, else auto-rollback. |

## Closed-loop regrade
The expensive `build-check.sh` (per-PR `go build/test`) runs once to produce raw signals
(`/tmp/build-results.raw.json`). Each loop iteration only re-runs the **classification layer**
(`policy_lowering.py` etc., seconds) over those cached signals — the layer where the gate
failures actually live — so iterations are fast and deterministic. Override `REGRADE_CMD` to
chain more post-processors as they're made stdin/stdout separable.

`golden_predictions.json` is a **ratchet**: it pins the PRs the tool *already gets right* so a
fix can never regress them. It is re-snapshotted on each ACCEPTED state.

## Run the gate (baseline on the current tool output)
```bash
git show origin/breakability-results:build-results.json > /tmp/build-results.json
python3 .github/breakability/harness/run_gate.py /tmp/build-results.json \
  .github/breakability/harness/corpus.json --repo . \
  --golden .github/breakability/harness/golden_predictions.json
```

## Current baseline (the honest starting point)
- false_green: **0** (tool is conservative — good)
- invented_citations: **1** (PR#38 — module-scoped reachability bug) → HARD FAIL
- false_block: **7** (safe PRs over-flagged as review) → auto_clear only 6.7% vs 30% target
- SCORE: 0.0, REJECTED

## What the loop must drive to ACCEPTED
1. Fix module-scoped reachability → invented_citations = 0.
2. Stop over-flagging clean patch/minor/CVE-fix bumps → false_block ↓, auto_clear ≥ 30%.
3. Keep false_green at 0 the whole time.
