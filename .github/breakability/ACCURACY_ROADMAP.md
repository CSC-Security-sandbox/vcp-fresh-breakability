# Reaching high accuracy WITHOUT a callgraph — AI is the decision layer

A function-level callgraph (CHA→VTA) was evaluated and **dropped**: too compute-heavy, and on this
repo it degrades to `UNKNOWN` (no clean entrypoints / pervasive reflect-unsafe-cgo-plugin) — paying
the cost for no answer. Accuracy is reached a different way: **cheap deterministic scoping to shrink
the residue, then the AI layer to DECIDE it** — because deterministic alone is provably not enough
(behavioral changes, cross-module attribution, removed-symbol judgment all need a reasoner).

## Self-review: where accuracy leaks
1. Reachability is import-level used as a call-level proxy (`files_importing` proves the package is
   imported, never that the changed symbol is called) — the #38 mechanism.
2. Cross-module attribution bug: multi-module repo (root, `cicd`, `automations/tstctl`). #38 bumps
   lib/pq in `automations/tstctl` but the tool counted `Error.Code` usages in the ROOT module.
3. Verdict prose can out-claim its evidence (#38 reason asserts reachability; evidence object null).
4. No reasoner on behavioral change: #23 (otel) is signature-identical; build/test/apidiff all pass.
   Only an AI reasoner — or a probe — can judge relevance.
5. AI evidence was existence-checked, not relevance-checked (fixed: `_cite_references_pkg` +
   manifest blocklist in run_gate.py).

## The stack (no callgraph): cheap scope → AI decision → probe proof
| Layer | Question | Mechanism | Cost | Runs on |
|-------|----------|-----------|------|---------|
| 0 Import | Package imported? | files_importing | ~0 | all |
| 1 API-diff | A symbol we could touch changed? | apidiff/semver | ~0 | all |
| 2 Module-scope filter | Are recorded usages even in the BUMPED module? | scope_check.py (path-prefix filter on deterministic.usages) | ~0 | residue |
| 3 AI adjudication | Does the change reach+affect OUR call? | per-PR AI agent, grounded in repo, falsifiable citation | low | residue |
| 4 AI-synth probe | Does behavior actually differ for our call? | AI writes harness, run old vs new in sandbox, diff output | medium | behavioral residue, probe-safe |

Layer 2 is the cheap stand-in for the ATTRIBUTION part of a callgraph (kills #38 in microseconds:
`Error.Code` usages are root-module, bump is automations/tstctl → not reachable). It is NOT a
callgraph — it only filters evidence by module. Everything still ambiguous is handed to AI.

## Why AI is the decision layer (demonstrated on a live run)
Three residue classes deterministic cannot close — each run through a real AI agent grounded in repo:
- Behavioral change, no signature diff (#23 otel): AI read the changelog + our exporter setup at
  utils/middleware/log/metric.go:72, kept REVIEW, and HONESTLY said it cannot prove no dropped-data
  without a runtime probe → defers to layer 4. (Never downgrades a behavioral change to safe.)
- Removed/deprecated symbol, no extracted usages (#10 go-jira): AI grepped cicd/cmd/jira, found we
  never call the removed Search()/SearchWithContext() and discard the changed Response — recommended
  safe with a cited call site. NOTE: verified corpus labels #10 true_review; the gate flagged the
  AI downgrade as a disagreement (false_green) rather than trusting AI over ground truth. The
  disagreement is the signal: reconcile the label or keep conservative. AI never overrides truth.
- Cross-module hard-break (#38 lib/pq): AI confirmed automations/tstctl has no lib/pq call sites
  (reachable=false) but could not cite a line for an ABSENCE → gate REJECTED the downgrade by design.
  #38 is instead auto-cleared deterministically by layer 2.

AI is MEASURED, never trusted: schema-validated (validate_adjudication.py), then scored against the
verified corpus by run_gate.py --ai. AI may downgrade REVIEW→SAFE only with reachable=false + a real
SOURCE citation; never FIX, never clear a CVE; fail-open to REVIEW on malformed/unfalsifiable output.

## Gate invariants (deterministic, seconds)
- false_green = 0 — never clear a real break.
- invented_citations = 0 — claims reachability but files_importing empty / files absent.
- overclaims = 0 — verdict text asserts symbol reachability while evidence is import-only/absent and
  build did not fail → forces the decision to AI/probe, not prose.
- AI citation must be a real SOURCE call site referencing the package — manifests and
  real-but-irrelevant files rejected (AI_REJECTED).

## Build the probe tier (layer 4 — turns "uncertain" into proof)
For CALL_OBSERVABLE breaks (break_class_router.py): AI synthesizes a minimal harness calling the
changed symbol the way our repo does, runs old-vs-new in a --network=none read-only sandbox, diffs
observable output. Evidence = the stdout/return/error diff. NOT_OBSERVABLE breaks (state/load/timing)
are never probed (a minimal probe false-greens) → honest REVIEW. This is the only mechanism that
decides #23-class behavioral changes by proof, and needs no callgraph.

## Ground-truth flywheel (prove accuracy statistically)
- Harvest every merged dependency PR + post-merge fate (later CI break / revert / incident) as a
  labeled example; corpus grows from 15 to thousands.
- Backtest on historical known-breaking upgrades (mined from revert commits) — the held-out
  known-break set that guards false-green recall (the golden ratchet alone cannot).
- Calibrated abstention: attach confidence; auto-clear only above a tuned threshold so empirical
  false-green ≤ target on the backtest. Abstaining on genuine uncertainty beats a forced binary.

## Definition of done (measurable, callgraph-free)
On the backtest known-break set: false-green recall at-or-above the dropped-callgraph baseline at
equal-or-higher auto_clear%, every auto_clear backed by a layer-2 out-of-module proof, a layer-3 AI
cited-absence, or a layer-4 no-diff probe. run_gate.py hard-fails on false-green, invented citations,
and overclaims, and reports the AI-on vs AI-off delta.
