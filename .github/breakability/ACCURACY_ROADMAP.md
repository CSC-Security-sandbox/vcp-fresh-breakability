# Reaching callgraph-level accuracy — process, not prompts

A self-review of the current design and the concrete, mechanical changes that close the gap to
callgraph-level precision. None of these are prompt tweaks; each is a verifiable process change.

## Honest self-review: where accuracy actually leaks today
1. **Reachability is import-level, used as a proxy for call-level.** `files_importing` /
   `declared_break_reachability.reachability_kind == "import"` prove the *package* is imported,
   never that the *changed symbol* is called. This is the #38 mechanism and the single biggest
   precision ceiling. A function-level callgraph (`deep.go`, already in-repo) resolves it but is
   **not triggered on the ambiguous residue** — so the system pays callgraph's accuracy nowhere.
2. **Verdict text can disagree with the evidence object.** #38's `merge_risk.reason` asserts
   "BREAK-reachable `Error.Code`" while `declared_break_reachability` is `null`. The prose won.
   Provenance was never enforced: the human-readable reason is generated independently of the
   structured evidence instead of *from* it.
3. **No execution ground truth.** Behavioral changes (a changed default, error format, ordering)
   are invisible to build, tests, api-diff AND callgraph. Only running code (a probe) resolves
   them. The probe tier is designed (lite.py tier 3) but unbuilt — so the hardest class is decided
   by inference, not proof.
4. **The corpus is tiny (15 PRs) and the golden file is a "currently-correct" ratchet.** Accuracy
   claims on 15 samples are noise, and a ratchet of conservative verdicts can lock in
   over-flagging as a local optimum. There is no held-out set of *known breaks* to measure
   false-green recall when the loop lowers flags. The loop can regress safety invisibly.
5. **AI evidence was existence-checked, not relevance-checked.** A real-but-irrelevant `file:line`
   (or the dependency manifest, which merely lists the package) passed. Closed in this change set
   (`_cite_references_pkg`, manifest blocklist) — but it illustrates the discipline gap.

## The accuracy stack (each tier narrower + more expensive; escalate, never skip)
| Tier | Question | Mechanism | Runs on | Soundness |
|------|----------|-----------|---------|-----------|
| 0 Import | Is the package imported? | `files_importing` (have) | all PRs | cheap, weak |
| 1 API-diff | Did a symbol we *could* touch change? | apidiff/semver (have) | all PRs | cheap |
| 2 **Function callgraph** | Is the *changed symbol* reachable from our entrypoints? | **`deep.go` CHA→VTA, scoped to importing pkgs** | **residue only** | **sound (callgraph-level)** |
| 3 **Probe** | Does behavior actually differ for *our* call? | synthesize harness, run old vs new in sandbox, diff output | behavioral residue, probe-safe only | **proof by execution** |
| 4 AI adjudication | Judgment when 2/3 are inconclusive | citation- or probe-backed verdict | last resort | falsifiable or discarded |

Key cost insight (why this is NOT Endor-expensive): you never build a whole-program callgraph.
Tier 2 is **demand-driven** — only for PRs that survive tiers 0/1 as ambiguous, only over the
packages that import the bumped module, only querying the specific changed symbols. That is the
same soundness as a full callgraph at a tiny fraction of the compute.

## The four mechanical changes that get us there
### A. Enforce evidence provenance (DONE in `run_gate.py`)
`overclaims_function_reach`: if the verdict text asserts symbol/function reachability while the
structured evidence is absent or import-only-and-unconfirmed (and the build did not actually
fail), it is an OVERCLAIM → gate fails. This makes #38 un-shippable and *forces* escalation to
tier 2. Next step in the pipeline itself: generate `merge_risk.reason` **from** the reachability
object, so prose can never assert more than the evidence licenses.

### B. Wire `deep.go` onto the residue (the callgraph-level lever — NOT yet wired)
Trigger condition (deterministic): `declared_break_reachability.reachability_kind == "import"`
AND `behavior_confirmed == false` AND symbol-level api-diff flagged a changed symbol we import.
Run `deep.go` with the changed symbols as targets. Outcomes:
- `not reachable` + no dynamic constructs → upgrade REVIEW→auto_clear (sound). Kills the
  import-level false-blocks (the current 7) deterministically, no AI.
- `reachable` → stays REVIEW, now with a *function-level* call path as evidence (not an import).
- dynamic constructs present → `POTENTIALLY_REACHABLE`, honest REVIEW (deep.go's honesty rule
  preserves zero false-green).
This is the change that converts "import proxy" into real reachability and lifts auto_clear
without touching false-green.

### C. Build the probe tier for behavioral changes (the true differentiator)
For `CALL_OBSERVABLE` breaks (per `break_class_router.py`): synthesize a minimal harness that
calls the changed symbol the way our repo does, run old-vs-new in `--network=none` read-only
sandbox, diff observable output. Evidence = the stdout/return/error diff. `NOT_OBSERVABLE` breaks
(state/load/timing) are never probed (a minimal probe false-greens) → honest REVIEW. This is the
only mechanism that decides behavioral changes by *proof* instead of inference — and it answers a
question callgraph cannot.

### D. Ground-truth flywheel + backtest (how we *prove* callgraph-level, statistically)
- **Harvest outcomes automatically**: every merged dependency PR + its post-merge fate (later CI
  break? revert? incident referencing it?) becomes a labeled example. The corpus grows from 15 to
  thousands; accuracy claims become statistically real.
- **Backtest on history**: replay historically known-breaking upgrades (mined from git reverts /
  rollback commits) through the pipeline and measure detection rate. This is the held-out
  *known-break* set that guards false-green recall — the counterweight the golden ratchet lacks.
- **Calibrated abstention**: attach a confidence to each verdict; auto-clear only above a
  threshold tuned so empirical false-green ≤ target (e.g. ≤0.5%) on the backtest set. "Accuracy"
  becomes a tunable operating point, and abstaining on genuine uncertainty beats callgraph's
  binary answer.
- **Ensemble disagreement routing**: when tier 2 (callgraph), tier 3 (probe) and tier 4 (AI)
  agree → high-confidence auto-decision; when they disagree → highest-value REVIEW *and* a labeled
  training example. Disagreement is a signal, harvested by process.

## Where AI is the differentiator vs where determinism wins (do not blur these)
- Tiers 0–2 are deterministic and carry the **bulk auto_clear lift**. Do not spend AI there.
- AI earns cost on tier 3/4 residue: synthesizing the probe harness, and judging behavioral
  relevance when even a callgraph path is inconclusive — always backed by a probe diff or a
  symbol-referencing `file:line`, scored against the corpus by `run_gate.py`. AI is *measured*,
  never trusted.

## Definition of done (callgraph-parity, measurable)
On the backtest known-break set: false-green recall ≥ callgraph baseline at equal-or-higher
auto_clear%, with every auto_clear backed by a tier-2 unreachable proof or a tier-3 no-diff proof.
`run_gate.py` already reports the AI-on/off delta and hard-fails on false-green, invented
citations, and overclaims — extend it to consume the backtest set as a second corpus.
