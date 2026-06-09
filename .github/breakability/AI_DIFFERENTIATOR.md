# AI as the Differentiator — Spec

> Why this exists: callgraph reachability is expensive and proves only *structural* reachability
> ("a path exists"). The breakage developers actually hit is *behavioral* (changed default, error
> format, ordering, validation) — invisible to callgraph and to a passing build. AI, used with
> discipline, answers the more useful question — *behavioral relevance to OUR code* — cheaply.
> AI here is NOT a cheap callgraph; it is a better oracle for a different, more useful question.

## The thesis
A passing build + passing tests does NOT prove an upgrade is safe (tests don't exercise changed
defaults/config-dependent behavior). A changelog "Breaking Changes" heading does NOT prove it
breaks US. The gap between those two is exactly where a human reviewer spends their time — and
exactly what AI can do at scale, IF every verdict is falsifiable.

## The dev's triage process → the AI move that replaces it
| Dev step | AI move | Cost | Where it runs |
|----------|---------|------|---------------|
| 1. Read changelog, find the breaking bullet | Changelog extraction (not just heading) | tiny | all PRs (deterministic-first) |
| 2. "Do we even use it?" | Usage scan (deterministic) | tiny | all PRs |
| 3. "*How* do we use it — does the change hit our call?" | **① Call-site adjudication** | low | REVIEW bucket |
| 4. "If unsure, write a quick test and run it" | **② Router-gated dynamic probe** | medium | REVIEW + probe-safe |
| 5. "Still unclear — dig" | **③ Agentic residue loop** | high | REVIEW residue only |
| (SCA) "Is the CVE reachable by attacker input?" | **④ Exploitability reachability** | low–med | CVE PRs |

## Deterministic-vs-AI split (do NOT waste AI on clean cases)
- **Deterministic handles the bulk auto_clear lift**: clean patch/minor/CVE-fix bumps with passing
  build+test and NO declared/apidiff break → `auto_clear`. No AI call. (This fixes most of the
  current 7 false-blocks; auto_clear 6.7% → target.)
- **AI earns its cost only on the ambiguous residue**: build+test pass BUT a breaking change is
  declared (changelog) or apidiff-flagged a symbol we touch. That's where ①/② decide relevance.
- Rule: never spend an AI call where the deterministic verdict is already unambiguous.

## ① Call-site semantic adjudication  (BUILT — `ai_adjudicator_prompt.md` + `validate_adjudication.py`)
Per REVIEW PR: feed the changed symbol + our call sites (module-scoped) + the changelog bullet.
AI judges relevance. Falsifiable: must cite a real `file:line` or it is rejected and the PR stays
REVIEW. Can downgrade REVIEW→SAFE only with `reachable=false` + a real citation. Never FIX, never
clears a CVE. This kills the #11-class (additive, we are a caller not implementer) false positive.

## ② Router-gated dynamic probe  (THE differentiator — half-built)
For *behavioral* changes static cannot see, AI synthesizes a tiny harness that calls the changed
symbol the way our repo does, runs it against from-version vs to-version in a sandbox, and diffs the
observable output. Evidence = the stdout/return/error **diff**, not prose.

**Safety valve (already built): `break_class_router.py`.** Before probing, classify the declared
break:
- `CALL_OBSERVABLE` (changed return/error/format/signature/default-with-call-evidence) → **probe**.
- `NOT_OBSERVABLE` (cardinality/memory/timing/concurrency/state — manifests under load) → **never
  probe** (a minimal probe false-greens); route to ① or ③ + honest REVIEW.
- `AMBIGUOUS` → treat as not-probe-able.

This router is what makes AI-generated probes trustworthy instead of dangerous. Wire it as the
gate in front of probe synthesis.

Sandbox contract: `--network=none`, read-only root, tmpfs `/tmp`, hard timeout (10–30s), no repo
secrets. Probe output schema: `{ symbol, oldOutput, newOutput, differs: bool, observable: bool }`.
If `differs && observable` → REVIEW with the diff as proof. If `!differs` → strong SAFE signal.

## ③ Agentic residue loop  (expensive — gate hard)
Multi-step ReAct only for PRs ① and ② cannot resolve: grep usage → read call sites → hypothesize →
run ONLY the affected repo tests (not the whole suite) → conclude with citation. Cap iterations and
run on a cheap model. Never on all PRs.

## ④ CVE exploitability reachability  (Dependabot cannot do this)
For CVE PRs: AI traces whether attacker-controlled input can reach the vulnerable function
(`REACHABLE / POTENTIALLY_REACHABLE / NOT_REACHABLE / UNKNOWN`, per `AI_LAYER_SPEC.md`). Turns a
version-match advisory into a reachability verdict. Advisory-only (never auto-clears a CVE).

## Falsifiability discipline (non-negotiable — this is where weeks were lost)
Every AI verdict carries machine-checkable evidence or is discarded:
- ① → real `file:line` (guarded by `validate_adjudication.py`).
- ② → probe stdout/return diff.
- ③ → the exact test run + result.
- All fail-open to deterministic; AI may downgrade-with-proof, never upgrade to FIX, never clear CVE.
- Output scored against `corpus.json` by the gate — AI is measured, not trusted.

## How we prove it's working (measurable)
`run_gate.py --ai ai_verdicts.json` applies only VALIDATED verdicts and reports the AI-on vs AI-off
delta: auto_clear%, false_block↓, false_green (must stay 0), and `ai_proof_added` (REVIEWs that
gained a real citation instead of generic caution). If AI-on does not improve the number without
raising false_green, the AI layer is not earning its cost — and the gate will say so.

## Status vs the old PRD
`AI_LAYER_SPEC.md` already specifies ① (behavioral reasoning) and ④ (exploitability) as single-shot
structured-output with an evidence validator — good, and aligned with what is built. It does NOT
specify ② (router-gated dynamic probe) or ③ (agentic loop). Those are the unwritten differentiator;
this doc is their spec. `break_class_router.py` is the prerequisite for ② and already exists.
