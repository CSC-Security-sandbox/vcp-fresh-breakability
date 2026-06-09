# Breakability MVP — Frozen Spec (v1)

> This file is the **acceptance contract**. The loop optimizes toward THIS, not a reviewer's mood.
> Do not renegotiate scope mid-loop. Changes to this file require a human commit, never an agent.

## Scope (v1) — frozen
- **Ecosystem: Go only.** Node / Python / Maven / Docker are OUT of v1. (Adapters come after the Go floor is locked and green.)
- **Signals: 4 deterministic + 1 narrow AI adjudicator.**
  1. RESOLVE   — `go mod tidy` for the affected module(s); capture real stdout + go.mod/go.sum diff.
  2. BUILD     — `go build` of affected dirs, diffed vs `main` baseline (new errors vs pre-existing).
  3. TEST      — `go test` affected pkgs, diffed vs `main` baseline.
  4. API-DIFF  — `apidiff` from→to on exported symbols; cross-referenced with importers **in the bumped module only**.
  5. AI-ADJUDICATOR (the differentiator) — see below. Runs ONLY on the REVIEW bucket, never on SAFE/FIX.
- **Output:** one comment per PR + one merge-plan Issue, format **frozen to the golden snapshot** (issue #121 style).

## Verdict buckets (what the dev sees)
- `SAFE`   — build+test pass, no break-reachable API change, no unresolved changelog break. Merge without review.
- `REVIEW` — build passes but a *reachable* breaking change or behavioral change is plausible. Needs the dev (or AI) to look.
- `FIX`    — build or test introduces NEW errors vs main. Do not merge.
- Each carries: verification level (L1–L4), CVE/priority, evidence with **real stdout**, and (for REVIEW) the AI verdict.

## The AI adjudicator — narrow, falsifiable, gated
The old design failed because it ran the entire 1,122-line prompt as one 2-hour `agent -p` call with
`continue-on-error: true` → it timed out, was swallowed, and the deterministic fallback posted. Result:
`Oracle confidence: not available` on every PR for weeks.

New contract — the AI does ONE thing a human reviewer does, per REVIEW-bucket PR, in <60s:

INPUT (deterministic, pre-computed): package, from→to, changelog/release-note breaking bullets,
the exported symbols apidiff flagged, and the list of **our** call sites (file:line) of those symbols.

OUTPUT (strict JSON, schema-validated — non-conforming = rejected, treated as abstain):
```json
{
  "pr": 11,
  "reachable": true|false|"uncertain",
  "evidence": "database/drivers/postgres/migrate.go:42 calls NewWithInstance with 3 args; v4.19 changed the 3rd arg default",
  "citation": "file:line that exists in THIS repo, or empty",
  "recommendation": "review|safe|fix",
  "confidence": 0.0-1.0
}
```
Hard rules:
- If `citation` does not resolve to a real file:line in the repo → **reject** (no invented symbols; this is the #38 failure mode).
- If `reachable=false` with a real citation showing the changed symbol is NOT used → adjudicator may **downgrade REVIEW→SAFE**.
- The adjudicator can NEVER upgrade to FIX (only the deterministic build can) and NEVER clear a CVE.
- `continue-on-error` stays, BUT a failed/empty/invalid AI run is recorded as `oracle: unavailable` and the PR
  **stays REVIEW** (fail-safe). The workflow must surface "agent ran: yes/no" — no more silent dormancy.

## Acceptance gate (the fitness function — deterministic, run in seconds)
Computed by `harness/run_gate.py` over the committed `build-results.json` + rendered comments:
1. **Zero false-green** vs `corpus.json` labels (a true_review/true_fix predicted SAFE) — HARD FAIL.
2. **Zero invented citations** — every symbol/file claimed in a comment must exist in the repo — HARD FAIL.
3. **No golden regression** — merge-plan categorization + comment structure must match `golden/` (diff-clean) — HARD FAIL.
4. **auto_clear ≥ 30%** (v1 realistic target; raise later) — SOFT (warn).
5. Report `false_block` (true_safe → REVIEW/FIX) as the noise metric to minimize.

A run is ACCEPTED iff gates 1–3 pass. The loop keeps a change only if the gate is ACCEPTED and score ≥ previous.

## Non-goals (v1)
- Full call-graph reachability (Endor-style) — too slow on the VSA repo; the AI adjudicator approximates it cheaply.
- Multi-language. CVE call-chain proof (govulncheck stays advisory/off). Enforce mode (stay advisory).
