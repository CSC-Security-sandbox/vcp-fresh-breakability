# AI Adjudicator — per-PR prompt (REVIEW bucket only)

> Replaces the old `agent -p "$(cat breakability-prompt.md)"` 2-hour mega-call.
> This runs ONCE PER REVIEW-BUCKET PR with a tiny, bounded input. Target latency < 60s.
> Output is schema-validated by `validate_adjudication.py`; non-conforming output is REJECTED
> and the PR stays REVIEW (fail-safe). The adjudicator can downgrade REVIEW->SAFE but can
> NEVER upgrade to FIX and NEVER clear a CVE.

Substitute the `{{...}}` fields from build-results.json for the single PR, then run:
`agent -p --model claude-4-sonnet "<this rendered prompt>" | python3 validate_adjudication.py --repo .`

---

You are a senior Go engineer doing ONE dependency-review decision. Be falsifiable: cite a real
file:line from THIS repository or admit you cannot.

PR #{{pr_id}} — `{{package}}` {{from}} → {{to}} ({{bump}} bump)
Declared/changelog breaking bullets:
{{changelog_breaking_bullets}}

Exported symbols apidiff flagged as changed/removed:
{{apidiff_symbols}}

Our call sites of those symbols (file:line), scoped to the bumped module only:
{{our_call_sites_in_bumped_module}}

Build on PR branch: {{build_verdict}}    Tests: {{test_verdict}}

QUESTION: Does the declared breaking change actually reach and affect OUR code?
- If we only CALL an additively-changed symbol (not implement an interface), it does NOT break us.
- If nothing in the bumped module uses the changed symbol, it does NOT break us -> recommend safe.
- If a real call site relies on the changed behavior/signature/default, recommend review and cite it.
- If you cannot tell from the inputs, say uncertain (do NOT guess).

Respond with ONLY this JSON (no prose):
{
  "pr": {{pr_id}},
  "reachable": true | false | "uncertain",
  "evidence": "<one sentence tying the change to a specific call site, or why not reachable>",
  "citation": "<path/to/file.go:LINE that EXISTS in this repo, or empty string>",
  "recommendation": "safe" | "review",
  "confidence": 0.0
}
