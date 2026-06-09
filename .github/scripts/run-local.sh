#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# run-local.sh — Run the FULL breakability pipeline locally, fast, no GitHub Actions.
#
# Same scripts the CI workflow (breakability-agent.yml) runs, chained end-to-end on
# your machine so you get feedback in seconds instead of a ~30 min CI round-trip:
#
#   1. deterministic   build-check.sh           -> /tmp/build-results.json   (slow, cacheable)
#   2. policy          policy_lowering.py --enrich
#   3. ai              independent_adjudicate.sh -> /tmp/ai_verdicts.json     (per-PR agent calls)
#   4. reconcile       reconcile_adjudication.py --write
#   5. comments        post-fallback-comments.sh (DRY_RUN by default)
#   6. summary         final verdict table
#
# The slow part is (1). Once you have a build-results.json you can iterate on the AI
# layer (3-5) in seconds with --from ai. Comments are dry-run by default (rendered to
# files, never posted) so you can iterate without touching real PRs. Add --post to
# actually comment, once you're shippable.
#
# Usage:
#   .github/scripts/run-local.sh --prs 10,23,38                 # full run, dry-run comments
#   .github/scripts/run-local.sh --prs 10,23,38 --from ai       # reuse cached build-results, redo AI+
#   .github/scripts/run-local.sh --prs 10 --from reconcile      # reuse cached AI verdicts, redo reconcile+
#   .github/scripts/run-local.sh --prs 10,23,38 --post          # actually post comments
#   .github/scripts/run-local.sh --seed path/to/build-results.json --from ai
#
# Flags:
#   --prs LIST        comma-separated PR numbers (required unless --seed already has them)
#   --from STAGE      start stage: deterministic|policy|ai|reconcile|comments  (default deterministic)
#   --to STAGE        stop after stage (default summary)
#   --model NAME      agent model for AI stage (default claude-4-sonnet)
#   --skip-ai         do not call the agent; rely on Tier-0 deterministic module-scope only
#   --post            actually post comments to GitHub (default: dry-run to files)
#   --seed FILE       copy FILE -> /tmp/build-results.json before starting (reuse a CI artifact)
#   --results FILE    results file path (default /tmp/build-results.json)
# ──────────────────────────────────────────────────────────────────────────────
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
HARNESS="$REPO_ROOT/.github/breakability/harness"

PRS=""
FROM="deterministic"
TO="summary"
MODEL="${ADJ_MODEL:-claude-4-sonnet}"
SKIP_AI=0
POST=0
SEED=""
RESULTS="/tmp/build-results.json"
VERDICTS="/tmp/ai_verdicts.json"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prs)      PRS="$2"; shift 2;;
    --from)     FROM="$2"; shift 2;;
    --to)       TO="$2"; shift 2;;
    --model)    MODEL="$2"; shift 2;;
    --skip-ai)  SKIP_AI=1; shift;;
    --post)     POST=1; shift;;
    --seed)     SEED="$2"; shift 2;;
    --results)  RESULTS="$2"; shift 2;;
    -h|--help)  sed -n '2,46p' "$0"; exit 0;;
    *) echo "unknown flag: $1" >&2; exit 2;;
  esac
done

# Stage ordering so --from/--to can gate.
stage_idx() { case "$1" in
  deterministic) echo 1;; policy) echo 2;; ai) echo 3;; reconcile) echo 4;; comments) echo 5;; summary) echo 6;;
  *) echo 0;; esac; }
FROM_I=$(stage_idx "$FROM"); TO_I=$(stage_idx "$TO")
[[ "$FROM_I" == 0 ]] && { echo "bad --from $FROM" >&2; exit 2; }
[[ "$TO_I"   == 0 ]] && { echo "bad --to $TO" >&2; exit 2; }
run_stage() { local i; i=$(stage_idx "$1"); [[ "$i" -ge "$FROM_I" && "$i" -le "$TO_I" ]]; }

cd "$REPO_ROOT"
# Auto-load a local, gitignored env file so CURSOR_API_KEY (and any registry tokens) are
# picked up without re-exporting every shell. Create .github/breakability/.env.local with:
#   export CURSOR_API_KEY=...
ENV_LOCAL="$REPO_ROOT/.github/breakability/.env.local"
if [[ -f "$ENV_LOCAL" ]]; then
  # shellcheck disable=SC1090
  set -a; source "$ENV_LOCAL"; set +a
  echo "loaded local env: $ENV_LOCAL"
fi
hr() { printf '─%.0s' {1..78}; echo; }
t0=$(date +%s)
say() { echo; hr; echo "▶ $*"; hr; }

# ── Preflight ─────────────────────────────────────────────────────────────────
# (a) The self-hosted CI runner lives on this same machine and writes the SAME
#     /tmp/build-results.json. Concurrent local + CI runs clobber each other.
if command -v gh >/dev/null 2>&1; then
  _ACTIVE_CI=$(gh run list --workflow=breakability-agent.yml --status in_progress \
                 --json databaseId -q '.[].databaseId' 2>/dev/null | head -1 || true)
  if [[ -n "${_ACTIVE_CI:-}" ]]; then
    echo "⚠️  A breakability CI run ($_ACTIVE_CI) is IN PROGRESS on the self-hosted runner."
    echo "    It shares /tmp/build-results.json with this machine and will corrupt local results."
    echo "    Cancel it first:  gh run cancel $_ACTIVE_CI"
    echo
  fi
fi
# (b) The AI stage uses `agent -p` (print mode), which requires CURSOR_API_KEY even
#     when you are interactively logged in (the keychain session is ignored headlessly).
if [[ "$SKIP_AI" == 0 && "$FROM_I" -le 3 && "$TO_I" -ge 3 ]]; then
  if [[ -z "${CURSOR_API_KEY:-}" ]]; then
    echo "⚠️  CURSOR_API_KEY is not set — agent -p (headless) cannot authenticate."
    echo "    The AI stage will be SKIPPED (Tier-0 deterministic module-scope still runs)."
    echo "    To enable the AI layer locally:  export CURSOR_API_KEY=<your key>  (same as the CI secret)"
    echo
    SKIP_AI=1
  fi
fi

if [[ -n "$SEED" ]]; then
  cp "$SEED" "$RESULTS"
  echo "Seeded $RESULTS from $SEED"
fi

# ── 1. Deterministic ──────────────────────────────────────────────────────────
if run_stage deterministic; then
  say "1/6 deterministic build analysis (PRs: ${PRS:-<all open>})"
  [[ -z "$PRS" ]] && { echo "note: no --prs given; build-check will discover open PRs"; }
  st=$(date +%s)
  PR_FILTER="$PRS" BREAKABILITY_PR_NUMBERS="$PRS" \
    CLI_PATH=".github/actions/breakability-check/index.js" \
    REPO_ROOT="$REPO_ROOT" \
    bash .github/scripts/build-check.sh
  echo "deterministic done in $(( $(date +%s) - st ))s -> $RESULTS"
else
  echo "skip deterministic (--from $FROM); reusing $RESULTS"
fi
[[ -f "$RESULTS" ]] || { echo "no $RESULTS — run the deterministic stage first" >&2; exit 1; }
# the pipeline scripts hardcode /tmp/build-results.json; keep them in sync
[[ "$RESULTS" != "/tmp/build-results.json" ]] && cp "$RESULTS" /tmp/build-results.json

# ── 2. Policy lowering ────────────────────────────────────────────────────────
if run_stage policy; then
  say "2/6 policy lowering (enrich verdicts)"
  # M8 changelog comprehension: enrich each PR with structured breaking_claims[]
  # (symbol/kind/old/new/severity) so reachability has named symbols to check, not
  # just a '### Breaking Changes' heading. Advisory + non-breaking: never changes a
  # verdict here. Set M8_USE_AI=1 (and BRK_AGENT_MODE) to enable AI enrichment.
  if [[ -f .github/scripts/changelog_comprehension.py ]]; then
    python3 .github/scripts/changelog_comprehension.py /tmp/build-results.json \
      ${M8_USE_AI:+--ai} --write \
      && echo "M8 changelog comprehension applied" \
      || echo "[warn] M8 changelog comprehension failed; continuing"
  fi
  if [[ -f .github/scripts/policy_lowering.py ]]; then
    python3 .github/scripts/policy_lowering.py /tmp/build-results.json --enrich -o /tmp/build-results.policy.json \
      && mv /tmp/build-results.policy.json /tmp/build-results.json \
      && echo "policy lowering applied" \
      || echo "[warn] policy lowering failed; continuing with raw verdicts"
  else
    echo "no policy_lowering.py; skipping"
  fi
fi

# ── 3. Independent AI adjudication (per-PR) ───────────────────────────────────
if run_stage ai; then
  say "3/6 independent AI adjudication (model: $MODEL, skip_ai=$SKIP_AI)"
  if [[ "$SKIP_AI" == 1 ]]; then
    echo '{}' > "$VERDICTS"
    echo "skip-ai: empty verdicts (Tier-0 deterministic module-scope still applies in reconcile)"
  else
    st=$(date +%s)
    bash .github/scripts/independent_adjudicate.sh /tmp/build-results.json "$VERDICTS" "$MODEL"
    echo "ai adjudication done in $(( $(date +%s) - st ))s -> $VERDICTS"
  fi
fi

# ── 4. Reconcile AI + deterministic ───────────────────────────────────────────
if run_stage reconcile; then
  say "4/6 reconcile AI + deterministic verdicts"
  V=""
  [[ -f "$VERDICTS" ]] && V="--verdicts $VERDICTS"
  python3 .github/scripts/reconcile_adjudication.py /tmp/build-results.json $V --repo "$REPO_ROOT" --write
fi

# ── 5. Comments (dry-run unless --post) ───────────────────────────────────────
if run_stage comments; then
  if [[ "$POST" == 1 ]]; then
    say "5/6 posting comments to GitHub (LIVE)"
    DRY_RUN=0 bash .github/scripts/post-fallback-comments.sh
  else
    say "5/6 rendering comments (DRY-RUN -> /tmp/breakability-local/comments)"
    DRY_RUN=1 DRY_RUN_DIR=/tmp/breakability-local/comments bash .github/scripts/post-fallback-comments.sh
    echo "rendered comments:"; ls -1 /tmp/breakability-local/comments/ 2>/dev/null | sed 's/^/  /' || true
  fi
fi

# ── 6. Summary table ──────────────────────────────────────────────────────────
if run_stage summary; then
  say "6/6 final verdicts"
  python3 - /tmp/build-results.json <<'PY'
import json, os, sys
sys.path.insert(0, os.path.join(os.getcwd(), ".github", "scripts"))
from verdict_contract import authoritative_verdict, prediction_for_pr, stage_report
d = json.load(open(sys.argv[1]))
prs = d.get("prs") or {}
rows = []
review_prs, ai_applied = 0, 0
for k, p in prs.items():
    av = authoritative_verdict(p)            # the SINGLE source of truth
    verdict = av.get("verdict", "?")
    conf = av.get("confidence", "-")
    src = av.get("source", "-")
    if verdict == "REVIEW":
        review_prs += 1
    if str((p.get("ai_adjudication") or {}).get("applied") or ""):
        ai_applied += 1
    pkg = p.get("package") or "?"
    rows.append((int(k) if str(k).isdigit() else k, verdict, conf, src, pkg))
rows.sort(key=lambda r: str(r[0]))
print(f"{'PR':>5}  {'VERDICT':<8} {'CONF':<5} {'SOURCE':<16} PACKAGE")
for r in rows:
    print(f"{r[0]:>5}  {r[1]:<8} {r[2]:<5} {r[3]:<16} {r[4]}")
# Loud AI-coverage visibility — surfaces the "dormant AI layer" failure immediately
# instead of weeks later. (Not a hard fail: a REVIEW PR may legitimately need no AI.)
print()
print(stage_report("ai-coverage", input_count=review_prs, processed_count=ai_applied))
if review_prs > 0 and ai_applied == 0:
    print("  [warn] AI layer applied 0 verdicts to %d REVIEW PR(s) — check the agent ran "
          "(auth/keychain/field-mismatch), not silently dormant." % review_prs)
PY
fi

echo
echo "total: $(( $(date +%s) - t0 ))s   results: /tmp/build-results.json"
