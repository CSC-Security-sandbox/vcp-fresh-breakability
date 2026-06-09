#!/bin/bash
# Gated breakability loop — Ralph-style, but graded by a DETERMINISTIC oracle.
#
# Why this exists: the original loop.sh graded each iteration with an LLM "SCORE: X.X"
# from a reviewer agent. That is a noisy, drifting oracle -> the loop oscillated for 60+
# iterations and never converged. This version grades with run_gate.py against a VERIFIED
# labeled corpus + golden target + invented-citation guard. A change is kept ONLY if the
# gate ACCEPTS it and the score does not regress. Convergence is now possible.
#
# Key differences vs loop.sh:
#   - Oracle = run_gate.py (seconds, local), not a 6-min Mac CI + LLM score.
#   - HARD gates: zero false-green, zero invented citations, no golden regression.
#   - Fix agent runs in a worktree on a CHEAP model; its diff is only merged if the gate
#     score strictly improves AND stays ACCEPTED-or-better. Otherwise auto-rollback.
#   - Fresh agent context every iteration (no 32-compaction rot).
set -uo pipefail

BASE_DIR="${BASE_DIR:-$(cd "$(dirname "$0")/../../.." && pwd)}"
HARNESS="$BASE_DIR/.github/breakability/harness"
RESULTS="${RESULTS:-/tmp/build-results.json}"     # committed deterministic output to grade
CORPUS="$HARNESS/corpus.json"
GOLDEN="$HARNESS/golden_predictions.json"
MAX_ITERS="${MAX_ITERS:-8}"
WORKTREES_DIR="${WORKTREES_DIR:-/tmp/brk-worktrees}"
COPILOT="${COPILOT:-copilot}"
FIX_MODEL="${FIX_MODEL:-claude-haiku-4.5}"         # cheap; gate decides correctness, not the model
REPO="${REPO:-CSC-Security-sandbox/vcp-vsa-breakability-test}"

log() { echo "$(date '+%H:%M:%S') [iter${ITER:-0}] $*"; }

run_gate() {
  python3 "$HARNESS/run_gate.py" "$RESULTS" "$CORPUS" --repo "$BASE_DIR" --golden "$GOLDEN" 2>&1
}

mkdir -p "$WORKTREES_DIR"
PREV_SCORE="-1"

for ITER in $(seq 1 "$MAX_ITERS"); do
  log "=== ITERATION $ITER ==="

  # 1. Grade the current committed scripts/output deterministically.
  GATE_OUT="$(run_gate)"; echo "$GATE_OUT"
  SCORE=$(echo "$GATE_OUT"  | awk -F': ' '/^SCORE:/{print $2}')
  ACCEPTED=$(echo "$GATE_OUT" | awk -F': ' '/^ACCEPTED:/{print $2}')
  log "score=$SCORE accepted=$ACCEPTED (prev=$PREV_SCORE)"

  if [[ "$ACCEPTED" == "True" ]]; then
    log "GATE ACCEPTED. Locking this state as the new golden baseline."
    cp "$RESULTS" "$HARNESS/golden_build-results.json" 2>/dev/null || true
    log "DONE — converged at score $SCORE"
    exit 0
  fi

  # 2. Tag for rollback, spin a worktree.
  PRE_TAG="gate-iter${ITER}-pre"
  git -C "$BASE_DIR" tag -f "$PRE_TAG" HEAD >/dev/null 2>&1
  WT="$WORKTREES_DIR/iter${ITER}"; rm -rf "$WT"
  git -C "$BASE_DIR" worktree add -q "$WT" -b "gate-iter${ITER}" HEAD 2>/dev/null || {
    git -C "$BASE_DIR" worktree prune; git -C "$BASE_DIR" branch -D "gate-iter${ITER}" 2>/dev/null
    git -C "$BASE_DIR" worktree add -q "$WT" -b "gate-iter${ITER}" HEAD; }

  # 3. Cheap fix agent: fed the EXACT gate findings + the SPEC. Narrow, falsifiable task.
  FIX_PROMPT="You are fixing a Go dependency-breakability analyzer. The acceptance gate FAILED.

SPEC (do not violate): $WT/.github/breakability/SPEC.md
GATE FINDINGS (each [P0]/[P1]/[P2] is a real defect to fix):
$GATE_OUT

The deterministic logic lives in: $WT/.github/scripts/build-check.sh, merge-results.sh, post-fallback-comments.sh
and the router $WT/.github/scripts/break_class_router.py.

ROOT CAUSES to fix (highest leverage first):
- INVENTED CITATION (P0): a PR claims break-reachability but nothing in the BUMPED MODULE imports it.
  Fix reachability to be MODULE-SCOPED: only count importers inside the go.mod being bumped. If
  files_importing is empty, the verdict MUST NOT claim reachability or tag High.
- FALSE_BLOCK (P2 noise): clean patch/minor bumps and CVE-fix bumps with build+test passing and no
  break-reachable symbol are being tagged 'review'. They should be auto_clear/Low per SPEC bucket rules.

RULES:
1. Edit only files under $WT/.github/. Do NOT hardcode PR numbers or package names (no repo-specific hacks).
2. After each edit run: bash -n on any .sh you touch.
3. Commit: cd $WT && git add -A && git commit -m 'gate-iter${ITER}: fix P0/P2 from gate'
4. Do NOT push.
"
  log "fix agent ($FIX_MODEL) ..."
  GH_TOKEN=$(gh auth token) "$COPILOT" -p "$FIX_PROMPT" --add-dir "$WT/.github" \
    --model "$FIX_MODEL" --yolo > "$BASE_DIR/gate_fix_iter${ITER}.md" 2>/dev/null || true

  if ! git -C "$WT" log --oneline -1 2>/dev/null | grep -q "gate-iter${ITER}"; then
    log "fix agent made no commit — rolling back worktree, stopping."
    git -C "$BASE_DIR" worktree remove --force "$WT" 2>/dev/null
    git -C "$BASE_DIR" branch -D "gate-iter${ITER}" 2>/dev/null
    break
  fi

  # 4. RE-GRADE the candidate fix BEFORE accepting it. (Requires regenerating $RESULTS by
  #    re-running the deterministic pipeline on the worktree — wire your build-check entrypoint here.)
  #    Until that entrypoint is wired, a human regenerates $RESULTS, then re-runs this loop.
  #    Guard: only merge if the new score strictly improves.
  log "candidate committed on gate-iter${ITER}. Regenerate $RESULTS from the worktree, then:"
  log "   NEW=\$(run_gate | awk -F': ' '/^SCORE:/{print \$2}')"
  log "   merge iff NEW > $SCORE, else: git -C $BASE_DIR reset --hard $PRE_TAG"

  # Conservative default: do NOT auto-merge until re-grade is wired. Keep branch for human review.
  PREV_SCORE="$SCORE"
  log "iteration paused for re-grade wiring (see TODO in loop). Branch gate-iter${ITER} left for inspection."
  break
done

log "loop exited. Inspect gate-result.json and gate_fix_iter*.md."
