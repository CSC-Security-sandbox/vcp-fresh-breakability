#!/bin/bash
set -euo pipefail
cd /tmp/brk

MAX_ITERS=5
TARGET_SCORE=9.5
WORKFLOW="breakability-agent.yml"
BRANCH="staging/v7-improvements"
BASE_DIR="/tmp/brk"
REVIEWS_DIR="/tmp/reviews"
WORKTREES_DIR="/tmp/brk-worktrees"
COPILOT="/Users/hpoornac/Library/Application Support/Code/User/globalStorage/github.copilot-chat/copilotCli/copilot"

log() { echo "$(date '+%H:%M:%S') [iter$ITER] $*" | tee -a /tmp/brk/loop.log; }

mkdir -p "$WORKTREES_DIR"

for ITER in $(seq 1 $MAX_ITERS); do
  log "=== ITERATION $ITER ==="
  ITER_BRANCH="iter${ITER}-fixes"
  ITER_TAG="iter${ITER}-pre"
  WORKTREE="$WORKTREES_DIR/iter${ITER}"

  # ── 0. Tag current state for rollback ──
  git -C "$BASE_DIR" tag -f "$ITER_TAG" HEAD
  log "Tagged $ITER_TAG at $(git -C "$BASE_DIR" rev-parse --short HEAD)"

  # ── 1. Wait for latest CI run ──
  RUN_ID=$(gh run list --workflow="$WORKFLOW" --branch "$BRANCH" -L 1 --json databaseId --jq '.[0].databaseId')
  log "Waiting for run $RUN_ID..."
  while true; do
    STATUS=$(gh run view "$RUN_ID" --json status --jq '.status' 2>/dev/null || echo "unknown")
    if [[ "$STATUS" == "completed" ]]; then
      CONCLUSION=$(gh run view "$RUN_ID" --json conclusion --jq '.conclusion')
      log "CI done: $CONCLUSION"
      break
    fi
    log "  status: $STATUS — sleeping 2min..."
    sleep 120
  done

  if [[ "$CONCLUSION" != "success" ]]; then
    log "ERROR: CI failed. Check run $RUN_ID. Stopping."
    exit 1
  fi

  # ── 2. Pull outputs ──
  log "Pulling outputs..."
  rm -rf "$REVIEWS_DIR" && mkdir -p "$REVIEWS_DIR"

  PLAN_ISSUE=$(gh issue list --state open --json number,title --jq '[.[] | select(.title | test("Merge Plan"; "i"))][0].number' 2>/dev/null || echo "")
  if [[ -z "$PLAN_ISSUE" ]]; then
    PLAN_ISSUE=$(gh issue list --label "merge-plan" --state open --json number --jq '.[0].number' 2>/dev/null || echo "62")
  fi
  log "Merge plan issue: #$PLAN_ISSUE"
  gh issue view "$PLAN_ISSUE" --json body --jq '.body' > "$REVIEWS_DIR/merge_plan.md"

  for PR in 1 4 5 10 12 17 19 20 22 30; do
    gh pr view $PR --json comments --jq '.comments[-1].body' > "$REVIEWS_DIR/pr${PR}.md" 2>/dev/null || true
  done
  log "Outputs saved"

  # ── 3. Run review with copilot CLI ──
  log "Running copilot review..."
  cat > "$REVIEWS_DIR/prompt_review.txt" << 'REVIEWEOF'
You are a senior security engineer reviewing CI tool outputs for a Dependabot PR analyzer.

Review ALL files in this directory (merge_plan.md, pr*.md). Score 1-10 overall.

Look for:
1. P0: Factual errors, wrong verdicts, dangerous merge advice, CVE misattribution
2. P1: Misleading info, inconsistent data between files, missing critical context
3. P2: Formatting bugs, duplicate content, template artifacts

Output in this EXACT format (machine-parseable):
SCORE: X.X
P0_COUNT: N
P1_COUNT: N
P2_COUNT: N
FIXES:
- [P0] filename|description of bug|suggested code fix in post-fallback-comments.sh or merge-results.sh
- [P1] filename|description|fix
...
END_FIXES

Be specific about which script and what logic to change. Reference line patterns or variable names.
REVIEWEOF

  "$COPILOT" -p "$(cat $REVIEWS_DIR/prompt_review.txt)" \
    --add-dir "$REVIEWS_DIR" \
    -s \
    --effort high \
    > "$REVIEWS_DIR/review_result.md" 2>/dev/null || true
  log "Review complete"
  cp "$REVIEWS_DIR/review_result.md" "$BASE_DIR/review_iter${ITER}.md"

  # ── 4. Extract score ──
  SCORE=$(grep "SCORE:" "$REVIEWS_DIR/review_result.md" | head -1 | sed 's/[^0-9.]//g' || echo "0")
  log "Score: $SCORE / target: $TARGET_SCORE"

  if [[ -n "$SCORE" ]] && (( $(echo "$SCORE >= $TARGET_SCORE" | bc -l 2>/dev/null || echo 0) )); then
    log "TARGET REACHED: $SCORE >= $TARGET_SCORE"
    cat "$REVIEWS_DIR/review_result.md"
    exit 0
  fi

  # ── 5. Create worktree for fixes (rollback-safe) ──
  log "Creating worktree for fixes..."
  rm -rf "$WORKTREE"
  git -C "$BASE_DIR" worktree add "$WORKTREE" -b "$ITER_BRANCH" HEAD 2>/dev/null || {
    git -C "$BASE_DIR" branch -D "$ITER_BRANCH" 2>/dev/null || true
    git -C "$BASE_DIR" worktree prune 2>/dev/null || true
    git -C "$BASE_DIR" worktree add "$WORKTREE" -b "$ITER_BRANCH" HEAD
  }
  log "Worktree at $WORKTREE (branch: $ITER_BRANCH)"

  # ── 6. Fix issues using copilot in worktree ──
  log "Fixing P0/P1 issues..."
  cat > "$REVIEWS_DIR/prompt_fix.txt" << FIXEOF
You are a senior engineer. Fix the bugs listed in $REVIEWS_DIR/review_result.md.

WORKING DIRECTORY: $WORKTREE/.github/scripts/
Only edit files there. The main scripts are:
- post-fallback-comments.sh (PR comments + merge plan generation)
- merge-results.sh (data aggregation from build results)
- build-check.sh (per-PR build analysis - avoid unless necessary)

RULES:
1. Fix ALL P0 issues and as many P1 issues as possible
2. After EACH file edit, run: bash -n <file> to validate syntax
3. Do NOT edit any YAML workflow files
4. When done, run these commands:
   cd $WORKTREE
   bash -n .github/scripts/post-fallback-comments.sh
   bash -n .github/scripts/merge-results.sh
   git add -A
   git commit -m 'iter${ITER}: fix P0/P1 from review (score $SCORE)'
5. Do NOT push - just commit locally

Key context:
- The merge plan is generated by an embedded Python heredoc in post-fallback-comments.sh (lines ~1150-1750)
- CVE attribution comes from merge-results.sh which matches Dependabot alerts to PRs
- go.sum line counts come from build-check.sh gosum_new_count field
- Exit code comparison is in build-check.sh error classification
- Double-hash ## bug is likely a string interpolation issue in post-fallback-comments.sh

Respond with a numbered list of what you fixed.
FIXEOF

  "$COPILOT" -p "$(cat $REVIEWS_DIR/prompt_fix.txt)" \
    --add-dir "$REVIEWS_DIR" \
    --add-dir "$WORKTREE" \
    --yolo \
    --effort high \
    > "$REVIEWS_DIR/fix_result.md" 2>/dev/null || true
  log "Fix agent done"
  tail -30 "$REVIEWS_DIR/fix_result.md"
  cp "$REVIEWS_DIR/fix_result.md" "$BASE_DIR/fix_iter${ITER}.md"

  # ── 7. Validate and merge fixes back ──
  if git -C "$WORKTREE" log --oneline -1 | grep -q "iter${ITER}"; then
    log "Fixes committed in worktree. Merging back..."
    cd "$BASE_DIR"
    git merge "$ITER_BRANCH" --no-edit
    git worktree remove "$WORKTREE" --force 2>/dev/null || true
    git branch -D "$ITER_BRANCH" 2>/dev/null || true
    log "Merged. HEAD now: $(git rev-parse --short HEAD)"

    # ── 8. Push and trigger CI ──
    git push origin "$BRANCH"
    log "Pushed"
    gh workflow run "$WORKFLOW" --ref "$BRANCH"
    sleep 15
    log "CI triggered. Looping to wait..."
  else
    log "WARNING: Fix agent did not commit. Skipping this iteration."
    git -C "$BASE_DIR" worktree remove "$WORKTREE" --force 2>/dev/null || true
    git -C "$BASE_DIR" branch -D "$ITER_BRANCH" 2>/dev/null || true
  fi
done

log "Max iterations ($MAX_ITERS) reached. Latest score: $SCORE"
log "Rollback tags available: $(git -C "$BASE_DIR" tag -l 'iter*-pre' | tr '\n' ' ')"
