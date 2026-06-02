#!/bin/bash
set -euo pipefail
cd /tmp/brk

MAX_ITERS=5
TARGET_SCORE=9.5
WORKFLOW="breakability-agent.yml"
BRANCH="staging/v7-improvements"
BASE_DIR="/tmp/brk"
WORKTREES_DIR="/tmp/brk-worktrees"
COPILOT="/Users/hpoornac/Library/Application Support/Code/User/globalStorage/github.copilot-chat/copilotCli/copilot"
REPO="CSC-Security-sandbox/vcp-vsa-breakability-test"

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

  # ── 2. Run end-user review (reviewer accesses PRs directly via gh CLI) ──
  log "Running end-user review..."
  REVIEW_PROMPT="You are a senior security engineer reviewing a CI tool that posts analysis comments on Dependabot PRs.

REPO: $REPO
GH_TOKEN is set — use 'gh pr view <N> --repo $REPO --json comments --jq .comments[-1].body' to read the latest comment on any PR.
Use 'gh issue list --repo $REPO --label merge-plan --state open --json number,body --jq .[0].body' to read the merge plan.

TASK:
1. Read comments on PRs: 1, 4, 5, 10, 12, 17, 19, 20, 22, 30, 38
2. Read the merge plan issue
3. Score the OVERALL quality 1-10 from an end-user developer perspective

EVALUATION CRITERIA (think as the developer receiving these comments):
- Can I TRUST these claims? Is there sufficient evidence/stdout shown for each assertion?
- Are security recommendations actionable and correct?
- Are verdicts (SAFE/RISK/REVIEW) accurate given the data?
- Is anything misleading, wrong, or missing that would cause a bad merge decision?
- Is the output noisy or useful? Would I ignore these comments?

OUTPUT FORMAT (machine-parseable):
SCORE: X.X
P0_COUNT: N
P1_COUNT: N
P2_COUNT: N
FINDINGS:
- [P0] PR#N or merge-plan | description | which script and what logic to change
- [P1] PR#N or merge-plan | description | fix location
- [P2] ...
END_FINDINGS

Be brutally honest. A checkmark emoji with no stdout proof is NOT evidence."

  GH_TOKEN=$(gh auth token) "$COPILOT" -p "$REVIEW_PROMPT" \
    --add-dir "$BASE_DIR/.github/scripts" \
    -s \
    --effort high \
    > "$BASE_DIR/review_iter${ITER}.md" 2>/dev/null || true
  log "Review complete"

  # ── 3. Extract score ──
  SCORE=$(grep "^SCORE:" "$BASE_DIR/review_iter${ITER}.md" | head -1 | sed 's/[^0-9.]//g' || echo "0")
  P0=$(grep "^P0_COUNT:" "$BASE_DIR/review_iter${ITER}.md" | head -1 | sed 's/[^0-9]//g' || echo "?")
  log "Score: $SCORE (P0=$P0) / target: $TARGET_SCORE"
  cat "$BASE_DIR/review_iter${ITER}.md" | tail -40

  if [[ -n "$SCORE" ]] && (( $(echo "$SCORE >= $TARGET_SCORE" | bc -l 2>/dev/null || echo 0) )); then
    log "TARGET REACHED: $SCORE >= $TARGET_SCORE"
    exit 0
  fi

  # ── 4. Create worktree for fixes ──
  log "Creating worktree for fixes..."
  rm -rf "$WORKTREE"
  git -C "$BASE_DIR" worktree add "$WORKTREE" -b "$ITER_BRANCH" HEAD 2>/dev/null || {
    git -C "$BASE_DIR" branch -D "$ITER_BRANCH" 2>/dev/null || true
    git -C "$BASE_DIR" worktree prune 2>/dev/null || true
    git -C "$BASE_DIR" worktree add "$WORKTREE" -b "$ITER_BRANCH" HEAD
  }

  # ── 5. Fix issues using copilot in worktree ──
  log "Fixing P0/P1 issues..."
  FIX_PROMPT="You are a senior engineer. Fix the bugs from this review:

$(cat "$BASE_DIR/review_iter${ITER}.md")

WORKING DIRECTORY: $WORKTREE/.github/scripts/
Only edit files there. The main scripts are:
- post-fallback-comments.sh (PR comment templates)
- merge-results.sh (data aggregation)
- build-check.sh (per-PR build/test analysis — captures stdout)

RULES:
1. Fix ALL P0 issues and as many P1 as possible
2. After EACH edit, run: bash -n <file> to validate syntax
3. Do NOT edit workflow YAML files
4. When done:
   cd $WORKTREE && bash -n .github/scripts/post-fallback-comments.sh && bash -n .github/scripts/merge-results.sh
   git add -A && git commit -m 'iter${ITER}: fix P0/P1 from review (score $SCORE)'
5. Do NOT push

KEY CONTEXT:
- build-check.sh saves test stdout to test.output_tail in JSON (up to 80 lines)
- build-check.sh saves build stdout to build.output_tail (up to 80 lines)
- post-fallback-comments.sh extracts these but only shows 8 lines in a collapsed block
- To show more evidence: increase what's extracted and restructure the 'How we checked' section
- The HOW_CHECKED templates are in the 'case \"\$VER_LABEL\" in' block around line 570"

  GH_TOKEN=$(gh auth token) "$COPILOT" -p "$FIX_PROMPT" \
    --add-dir "$WORKTREE/.github/scripts" \
    --yolo \
    --effort high \
    > "$BASE_DIR/fix_iter${ITER}.md" 2>/dev/null || true
  log "Fix agent done"
  tail -20 "$BASE_DIR/fix_iter${ITER}.md"

  # ── 6. Validate and merge fixes back ──
  if git -C "$WORKTREE" log --oneline -1 | grep -q "iter${ITER}"; then
    log "Fixes committed. Merging back..."
    cd "$BASE_DIR"
    git merge "$ITER_BRANCH" --no-edit
    git worktree remove "$WORKTREE" --force 2>/dev/null || true
    git branch -D "$ITER_BRANCH" 2>/dev/null || true
    log "Merged. HEAD: $(git rev-parse --short HEAD)"

    # ── 7. Push and trigger CI ──
    git push origin "$BRANCH"
    log "Pushed"
    gh workflow run "$WORKFLOW" --ref "$BRANCH"
    sleep 15
    log "CI triggered. Looping..."
  else
    log "WARNING: Fix agent did not commit. Skipping iteration."
    git -C "$BASE_DIR" worktree remove "$WORKTREE" --force 2>/dev/null || true
    git -C "$BASE_DIR" branch -D "$ITER_BRANCH" 2>/dev/null || true
  fi
done

log "Max iterations ($MAX_ITERS) reached. Latest score: $SCORE"
