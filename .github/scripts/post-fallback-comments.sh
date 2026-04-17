#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# post-fallback-comments.sh — Deterministic build analysis comments + merge plan
#
# PRIMARY comment system: posts structured build analysis comments for every PR
# and generates a merge plan issue. Comments are generated directly from the
# deterministic JSON results — no AI required. If the AI agent already posted
# richer comments (<!-- breakability-agent -->), those PRs are skipped.
# CR5-1/M3: This is the primary system, not a fallback. The AI agent is optional.
#
# Comment quality by scenario:
#   - Trivially safe (patch/transitive/actions/docker): brief SAFE one-liner
#   - Build pass with new errors or major bump: BUILD ANALYSIS with details
#   - Build fail: BUILD_FAILS with error excerpt and remediation note
#   - Build not run (skip/infra_error): REVIEW with infrastructure context
#   - Security fix: SECURITY FIX with severity and MERGE NOW recommendation
#   - Pre-existing (L1, zero new errors): LIKELY SAFE
# ──────────────────────────────────────────────────────────────────────────────
set -u

RESULTS_FILE="/tmp/build-results.json"

if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "No build-results.json found — nothing to do"
  exit 0
fi

OWNER_REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || echo "unknown/unknown")
OWNER="${OWNER_REPO%%/*}"
REPO="${OWNER_REPO##*/}"

POSTED=0
SKIPPED=0
FAILED=0

# Read advisory mode from results metadata
BC_MODE=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
print(data.get('metadata', {}).get('mode', 'advisory'))
" 2>/dev/null || echo "advisory")

ADVISORY_FOOTER=""
if [[ "$BC_MODE" == "advisory" ]]; then
  ADVISORY_FOOTER="
> 🔬 **Advisory mode** — This analysis is informational. No merges are blocked."
fi

echo "Posting deterministic analysis comments (mode: $BC_MODE)..."

# V8 FIX (C2): Detect discovered-but-not-analyzed PRs (cancelled batch timeout).
# Compare the discover list (all open Dependabot PRs) against the build-results.json.
# Post a "Skipped — batch was cancelled" comment for missing PRs.
echo "Checking for cancelled/missing PRs..."
DISCOVERED_PRS=$(gh pr list --label "dependencies" --state open \
  --json number --jq '.[].number' --limit 500 2>/dev/null | sort -n || echo "")
ANALYZED_PRS=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
for num in sorted(data.get('prs', {}).keys(), key=int):
    print(num)
" 2>/dev/null || echo "")

# Find PRs in discovered list but NOT in analyzed results
CANCELLED_PRS=""
for _disc_pr in $DISCOVERED_PRS; do
  _found=false
  for _anal_pr in $ANALYZED_PRS; do
    if [[ "$_disc_pr" == "$_anal_pr" ]]; then
      _found=true
      break
    fi
  done
  if [[ "$_found" == "false" ]]; then
    CANCELLED_PRS="$CANCELLED_PRS $_disc_pr"
  fi
done

# Post "Skipped" comments for cancelled PRs and add them to results JSON
for _CANCEL_PR in $CANCELLED_PRS; do
  [[ -z "$_CANCEL_PR" ]] && continue
  # Check if we already have a recent comment — don't spam
  _HAS_RECENT=$(gh api "repos/$OWNER/$REPO/issues/$_CANCEL_PR/comments" \
    --jq '[.[] | select(.body | contains("<!-- breakability-check -->")) | select(.body | contains("batch was cancelled"))] | length' \
    2>/dev/null || echo "0")
  if [[ "$_HAS_RECENT" -gt 0 ]]; then
    continue
  fi
  # Delete old deterministic comments before posting new one
  _OLD_IDS=$(gh api "repos/$OWNER/$REPO/issues/$_CANCEL_PR/comments" \
    --jq '.[] | select(.body | contains("<!-- breakability-check -->")) | select(.body | contains("<!-- breakability-agent -->") | not) | .id' \
    2>/dev/null || true)
  for _CID in $_OLD_IDS; do
    gh api -X DELETE "repos/$OWNER/$REPO/issues/comments/$_CID" 2>/dev/null || true
  done
  _CANCEL_TITLE=$(gh pr view "$_CANCEL_PR" --json title --jq '.title' 2>/dev/null || echo "Unknown")
  _CANCEL_COMMENT="<!-- breakability-check -->
## ⚠️ SKIPPED — Analysis incomplete (batch was cancelled)

This PR was discovered but the analysis batch was cancelled or timed out before it could be processed.

**What to do:** Re-run the analysis: \`gh workflow run breakability-agent.yml\`

> 🔬 *Deterministic analysis — batch incomplete*"
  gh pr comment "$_CANCEL_PR" --body "$_CANCEL_COMMENT" 2>/dev/null && \
    echo "  Posted 'cancelled' comment for PR #$_CANCEL_PR" || true

  # Add to results JSON so merge plan picks it up
  python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
data['prs']['$_CANCEL_PR'] = {
    'package': '$(echo "$_CANCEL_TITLE" | sed "s/'/\\\\'/g")',
    'from': '?', 'to': '?', 'ecosystem': 'unknown', 'bump': 'unknown',
    'dep_type': 'unknown', 'dep_relation': 'unknown', 'cves': [],
    'build': {'verdict': 'cancelled', 'main_exit': -1, 'pr_exit': -1,
              'output_tail': '', 'new_errors': [], 'install_method': 'none', 'error_class': ''},
    'test': {'ran': False, 'exit': None, 'output_tail': ''},
    'files_importing': [], 'pkg_dir': '/', 'install_ok': False,
    'verification_level': -1, 'verification_label': 'NA_cancelled',
    'verification_steps': [], 'skip_reason': 'batch cancelled/timed out'
}
with open('$RESULTS_FILE', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || true
done
if [[ -n "$CANCELLED_PRS" ]]; then
  echo "  Cancelled PRs:$CANCELLED_PRS"
else
  echo "  No cancelled PRs detected"
fi

# Get all PR numbers from build-results.json
PR_NUMBERS=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
for num in sorted(data.get('prs', {}).keys(), key=int):
    print(num)
")

for PR_NUM in $PR_NUMBERS; do
  # Per-PR atomic comment management (A3-9):
  # 1. Check for existing AI agent comments (preserve those)
  # 2. Delete old deterministic comments (<!-- breakability-check --> without <!-- breakability-agent -->)
  # 3. Post new deterministic comment
  # This avoids the race where merge-results.sh deletes comments before this script posts.
  HAS_AGENT_COMMENT=$(gh api "repos/$OWNER/$REPO/issues/$PR_NUM/comments" \
    --jq '[.[] | select(.body | contains("<!-- breakability-agent -->"))] | length' \
    2>/dev/null || echo "0")

  if [[ "$HAS_AGENT_COMMENT" -gt 0 ]]; then
    # AI agent already posted a richer comment — skip deterministic fallback
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  # Delete old deterministic comments for this PR (atomic: delete before posting new one)
  OLD_COMMENT_IDS=$(gh api "repos/$OWNER/$REPO/issues/$PR_NUM/comments" \
    --jq '.[] | select(.body | contains("<!-- breakability-check -->")) | select(.body | contains("<!-- breakability-agent -->") | not) | .id' \
    2>/dev/null || true)
  for CID in $OLD_COMMENT_IDS; do
    gh api -X DELETE "repos/$OWNER/$REPO/issues/comments/$CID" 2>/dev/null || true
  done

  # Extract all fields for this PR in one python call
  PR_FIELDS=$(python3 -c "
import json, sys
with open('$RESULTS_FILE') as f:
    data = json.load(f)
pr = data['prs'].get('$PR_NUM', {})
build = pr.get('build', {})
test = pr.get('test', {})
sec = data.get('security_posture', {}).get('prs_fixing_alerts', {}).get('$PR_NUM', {})
cve_severities = sec.get('severities', [])
cve_ids = sec.get('cve_ids', [])
print(json.dumps({
    'package':      pr.get('package', '?'),
    'from':         pr.get('from', '?'),
    'to':           pr.get('to', '?'),
    'bump':         pr.get('bump', '?'),
    'dep_type':     pr.get('dep_type', '?'),
    'dep_relation': pr.get('dep_relation', '?'),
    'ecosystem':    pr.get('ecosystem', '?'),
    'verdict':      build.get('verdict', '?'),
    'install_method': build.get('install_method', ''),
    'install_ok':   build.get('install_ok', False),
    'new_errors':   build.get('new_errors', []),
    'output_tail':  build.get('output_tail', ''),
    'test_ran':     test.get('ran', False),
    'test_exit':    test.get('exit', -1),
    'verification_label': pr.get('verification_label', ''),
    'files_importing': pr.get('files_importing', []),
    'cves':         pr.get('cves', []),
    'error_class':  build.get('error_class', ''),
    'pkg_dir':      pr.get('pkg_dir', '/'),
    'main_exit':    build.get('main_exit', -1),
    'cve_severities': cve_severities,
    'cve_ids':      cve_ids,
    'gosum_new_count': pr.get('gosum_new_count', 0),
}))
" 2>/dev/null || echo '{}')

  # Extract all fields in a single Python call instead of 13 separate spawns (CR2-7).
  # This reduces per-PR Python process spawns from 15 to 3 (fields + CVE extraction).
  _FIELDS_EXTRACTED=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
fields = {
    'PKG': d.get('package', '?'),
    'FROM': d.get('from', '?'),
    'TO': d.get('to', '?'),
    'BUMP': d.get('bump', '?'),
    'DEP_TYPE': d.get('dep_type', '?'),
    'DEP_REL': d.get('dep_relation', '?'),
    'ECOSYSTEM': d.get('ecosystem', '?'),
    'VERDICT': d.get('verdict', '?'),
    'INSTALL_METHOD': d.get('install_method', ''),
    'INSTALL_OK': str(d.get('install_ok', False)),
    'VER_LABEL': d.get('verification_label', ''),
    'NEW_ERR_COUNT': str(len(d.get('new_errors', []))),
    'FILES_COUNT': str(len(d.get('files_importing', []))),
    'PKG_DIR': d.get('pkg_dir', '/'),
    'ERROR_CLASS': d.get('error_class', ''),
    'OOM_OVERRIDE': str(d.get('oom_override', False)),
    'OOM_PACKAGES': ','.join(d.get('oom_packages', [])),
    'GOSUM_NEW_COUNT': str(d.get('gosum_new_count', 0)),
    'FILES_LIST': '|'.join((f.split(':')[0] if ':' in f else f) for f in d.get('files_importing', [])[:8]),
}
for k, v in fields.items():
    # Use null byte as delimiter to safely handle any value content
    print(f'{k}={v}')
" 2>/dev/null || echo "")
  # Parse the output into shell variables
  PKG=$(echo "$_FIELDS_EXTRACTED" | grep '^PKG=' | cut -d= -f2-)
  FROM=$(echo "$_FIELDS_EXTRACTED" | grep '^FROM=' | cut -d= -f2-)
  TO=$(echo "$_FIELDS_EXTRACTED" | grep '^TO=' | cut -d= -f2-)
  BUMP=$(echo "$_FIELDS_EXTRACTED" | grep '^BUMP=' | cut -d= -f2-)
  # 0.x semver: only flag 0.x major bumps, not real v1→v2 upgrades
  FROM_MAJOR="${FROM%%.*}"
  FROM_MAJOR="${FROM_MAJOR#v}"
  if [[ "$BUMP" == "major" && "$FROM_MAJOR" == "0" ]]; then
    BUMP_DISPLAY="major ⚠️ (0.x unstable — treat as breaking)"
  else
    BUMP_DISPLAY="$BUMP"
  fi
  DEP_TYPE=$(echo "$_FIELDS_EXTRACTED" | grep '^DEP_TYPE=' | cut -d= -f2-)
  DEP_REL=$(echo "$_FIELDS_EXTRACTED" | grep '^DEP_REL=' | cut -d= -f2-)
  ECOSYSTEM=$(echo "$_FIELDS_EXTRACTED" | grep '^ECOSYSTEM=' | cut -d= -f2-)
  VERDICT=$(echo "$_FIELDS_EXTRACTED" | grep '^VERDICT=' | cut -d= -f2-)
  INSTALL_METHOD=$(echo "$_FIELDS_EXTRACTED" | grep '^INSTALL_METHOD=' | cut -d= -f2-)
  INSTALL_OK=$(echo "$_FIELDS_EXTRACTED" | grep '^INSTALL_OK=' | cut -d= -f2-)
  VER_LABEL=$(echo "$_FIELDS_EXTRACTED" | grep '^VER_LABEL=' | cut -d= -f2-)
  NEW_ERR_COUNT=$(echo "$_FIELDS_EXTRACTED" | grep '^NEW_ERR_COUNT=' | cut -d= -f2-)
  FILES_COUNT=$(echo "$_FIELDS_EXTRACTED" | grep '^FILES_COUNT=' | cut -d= -f2-)
  PKG_DIR=$(echo "$_FIELDS_EXTRACTED" | grep '^PKG_DIR=' | cut -d= -f2-)
  ERROR_CLASS=$(echo "$_FIELDS_EXTRACTED" | grep '^ERROR_CLASS=' | cut -d= -f2-)
  OOM_OVERRIDE=$(echo "$_FIELDS_EXTRACTED" | grep '^OOM_OVERRIDE=' | cut -d= -f2-)
  OOM_PACKAGES=$(echo "$_FIELDS_EXTRACTED" | grep '^OOM_PACKAGES=' | cut -d= -f2-)
  GOSUM_NEW_COUNT=$(echo "$_FIELDS_EXTRACTED" | grep '^GOSUM_NEW_COUNT=' | cut -d= -f2-)
  FILES_LIST=$(echo "$_FIELDS_EXTRACTED" | grep '^FILES_LIST=' | cut -d= -f2-)

  # CVE extraction — core security data
  CVE_LIST=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
cves = d.get('cves', [])
if cves:
    print(','.join(cves))
else:
    print('')
" 2>/dev/null || echo "")
  CVE_COUNT=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('cves',[])))")

  # Extract CVE severity info (end-user P1: make CVEs impossible to miss)
  CVE_MAX_SEVERITY=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
sevs = d.get('cve_severities', [])
if not sevs:
    print('')
else:
    order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    worst = min(sevs, key=lambda s: order.get(s.lower(), 99))
    print(worst.upper())
" 2>/dev/null || echo "")

  # V8 FIX: Build enriched security line with severity, CVSS, and advisory links
  CVE_LINE=""
  CVE_DETAIL_BLOCK=""
  if [[ "$CVE_COUNT" -gt 0 && "$CVE_COUNT" != "0" ]]; then
    # Build per-CVE detail lines with severity and advisory link
    CVE_DETAIL_BLOCK=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
# Look up cve_details from build-results.json (richer than PR_FIELDS inline)
pr_num = '$PR_NUM'
try:
    with open('/tmp/build-results.json') as f:
        results = json.load(f)
    details = results.get('prs', {}).get(pr_num, {}).get('cve_details', [])
except:
    details = []
if not details:
    sys.exit(0)
for det in details:
    _id = det.get('id', '?')
    sev = det.get('severity', 'unknown').upper()
    cvss = det.get('cvss_score')
    summary = det.get('summary', '')
    url = det.get('advisory_url', '')
    line = f'- **{_id}** ({sev}'
    if cvss:
        line += f', CVSS {cvss}'
    line += ')'
    if summary:
        line += f': {summary}'
    if url:
        line += f' — [advisory]({url})'
    print(line)
" 2>/dev/null || echo "")

    if [[ -n "$CVE_MAX_SEVERITY" ]]; then
      CVE_LINE="
🔴 **Security ($CVE_MAX_SEVERITY): $CVE_COUNT CVE(s) fixed by this upgrade:**"
    else
      CVE_LINE="
🔴 **Security: $CVE_COUNT CVE(s) fixed by this upgrade:** $CVE_LIST"
    fi
    if [[ -n "$CVE_DETAIL_BLOCK" ]]; then
      CVE_LINE="$CVE_LINE
$CVE_DETAIL_BLOCK"
    else
      CVE_LINE="$CVE_LINE $CVE_LIST"
    fi
  fi

  # Module line for monorepo context (end-user feedback: which module does this affect?)
  MODULE_LINE=""
  if [[ "$ECOSYSTEM" == "gomod" && "$PKG_DIR" != "/" && -n "$PKG_DIR" ]]; then
    MODULE_LINE=" · Module: \`$PKG_DIR\`"
  fi

  # Build "How we checked" checklist from verification_label
  # Build file-list detail block for evidence
  _FILES_DETAIL_BLOCK=""
  if [[ -n "$FILES_LIST" && "$FILES_LIST" != "" ]]; then
    _FILES_DETAIL_BLOCK="
<details><summary>📂 Files importing this package ($FILES_COUNT file(s))</summary>

$(echo "$FILES_LIST" | tr '|' '\n' | sed 's/^/- `/' | sed 's/$/`/')
</details>"
  fi
  # Build transitive dep note
  _TRANSITIVE_NOTE=""
  if [[ -n "$GOSUM_NEW_COUNT" && "$GOSUM_NEW_COUNT" -gt 0 ]]; then
    _TRANSITIVE_NOTE="
- ℹ️ go.sum: $GOSUM_NEW_COUNT new transitive dep entries (run \`govulncheck ./...\` if concerned)"
  fi
  # Build build-stdout evidence block
  _BUILD_STDOUT_BLOCK=""
  _BUILD_STDOUT_SNIPPET=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
tail = d.get('output_tail', '')
# Show last 8 non-empty lines of build output as evidence
lines = [l for l in tail.splitlines() if l.strip()][-8:]
if lines:
    print('\n'.join(lines))
" 2>/dev/null || true)
  if [[ -n "$_BUILD_STDOUT_SNIPPET" ]]; then
    _BUILD_STDOUT_BLOCK="
<details><summary>🖥️ Build output (last lines)</summary>

\`\`\`
${_BUILD_STDOUT_SNIPPET}
\`\`\`
</details>"
  fi
  HOW_CHECKED=""
  case "$VER_LABEL" in
    L4*)
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ✅ Automated tests pass
- ✅ No new errors introduced vs. main${_TRANSITIVE_NOTE}
</details>${_FILES_DETAIL_BLOCK}${_BUILD_STDOUT_BLOCK}"
      ;;
    L3*)
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ⬜ Tests not configured or not run
- ✅ No new errors introduced vs. main${_TRANSITIVE_NOTE}
</details>${_FILES_DETAIL_BLOCK}${_BUILD_STDOUT_BLOCK}"
      ;;
    L2*)
      # V9.3 FIX (P1-2): BUILD_FAILS PRs must NOT use the "builds clean" checklist.
      # Split on verdict: fail gets a failure-specific checklist, pass/pre_existing gets the original.
      if [[ "$VERDICT" == "fail" || "$VERDICT" == "pre_existing_plus_new" ]]; then
        # Build failed — show failure-specific checklist
        if [[ "$OOM_OVERRIDE" == "True" ]]; then
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ⚙️ Build hit OOM (\`signal: killed\`) on unrelated sub-packages — not caused by this upgrade
- ✅ PR's targeted packages are not affected
- ✅ No new type errors introduced vs. main
</details>"
        else
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ❌ Project build fails on PR branch
- ✅ Build passes on main — errors are introduced by this upgrade
- ⬜ Tests not run (build must pass first)
</details>"
        fi
      else
        # Build passed — original L2 checklist
        TEST_EXIT_RAW=$(echo "$PR_FIELDS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('test_exit',-1))" 2>/dev/null || echo "-1")
        TEST_RAN_RAW=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('test_ran',False))" 2>/dev/null || echo "False")
        if [[ "$TEST_RAN_RAW" == "True" && "$TEST_EXIT_RAW" != "0" && "$TEST_EXIT_RAW" != "-1" ]]; then
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ⚙️ Automated tests fail (exit=$TEST_EXIT_RAW — pre-existing, same failure on main)
- ✅ No new build errors introduced vs. main${_TRANSITIVE_NOTE}
</details>${_FILES_DETAIL_BLOCK}${_BUILD_STDOUT_BLOCK}"
        else
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ✅ Project builds / type-checks clean
- ⬜ Tests not configured or not run
- ✅ No new build errors introduced vs. main${_TRANSITIVE_NOTE}
</details>${_FILES_DETAIL_BLOCK}${_BUILD_STDOUT_BLOCK}"
        fi
      fi
      ;;
    L1*)
      # V8 FIX (C3): L1 comments must include WHAT failed and WHERE, not just
      # "Build verification limited". Extract module and error excerpt from build output.
      _L1_MAIN_EXIT=$(echo "$PR_FIELDS" | python3 -c "import json,sys; print(json.load(sys.stdin).get('main_exit',-1))" 2>/dev/null || echo "-1")
      # V9.3: Enhanced excerpt + module attribution for OOM errors.
      # Identifies which sub-packages had errors and whether they're related to the PR's package.
      _L1_EXCERPT_AND_ATTR=$(echo "$PR_FIELDS" | _BC_PKG="$PKG" python3 -c "
import json, sys, os, re
d = json.load(sys.stdin)
tail = d.get('output_tail', '')
pkg = os.environ.get('_BC_PKG', '')
error_class = d.get('error_class', '')
# Extract error/fail lines for context
lines = [l.strip() for l in tail.splitlines()
         if any(k in l.lower() for k in ('error', 'fail', 'cannot', 'undefined', 'fatal', 'signal: kill', 'killed'))][:6]
if not lines:
    non_empty = [l.strip() for l in tail.splitlines() if l.strip()][-4:]
    lines = non_empty
# Extract OOM-killed package names for attribution
killed_pkgs = []
for line in tail.splitlines():
    if 'signal: killed' in line.lower() or 'signal: kill' in line.lower():
        m = re.match(r'^(\S+?):\s', line)
        if m:
            killed_pkgs.append(m.group(1))
# Build attribution note
attr = ''
if killed_pkgs and error_class == 'resource_exhaustion':
    short_names = [p.split('/')[-1] if '/' in p else p for p in killed_pkgs[:3]]
    attr = 'OOM_PKGS=' + ','.join(killed_pkgs[:3])
    # Check if any killed package relates to PR's package
    related = any(pkg.lower() in kp.lower() for kp in killed_pkgs) if pkg else False
    if not related:
        attr += '|UNRELATED'
    else:
        attr += '|RELATED'
# Output: first line is attribution metadata, rest is excerpt
print(attr)
print('---EXCERPT---')
print('\n'.join(lines) if lines else '')
" 2>/dev/null || echo "")
      _L1_ATTR=$(echo "$_L1_EXCERPT_AND_ATTR" | head -1)
      _L1_EXCERPT=$(echo "$_L1_EXCERPT_AND_ATTR" | sed '1,/^---EXCERPT---$/d')
      _L1_EXCERPT_BLOCK=""
      if [[ -n "$_L1_EXCERPT" ]]; then
        _L1_EXCERPT_BLOCK="
\`\`\`
${_L1_EXCERPT}
\`\`\`"
      fi
      _L1_MODULE_NOTE=""
      if [[ -n "$PKG_DIR" && "$PKG_DIR" != "/" ]]; then
        _L1_MODULE_NOTE="
- PR targets module: \`$PKG_DIR\`"
      fi
      # V9.3: Add OOM attribution note if errors are in unrelated sub-packages
      _L1_OOM_NOTE=""
      if echo "$_L1_ATTR" | grep -q "OOM_PKGS="; then
        _OOM_PKG_NAMES=$(echo "$_L1_ATTR" | sed 's/OOM_PKGS=//;s/|.*//')
        _OOM_PKG_SHORT=$(echo "$_OOM_PKG_NAMES" | tr ',' '\n' | while read -r p; do echo "$p" | rev | cut -d/ -f1 | rev; done | tr '\n' ',' | sed 's/,$//')
        if echo "$_L1_ATTR" | grep -q "UNRELATED"; then
          _L1_OOM_NOTE="
- ⚙️ Pre-existing failure is in \`${_OOM_PKG_SHORT}\` — **unrelated** to this PR's package (\`$PKG\`)"
        fi
      fi
      HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ✅ Dependency resolved successfully
- ⚠️ Build fails on both \`main\` (exit=${_L1_MAIN_EXIT}) and PR branch — same errors${_L1_MODULE_NOTE}${_L1_OOM_NOTE}
- ✅ No NEW errors introduced by this upgrade

**Pre-existing build errors:**${_L1_EXCERPT_BLOCK}

Fix these on \`main\` to unlock full L2+ verification.
</details>"
      ;;
    *)
      if [[ -n "$VER_LABEL" ]]; then
        # End-user feedback: "Limited verification performed" is misleading when the
        # tool DID compare builds and found zero new errors. Show what we actually did.
        if [[ "$VERDICT" == "pre_existing" && "$NEW_ERR_COUNT" -eq 0 ]]; then
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ⚙️ Built both \`main\` and PR branch
- ✅ Compared build errors — **zero new errors** from this upgrade
- ⚠️ Baseline build has pre-existing failures (not caused by upgrade)
</details>"
        else
          HOW_CHECKED="
<details><summary>🔍 How we checked (verification: $VER_LABEL)</summary>

- ⬜ Build verification limited by infrastructure issues
</details>"
        fi
      fi
      ;;
  esac

  # Excerpt of build output (first 10 lines of errors for context)
  BUILD_EXCERPT=$(echo "$PR_FIELDS" | python3 -c "
import json, sys
d = json.load(sys.stdin)
tail = d.get('output_tail', '')
lines = [l for l in tail.splitlines() if 'error' in l.lower() or 'Error' in l][:8]
if lines:
    print('\n'.join(lines))
else:
    print(tail[:300] if tail else '')
" 2>/dev/null || echo "")

  # Find merge plan issue number
  MERGE_PLAN_NUM=$(gh issue list --label "merge-plan" --state open --json number -q '.[0].number' 2>/dev/null || echo "")
  PLAN_LINE=""
  if [[ -n "$MERGE_PLAN_NUM" ]]; then
    PLAN_LINE="
📋 Merge plan: #$MERGE_PLAN_NUM"
  fi

  # ── Classify and generate comment ─────────────────────────────────────────
  COMMENT=""

  # PRs with breakability:skip label were intentionally excluded from analysis.
  # Don't post any comment — the developer already opted out of analysis.
  if [[ "$VERDICT" == "skipped" ]]; then
    echo "  PR #$PR_NUM has breakability:skip label — skipping fallback comment"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  # V8 FIX (C2): Cancelled PRs already have a comment posted above
  if [[ "$VERDICT" == "cancelled" ]]; then
    echo "  PR #$PR_NUM was cancelled — comment already posted"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  if [[ "$ECOSYSTEM" == "actions" ]]; then
    # GitHub Actions — always safe, no app code affected.
    # No L0/fallback labels — CI-only changes need no build verification (end-user feedback 2.4).
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · dev (CI) · $BUMP_DISPLAY

GitHub Actions workflow dependency. No application code affected. No build verification needed.${CVE_LINE}${PLAN_LINE}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — CI-only change, no build impact*"

  elif [[ "$ECOSYSTEM" == "docker" && "$BUMP" != "major" ]]; then
    # Docker non-major — typically safe
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · production · $BUMP_DISPLAY

Docker base image $BUMP_DISPLAY bump. No application source changes.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "pass" && "$OOM_OVERRIDE" == "True" ]]; then
    # V9.3: OOM override — build was killed by OOM on unrelated sub-packages.
    # The PR's own targeted packages were not affected.
    _OOM_PKG_NOTE=""
    if [[ -n "$OOM_PACKAGES" ]]; then
      _OOM_PKG_LIST=$(echo "$OOM_PACKAGES" | tr ',' '\n' | sed 's/^/  - /' | head -5)
      _OOM_PKG_NOTE="

OOM-killed sub-packages (unrelated to this upgrade):
\`\`\`
${_OOM_PKG_LIST}
\`\`\`"
    fi
    _DEV_DEP_NOTE=""
    if [[ "$FILES_COUNT" -eq 0 ]]; then
      _DEV_DEP_NOTE=" · ⚙️ 0 direct imports (dev/indirect dependency)"
    fi
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ✅ infra OOM on unrelated sub-packages — not caused by this upgrade · Verification: **${VER_LABEL:-L2}** · Usage: $FILES_COUNT file(s)${MODULE_LINE}${_DEV_DEP_NOTE}${CVE_LINE}

### What this means
The CI runner ran out of memory (\`signal: killed\`) building sub-packages unrelated to \`$PKG\`. This PR's targeted packages are not affected. The same OOM occurs on \`main\` — it is an infrastructure limitation, not a code regression.${_OOM_PKG_NOTE}

**Recommendation:** Safe to merge. The OOM is a CI runner memory issue, not caused by this $BUMP_DISPLAY bump.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "pass" && "$BUMP" == "patch" && "$FILES_COUNT" -lt 5 ]]; then
    # Patch bump, build passes, low usage surface — simple safe
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · patch

Build: ✅ passes · Verification: **${VER_LABEL:-L1}** · Usage: $FILES_COUNT file(s)${MODULE_LINE}

$BUMP_DISPLAY bump with passing build. No new type errors introduced.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "pass" && "$DEP_REL" == "transitive" ]]; then
    # Transitive dep, build passes
    COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · transitive · $BUMP_DISPLAY

Build: ✅ passes · Verification: **${VER_LABEL:-L1}**

Transitive dependency — your code does not import it directly. Build passes.${CVE_LINE}${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "pass" ]]; then
    # Build passes — general case
    NEW_ERR_NOTE=""
    if [[ "$NEW_ERR_COUNT" -gt 0 ]]; then
      NEW_ERR_NOTE=" · ⚠️ $NEW_ERR_COUNT new error(s) found"
    fi
    COMMENT="<!-- breakability-check -->
## 🔍 BUILD ANALYSIS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ✅ passes · Verification: **${VER_LABEL:-L1}** · Usage: $FILES_COUNT file(s)${MODULE_LINE}$NEW_ERR_NOTE${CVE_LINE}

### Summary (deterministic analysis)
- Package: \`$PKG\` $FROM → $TO ($BUMP_DISPLAY bump)
- Type: $DEP_TYPE / $DEP_REL
- Build passes on PR branch
- New type errors: $NEW_ERR_COUNT

**Recommendation:** Review changelog for $BUMP_DISPLAY bump breaking changes. Build passes — merge when ready.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "fail" ]]; then
    # Build fails — most important fallback to get right
    EXCERPT_BLOCK=""
    if [[ -n "$BUILD_EXCERPT" ]]; then
      EXCERPT_BLOCK="
\`\`\`
${BUILD_EXCERPT}
\`\`\`"
    fi
    COMMENT="<!-- breakability-check -->
## ❌ BUILD_FAILS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ❌ fails on PR branch, ✅ passes on main · Usage: $FILES_COUNT file(s)${CVE_LINE}

### Build errors (excerpt)$EXCERPT_BLOCK

### What to do
1. Check the full build output in the Actions run for this PR
2. Review the \`$PKG\` $FROM → $TO changelog for breaking changes
3. Fix type errors or update your code to match the new API
4. Re-run the breakability analysis after your fix

**Do not merge — build is broken.** ($BUMP_DISPLAY bump)${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "pre_existing" ]]; then
    # Pre-existing failures — split on verification level (Finding-3.3, A2-8).
    # L2+ means tsc/go-build actually passed (identical errors = no new problems) → SAFE.
    # L1 means deps resolved but build inconclusive → LIKELY SAFE.
    # L0 means deps didn't even resolve → UNVERIFIED (do NOT say "LIKELY SAFE").
    if [[ "$VER_LABEL" == L2* || "$VER_LABEL" == L3* || "$VER_LABEL" == L4* || "$VER_LABEL" == L5* ]]; then
      COMMENT="<!-- breakability-check -->
## ✅ SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ✅ verified — same result as main baseline, not caused by this change · Verification: **${VER_LABEL}** · Usage: $FILES_COUNT file(s)${MODULE_LINE}${CVE_LINE}

### What this means
The build produces the same errors on both \`main\` and this PR branch. This upgrade does **not** introduce new failures. Verified at **${VER_LABEL}**.

**Recommendation:** Safe to merge. Pre-existing build issues are unrelated to this upgrade.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
    elif [[ "$VER_LABEL" == L1* ]]; then
      # L1: dependency resolution passed but build/type-check inconclusive
      COMMENT="<!-- breakability-check -->
## ⚙️ LIKELY SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ⚙️ same errors on main and PR branch — pre-existing failure, **not caused by this upgrade** · Verification: **${VER_LABEL}**${MODULE_LINE}${CVE_LINE}

### What this means
Dependencies resolved successfully. The build fails on both \`main\` and this PR with the same errors. This upgrade does **not** introduce new failures. Full build verification was limited by pre-existing issues on \`main\`.

**Recommendation:** Likely safe to merge — no new errors detected. Fix pre-existing build failures on \`main\` for full verification coverage.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
    else
      # L0: dependency resolution failed or build inconclusive.
      # End-user feedback (P1): "UNVERIFIED" is misleading when the tool DID compare
      # both branches and found zero new errors. That IS a safety signal.
      # Use "LIKELY SAFE" when zero new errors detected, "UNVERIFIED" only when
      # we truly have no signal (e.g., install_ok=false with no comparison done).
      if [[ "$NEW_ERR_COUNT" -eq 0 ]]; then
        COMMENT="<!-- breakability-check -->
## ⚙️ LIKELY SAFE — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ⚙️ same errors on \`main\` and PR branch — **not caused by this upgrade** · Verification: **${VER_LABEL:-L0}**${MODULE_LINE}${CVE_LINE}

### What this means
Both \`main\` and this PR branch produce the same build errors. This upgrade does **not** introduce new failures. Build verification was limited by pre-existing infrastructure issues.

**Recommendation:** Likely safe to merge — zero new errors detected. Fix baseline build on \`main\` for full verification.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
      else
        COMMENT="<!-- breakability-check -->
## ⚠️ UNVERIFIED — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ⚠️ build verification could not complete — infrastructure/configuration errors · Verification: **${VER_LABEL:-L0}**${MODULE_LINE}${CVE_LINE}

### What to do
1. Fix the baseline build on \`main\` (see merge plan for error details)
2. Re-run analysis: \`gh workflow run breakability-agent.yml\`

**Recommendation:** Cannot confirm safety. Fix build environment first, then re-analyze.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
      fi
    fi

  elif [[ "$VERDICT" == "pre_existing_plus_new" ]]; then
    # Pre-existing + new errors
    COMMENT="<!-- breakability-check -->
## ❌ BUILD_FAILS — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ❌ new errors introduced by this PR (on top of pre-existing failures)${CVE_LINE}

This upgrade introduces **$NEW_ERR_COUNT new error(s)** not present on \`main\`. Fix required before merging.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "security_review" ]]; then
    # Build passes but npm audit found CRITICAL/HIGH vulnerabilities
    COMMENT="<!-- breakability-check -->
## ⚠️ SECURITY REVIEW — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ✅ passes · Verification: **${VER_LABEL:-L1}** · Usage: $FILES_COUNT file(s)${CVE_LINE}

### Security concern
Build passes, but \`npm audit\` found **critical or high** vulnerabilities in this upgrade. Manual security review recommended before merging.

**Recommendation:** Review the npm audit output and CVE details. If vulnerabilities are in transitive deps not used by your code, merge may still be safe.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$INSTALL_METHOD" == "infra_error" ]]; then
    # Infrastructure blocked analysis
    COMMENT="<!-- breakability-check -->
## 🔍 REVIEW — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build: ⚠️ blocked by infrastructure error — build verification could not run${CVE_LINE}

### What happened
The build check was blocked by an infrastructure issue (private registry, network timeout, or missing dependency not caused by this upgrade). **This is not a build failure from the upgrade.**

**Recommendation:** Verify infrastructure health, then re-run. If infrastructure is healthy, review manually.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  elif [[ "$VERDICT" == "conflict" ]]; then
    # Conflicted PR — cannot merge or analyze until rebased (Finding-3.6)
    COMMENT="<!-- breakability-check -->
## ⚠️ CONFLICTED — \`$PKG\` $FROM → $TO — rebase required

This PR has merge conflicts and cannot be merged or analyzed until rebased.
Run \`@dependabot recreate\` or rebase manually.${PLAN_LINE}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"

  else
    # Catch-all: skip/unknown verdict
    COMMENT="<!-- breakability-check -->
## 🔍 REVIEW — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

Build analysis status: \`$VERDICT\` (verification: ${VER_LABEL:-unknown})${CVE_LINE}

Automated build analysis was not conclusive for this PR. Manual review recommended.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
  fi

  # CVE override: when a PR fixes CVEs, escalate the presentation regardless of verdict.
  # CR5-8: Fire for ALL verdicts, not just pass/pre_existing. A HIGH CVE must be visually
  # distinct and recommend immediate merge even when the build has issues. The developer
  # needs to know: "This PR fixes a HIGH CVE but may also need build fixes."
  # End-user feedback: PR #10 (HIGH CVE) was indistinguishable from 27 other PRs.
  if [[ "$CVE_COUNT" -gt 0 && "$CVE_COUNT" != "0" ]]; then
    _SEV_BADGE=""
    if [[ -n "$CVE_MAX_SEVERITY" ]]; then
      _SEV_BADGE=" ($CVE_MAX_SEVERITY)"
    fi
    if [[ "$NEW_ERR_COUNT" -eq 0 ]]; then
      # No new errors — recommend immediate merge regardless of baseline state
      COMMENT="<!-- breakability-check -->
## 🔴 SECURITY FIX${_SEV_BADGE} — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

### ⚠️ $CVE_COUNT CVE(s) resolved: $CVE_LIST
$(if [[ -n "$CVE_MAX_SEVERITY" ]]; then echo "**Severity: ${CVE_MAX_SEVERITY}** — This PR fixes a known security vulnerability."; fi)

**Build Impact:** No new errors introduced by this upgrade.${MODULE_LINE}
$(if [[ "$VERDICT" == "pre_existing" ]]; then echo "Baseline build has pre-existing failures (not related to this package)."; elif [[ "$VERDICT" == "pass" ]]; then echo "Build passes on PR branch."; else echo "Build status: \`$VERDICT\` — no new errors detected."; fi)

### Recommendation
**MERGE THIS PR IMMEDIATELY.** It resolves $CVE_COUNT known CVE(s) and introduces zero new build errors.
Security fixes should be prioritized over routine dependency upgrades.
$(if [[ "$VERDICT" == "pre_existing" && "$VER_LABEL" == L0* ]]; then echo "> If baseline build failures concern you, verify locally before merging. The security fix is independent of the baseline issue."; fi)

Verification: **${VER_LABEL:-L0}**${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
    else
      # Has new errors BUT also fixes CVEs — show both facts prominently
      COMMENT="<!-- breakability-check -->
## 🔴 SECURITY FIX (BUILD ISSUES)${_SEV_BADGE} — \`$PKG\` $FROM → $TO · $DEP_TYPE · $BUMP_DISPLAY

### ⚠️ $CVE_COUNT CVE(s) resolved: $CVE_LIST
$(if [[ -n "$CVE_MAX_SEVERITY" ]]; then echo "**Severity: ${CVE_MAX_SEVERITY}** — This PR fixes a known security vulnerability."; fi)

**Build Impact:** ❌ $NEW_ERR_COUNT new error(s) introduced by this upgrade.${MODULE_LINE}

### Recommendation
**This PR fixes a $CVE_MAX_SEVERITY CVE but also introduces build errors.** Fix the build errors, then merge immediately.
Do not delay — the security fix is critical.${PLAN_LINE}${HOW_CHECKED}${ADVISORY_FOOTER}
> 🔬 *Deterministic analysis — based on build comparison of main vs PR branch*"
    fi
  fi

  if [[ -n "$COMMENT" ]]; then
    if gh pr comment "$PR_NUM" --body "$COMMENT" 2>/dev/null; then
      echo "  Posted comment for PR #$PR_NUM ($PKG ${FROM}→${TO}, $VERDICT)"
      POSTED=$((POSTED + 1))
    else
      echo "  ⚠️  Failed to post comment for PR #$PR_NUM"
      FAILED=$((FAILED + 1))
    fi
  fi
done

echo ""
echo "Deterministic comments: posted $POSTED, skipped $SKIPPED (AI agent already commented), failed $FAILED"

# ── Regenerate merge plan issue ──────────────────────────────────────────────
# The merge plan must always reflect the latest build-results.json.
# If the AI agent generated a previous plan, it may be stale after a
# deterministic rerun. This section creates/updates the merge plan issue
# from the current data so PR comments and the plan never contradict.
echo ""
echo "════════════ MERGE PLAN ════════════"

MERGE_PLAN_BODY=$(python3 << 'PYEOF'
import json, sys, subprocess
from datetime import datetime, timezone

with open("/tmp/build-results.json") as f:
    data = json.load(f)

prs = data.get("prs", {})
meta = data.get("metadata", {})
cross = data.get("cross_pr_deps", [])
security = data.get("security_posture", {})

# Count total open PRs (not just Dependabot) for completeness note
try:
    result = subprocess.run(
        ["gh", "pr", "list", "--state", "open", "--json", "number", "-q", "length"],
        capture_output=True, text=True, timeout=30
    )
    total_open_prs = int(result.stdout.strip()) if result.returncode == 0 else 0
except (Exception,):
    total_open_prs = 0

non_dependabot_count = max(0, total_open_prs - len(prs))

# Display helper: 0.x semver versions may contain breaking changes
def fmt_bump(bump, from_ver=""):
    """Format bump type for display. Only flags 0.x major bumps, not real v1→v2."""
    if bump == "major":
        fv = from_ver.lstrip("v").split(".")[0] if from_ver else ""
        if fv == "0":
            return "major ⚠️ (0.x unstable)"
        return "major"
    return bump

# Categorize PRs
safe = []        # pass verdicts + pre_existing with L2+ verification
blocked = []     # fail / pre_existing_plus_new
review = []      # pre_existing (unverified) / error / infra_error
skipped = []     # skip (breakability:skip label)
ci_only = []     # V8 FIX (H3): Actions/Docker PRs — no build verification needed
not_analyzed = []  # PRs from cancelled/incomplete batches
cancelled = []   # V8 FIX (C2): discovered but not in results

for num, pr in sorted(prs.items(), key=lambda x: int(x[0])):
    v = pr.get("build", {}).get("verdict", "?")
    pkg = pr.get("package", "?")
    fr = pr.get("from", "?")
    to = pr.get("to", "?")
    bump = pr.get("bump", "?")
    dep_type = pr.get("dep_type", "?")
    ver = pr.get("verification_label", "?")
    cves = pr.get("cves", [])
    eco = pr.get("ecosystem", "?")
    install_ok = pr.get("install_ok", False)
    pkg_dir = pr.get("pkg_dir", "/")
    error_class = pr.get("build", {}).get("error_class", "")
    new_errors = pr.get("build", {}).get("new_errors", [])
    main_exit = pr.get("build", {}).get("main_exit", -1)
    entry = {"num": num, "pkg": pkg, "from": fr, "to": to, "bump": bump, "dep_type": dep_type, "ver": ver, "cves": cves, "eco": eco, "verdict": v, "install_ok": install_ok, "pkg_dir": pkg_dir, "error_class": error_class, "new_error_count": len(new_errors), "main_exit": main_exit}

    if v == "skipped":
        skipped.append(entry)
    elif v == "skip":
        skipped.append(entry)
    elif v == "cancelled":
        cancelled.append(entry)
    elif eco in ("actions",) or ver == "CI_ONLY":
        # V8 FIX (H3): Separate CI-only PRs — don't inflate "verified" count
        ci_only.append(entry)
    elif v in ("pass",):
        safe.append(entry)
    elif v in ("fail", "pre_existing_plus_new"):
        blocked.append(entry)
    elif v == "pre_existing":
        # pre_existing with L2+ verification = safe (same errors, no new problems)
        # pre_existing with L0/L1 and zero new errors = likely safe
        # pre_existing with L0 and new errors or no comparison = needs review
        if ver.startswith("L2") or ver.startswith("L3") or ver.startswith("L4") or ver.startswith("L5"):
            safe.append(entry)
        else:
            review.append(entry)
    elif v in ("error", "security_review"):
        review.append(entry)
    elif v == "conflict":
        blocked.append(entry)
    else:
        not_analyzed.append(entry)

# ── V9.6 FIX: Coordinated upgrade companion blocking ─────────────────────────
# If a PR is "safe" but its coordinated-upgrade companion is "blocked",
# move it from safe to a separate companion_blocked list with explanation.
# This prevents showing "#30 Safe" when "#21 Fix Required — must merge together".
blocked_nums = {e["num"] for e in blocked}
companion_blocked = []
safe_after_coord = []
for entry in safe:
    num = entry["num"]
    companion_blocked_by = []
    for group in cross:
        pr_a = str(group.get("pr_a", ""))
        pr_b = str(group.get("pr_b", ""))
        if num == pr_a and pr_b in blocked_nums:
            companion_blocked_by.append(pr_b)
        elif num == pr_b and pr_a in blocked_nums:
            companion_blocked_by.append(pr_a)
    if companion_blocked_by:
        entry = dict(entry)
        entry["companion_blocked_by"] = companion_blocked_by
        companion_blocked.append(entry)
    else:
        safe_after_coord.append(entry)
safe = safe_after_coord

# Build markdown
lines = []
lines.append("<!-- breakability-merge-plan -->")
lines.append(f"# 📋 Breakability Merge Plan")
lines.append(f"")
_gen_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
lines.append(f"**Generated:** {_gen_ts} (deterministic)")
lines.append(f"**PRs analyzed:** {len(prs)} Dependabot PRs")
if non_dependabot_count > 0:
    lines.append(f"**Not analyzed:** {non_dependabot_count} non-Dependabot PR(s) (out of scope — this tool only analyzes Dependabot dependency upgrades)")
lines.append(f"")

# V8 FIX (M3): Staleness banner — critical for developer trust (Blind Spot 4A)
lines.append(f"> ⏱️ **Snapshot** generated at `{_gen_ts}`. PR states may have changed since analysis.")
lines.append(f"> To refresh: `gh workflow run breakability-agent.yml`")

# V9.5 FIX: Warn if batch was cancelled/incomplete
if meta.get('incomplete'):
    _missing = meta.get('missing_pr_count', '?')
    _ibs = meta.get('incomplete_batches', [])
    lines.append(f"> ")
    lines.append(f"> ⚠️ **INCOMPLETE RUN:** {_missing} PRs were NOT analyzed (batch{'es' if len(_ibs) != 1 else ''} {', '.join(_ibs) if _ibs else '?'} cancelled/failed).")
    lines.append(f"> PRs missing from this plan should be re-analyzed before merging.")
lines.append("")

# Summary table
lines.append("## Summary")
lines.append("")
lines.append(f"| Category | Count |")
lines.append(f"|----------|-------|")
likely_safe_count = sum(1 for e in review if e.get("verdict") == "pre_existing" and e.get("new_error_count", 0) == 0)
unverified_count = sum(1 for e in review if e.get("verdict") == "pre_existing" and e.get("new_error_count", 0) > 0)
needs_review_count = len(review) - likely_safe_count - unverified_count
lines.append(f"| ✅ Safe to merge — tests pass (L4) | {sum(1 for e in safe if e['ver'].startswith('L4') or e['ver'].startswith('L5'))} |")
lines.append(f"| ✅ Safe to merge — build passes (L2/L3) | {sum(1 for e in safe if not (e['ver'].startswith('L4') or e['ver'].startswith('L5')))} |")
if companion_blocked:
    lines.append(f"| 🔗 Blocked (safe but companion PR needs fix) | {len(companion_blocked)} |")
if ci_only:
    lines.append(f"| 🔧 CI-only (Actions/Docker — no app impact) | {len(ci_only)} |")
if likely_safe_count > 0:
    lines.append(f"| ⚙️ Likely safe (deps resolved, no new errors) | {likely_safe_count} |")
if unverified_count > 0:
    lines.append(f"| ⚠️ Unverified (deps failed — infra issue) | {unverified_count} |")
lines.append(f"| ❌ Fix required | {len(blocked)} |")
if needs_review_count > 0:
    lines.append(f"| 🔍 Manual review | {needs_review_count} |")
if skipped:
    lines.append(f"| ⏭️ Skipped (opted out) | {len(skipped)} |")
if cancelled:
    lines.append(f"| 🚫 Cancelled / Incomplete | {len(cancelled)} |")
if not_analyzed:
    lines.append(f"| ❓ Not analyzed | {len(not_analyzed)} |")
lines.append("")

# V8 FIX (M4): Developer Action Summary — prioritized numbered steps (regression from ref plan #39)
lines.append("## Developer Action Summary")
lines.append("")
_step = 1
# Security fixes first
_sec_safe = [e for e in safe + ci_only if e.get("cves")]
_sec_blocked = [e for e in blocked if e.get("cves")]
if _sec_safe:
    _sec_nums = ", ".join(f"#{e['num']}" for e in _sec_safe)
    lines.append(f"{_step}. **MERGE NOW — security fixes:** {_sec_nums} ({len(_sec_safe)} PR(s) fix known CVEs, build verified)")
    _step += 1
if _sec_blocked:
    _sec_nums = ", ".join(f"#{e['num']}" for e in _sec_blocked)
    lines.append(f"{_step}. **FIX + MERGE — security with build issues:** {_sec_nums}")
    _step += 1
# L4 safe PRs
_l4_safe = [e for e in safe if e.get("ver", "").startswith("L4") and not e.get("cves")]
if _l4_safe:
    lines.append(f"{_step}. **Batch merge — {len(_l4_safe)} PRs with full test pass** (L4 verified, lowest risk)")
    _step += 1
# L2 safe PRs (build passes, tests fail or not run)
_l2_safe = [e for e in safe if not e.get("ver", "").startswith("L4") and not e.get("cves")]
if _l2_safe:
    lines.append(f"{_step}. **Review then merge — {len(_l2_safe)} PRs** (build + type-check pass, tests not run — check changelog for major bumps)")
    _step += 1
# Companion blocked
if companion_blocked:
    _cb_nums = ", ".join(f"#{e['num']}" for e in companion_blocked)
    lines.append(f"{_step}. **Fix companion PR first — {len(companion_blocked)} PR(s) blocked:** {_cb_nums} (build passes but must merge with companion)")
    _step += 1
# CI-only PRs
if ci_only:
    lines.append(f"{_step}. **Merge CI/Actions PRs — {len(ci_only)} PRs** (no app code impact)")
    _step += 1
# Likely safe
if likely_safe_count > 0:
    lines.append(f"{_step}. **Investigate — {likely_safe_count} 'Likely Safe' PRs** (no new errors but baseline unclear)")
    _step += 1
# Fix required
_non_sec_blocked = [e for e in blocked if not e.get("cves")]
if _non_sec_blocked:
    lines.append(f"{_step}. **Assign to team — {len(_non_sec_blocked)} PRs need code fixes** before merge")
    _step += 1
lines.append("")

# Infrastructure banner — when many PRs are in review with the same root cause
if len(review) > 0:
    infra_count = sum(1 for e in review if e.get("verdict") == "pre_existing")
    if infra_count > len(prs) * 0.5:
        # More than half of PRs are pre_existing — likely a systemic issue
        lines.append("> ⚠️ **Infrastructure Issue:** %d of %d PRs have pre-existing build failures (not caused by upgrades)." % (infra_count, len(prs)))
        lines.append("> Fix the baseline build on `main` and re-run analysis to unlock full verification for these PRs.")
        lines.append("> PRs marked \"Likely Safe\" below have no new errors — they are probably safe to merge despite incomplete verification.")
        lines.append("")

        # Show baseline error details so developers know WHAT to fix (end-user feedback 1.2)
        main_build = data.get("main_build", {})
        baseline_errors = []
        # Go baseline errors
        go_data = main_build.get("go", {})
        go_exit = go_data.get("exit", -1)
        go_output = go_data.get("output_tail", "")
        if go_exit is not None and go_exit not in (-1, 0) and go_output:
            error_lines = [l.strip() for l in go_output.split('\n')
                          if 'error' in l.lower() or 'Error' in l or 'FAIL' in l
                          or 'cannot' in l.lower() or 'undefined' in l.lower()][:10]
            if error_lines:
                baseline_errors.extend(error_lines)
        # npm baseline errors
        npm_data = main_build.get("npm", {})
        npm_exit = npm_data.get("exit", -1)
        npm_output = npm_data.get("output_tail", "")
        if npm_exit is not None and npm_exit not in (-1, 0) and npm_output:
            npm_errs = [l.strip() for l in npm_output.split('\n')
                       if 'error' in l.lower() or 'TS' in l][:5]
            if npm_errs:
                baseline_errors.extend(npm_errs)
        # Check per-module baselines from any PR's output (if available)
        error_classes = set()
        for num, pr in prs.items():
            ec = pr.get("build", {}).get("error_class", "")
            if ec and ec != "build_fail":
                error_classes.add(ec)
        if baseline_errors or error_classes:
            lines.append("### Baseline Build Errors on `main`")
            lines.append("")
            if error_classes:
                class_descriptions = {
                    "infra_error": "Infrastructure/network error (GOSUMDB, proxy, or registry issue)",
                    "private_module": "Private module access denied (GOPRIVATE not configured)",
                    "resource_exhaustion": "Out of memory / compiler killed",
                    "timeout": "Build timed out",
                    "cache_corruption": "Go build cache corruption",
                }
                for ec in sorted(error_classes):
                    desc = class_descriptions.get(ec, ec)
                    lines.append(f"- **{ec}:** {desc}")
                lines.append("")
            if baseline_errors:
                lines.append("```")
                for err in baseline_errors[:8]:
                    lines.append(err[:200])
                lines.append("```")
                lines.append("")
            lines.append("Fix these issues on `main`, then re-run: `gh workflow run breakability-agent.yml`")
            lines.append("")

# CVE highlight
all_cves = []
for cat in [safe, blocked, review, skipped]:
    for e in cat:
        if e["cves"]:
            all_cves.append(e)
if all_cves:
    lines.append("## 🔴 Security — CVEs Fixed by These Upgrades")
    lines.append("")
    lines.append("> **ACTION REQUIRED:** Merge security fix PRs as soon as possible to resolve known vulnerabilities.")
    lines.append("")
    for e in all_cves:
        cve_str = ", ".join(e["cves"])
        verdict_note = ""
        if e["verdict"] == "pass":
            verdict_note = " ✅ **SAFE — merge now**"
        elif e["verdict"] == "pre_existing":
            verdict_note = " ⚙️ **Likely safe — no new errors**"
        elif e["verdict"] == "fail":
            verdict_note = " ❌ Fix required before merge"
        lines.append(f"- **PR #{e['num']}** `{e['pkg']}` {e['from']}→{e['to']} — {cve_str}{verdict_note}")
    lines.append("")

# Safe to merge — split L4 (tests pass) vs L2/L3 (build only)
safe_l4 = [e for e in safe if e["ver"].startswith("L4") or e["ver"].startswith("L5")]
safe_l2 = [e for e in safe if not (e["ver"].startswith("L4") or e["ver"].startswith("L5"))]

if safe_l4:
    lines.append("## ✅ Safe to Merge — Tests Pass (L4 verified, lowest risk)")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Verification |")
    lines.append("|----|---------|---------|----|-------------|")
    for e in safe_l4:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e['cves'] else ""
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {e['ver']}{cve_badge} |")
    lines.append("")

if safe_l2:
    lines.append("## ✅ Safe to Merge — Build Passes, No New Errors (L2/L3 verified)")
    lines.append("")
    lines.append("> Build and type-check pass. Tests were not run or had pre-existing failures. Review changelog for major bumps.")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Verification |")
    lines.append("|----|---------|---------|----|-------------|")
    for e in safe_l2:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e['cves'] else ""
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {e['ver']}{cve_badge} |")
    lines.append("")

# Companion-blocked: safe PRs that can't be merged yet because their coordinated partner is broken
if companion_blocked:
    lines.append("## 🔗 Blocked — Safe but Companion PR Needs Fix First")
    lines.append("")
    lines.append("These PRs pass build verification but **must be merged together** with a companion PR that currently has build failures.")
    lines.append("Fix the companion PR first, then merge both together.")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Verification | Blocked By |")
    lines.append("|----|---------|---------|------|-------------|------------|")
    for e in companion_blocked:
        companions = ", ".join(f"#{n}" for n in e.get("companion_blocked_by", []))
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {e['ver']} ✅ | Fix #{companions} first |")
    lines.append("")

# Cross-PR deps
if cross:
    lines.append("## 🔗 Coordinated Upgrades (merge together)")
    lines.append("")
    for group in cross:
        reason = group.get("reason", "related")
        pr_a = group.get("pr_a", "?")
        pr_b = group.get("pr_b", "?")
        order = group.get("merge_order", "")
        order_text = f" ({order})" if order else ""
        lines.append(f"- **{reason}:** #{pr_a} + #{pr_b}{order_text}")
    lines.append("")

# Blocked
if blocked:
    lines.append("## ❌ Fix Required — Do Not Merge")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Issue |")
    lines.append("|----|---------|---------|----|-------|")
    for e in blocked:
        if e["verdict"] == "fail":
            issue = "Build fails"
        elif e["verdict"] == "conflict":
            issue = "Merge conflicts — rebase required"
        else:
            issue = "New errors on top of pre-existing"
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {issue} |")
    lines.append("")

# Review — split into "Likely Safe" and "Needs Review".
# End-user feedback: L0 pre_existing with zero new errors IS a safety signal.
# The tool compared both branches and found no new errors — that's useful info.
# Only truly "unverified" PRs (where comparison couldn't happen) go into unverified.
likely_safe = [e for e in review if e["verdict"] == "pre_existing" and e.get("new_error_count", 0) == 0]
unverified = [e for e in review if e["verdict"] == "pre_existing" and e.get("new_error_count", 0) > 0]
needs_review = [e for e in review if e["verdict"] != "pre_existing"]

if likely_safe:
    lines.append("## ⚙️ Likely Safe — No New Errors (pre-existing build failure)")
    lines.append("")
    lines.append("These PRs do **not** introduce new failures. Both `main` and the PR branch")
    lines.append("produce the same build errors. The upgrades are likely safe to merge.")
    lines.append("Fix baseline build on `main` and re-run for full L2+ verification.")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Module | Status |")
    lines.append("|----|---------|---------|----|--------|--------|")
    for e in likely_safe:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e.get('cves') else ""
        pkg_dir = e.get('pkg_dir', '/')
        mod_col = pkg_dir if pkg_dir != '/' else 'root'
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {mod_col} | {e['ver']} — no new errors{cve_badge} |")
    lines.append("")

if unverified:
    lines.append("## ⚠️ Needs Investigation (new errors detected or comparison failed)")
    lines.append("")
    lines.append("These PRs have new errors or could not be compared against the baseline.")
    lines.append("Manual review is recommended before merging.")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Module | Issue |")
    lines.append("|----|---------|---------|----|--------|-------|")
    for e in unverified:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e.get('cves') else ""
        pkg_dir = e.get('pkg_dir', '/')
        mod_col = pkg_dir if pkg_dir != '/' else 'root'
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | {mod_col} | Deps failed — infra issue{cve_badge} |")
    lines.append("")

if needs_review:
    lines.append("## ⚠️ Manual Review Needed")
    lines.append("")
    for e in needs_review:
        if e["verdict"] == "security_review":
            reason = "Build passes but npm audit found critical/high vulnerabilities"
        else:
            reason = "Build error / infrastructure issue"
        lines.append(f"- **PR #{e['num']}** `{e['pkg']}` {e['from']}→{e['to']} — {reason}")
    lines.append("")

# V8 FIX (H3/L3): CI-only PRs in their own section, not mixed with verified Go/npm PRs
if ci_only:
    lines.append("## 🔧 CI-Only (Actions / Docker — no application impact)")
    lines.append("")
    lines.append("These PRs only affect CI/CD workflows. No build verification needed — zero app code impact.")
    lines.append("")
    lines.append("| PR | Package | Version | Bump | Verification |")
    lines.append("|----|---------|---------|----|-------------|")
    for e in ci_only:
        cve_badge = f" 🔴 {','.join(e['cves'])}" if e.get('cves') else ""
        lines.append(f"| #{e['num']} | `{e['pkg']}` | {e['from']}→{e['to']} | {fmt_bump(e['bump'], e.get('from', ''))} | CI_ONLY — auto-safe{cve_badge} |")
    lines.append("")

# Skipped (breakability:skip label)
if skipped:
    lines.append("## ⏭️ Skipped (opted out)")
    lines.append("")
    for e in skipped:
        lines.append(f"- PR #{e['num']} `{e['pkg']}` — skipped ({e.get('eco', '?')})")
    lines.append("")

# Cancelled / Incomplete
if cancelled:
    lines.append("## 🚫 Cancelled / Incomplete")
    lines.append("")
    lines.append("These PRs were discovered but not analyzed (batch timeout or cancellation).")
    lines.append("")
    for e in cancelled:
        lines.append(f"- PR #{e['num']} `{e['pkg']}` — analysis incomplete")
    lines.append("")

# Security posture
if security:
    lines.append("## 🛡️ Repository Security Posture")
    lines.append("")
    open_alerts = security.get("total_open_alerts", 0)
    fixable = security.get("alerts_fixable_by_merging", 0)
    lines.append(f"- Open Dependabot alerts: **{open_alerts}**")
    if fixable:
        lines.append(f"- Alerts fixable by merging these PRs: **{fixable}**")
    by_sev = security.get("severity_counts", {})
    if by_sev:
        sev_str = ", ".join(f"{s}: {c}" for s, c in sorted(by_sev.items()))
        lines.append(f"- By severity: {sev_str}")
    lines.append("")

lines.append("---")
lines.append("> 🔬 *Deterministic merge plan — generated from build-results.json. Refer to individual PR comments for full details.*")

print("\n".join(lines))
PYEOF
)

if [[ -n "$MERGE_PLAN_BODY" && "$MERGE_PLAN_BODY" != *"Traceback"* ]]; then
  # P0 FIX (V8 review C1): Create a NEW issue for each complete run.
  # Old approach updated issue in-place but didn't update the title, so the title
  # showed stale dates (e.g., "2026-03-31" when body said "2026-04-16").
  # New approach: close all old merge plan issues, then create a fresh one.
  _MP_LABEL="breakability-merge-plan"

  # Count analyzed PRs for the title
  _MP_PR_COUNT=$(python3 -c "
import json
with open('$RESULTS_FILE') as f:
    data = json.load(f)
print(len(data.get('prs', {})))
" 2>/dev/null || echo "?")
  _MP_TITLE="📋 Breakability Merge Plan $(date -u '+%Y-%m-%d %H:%M UTC') (${_MP_PR_COUNT} PRs)"

  # Step 1: Close ALL existing merge plan issues (both labels)
  _OLD_ISSUES=$(gh issue list --label "$_MP_LABEL" --state open --json number -q '.[].number' 2>/dev/null || echo "")
  # Also check old "dependencies" label for legacy issues
  _OLD_ISSUES_LEGACY=$(gh issue list --label "dependencies" --state open --json number,title \
    -q '.[] | select(.title | test("📋.*[Mm]erge [Pp]lan")) | .number' 2>/dev/null || echo "")
  for _OLD_NUM in $_OLD_ISSUES $_OLD_ISSUES_LEGACY; do
    [[ -z "$_OLD_NUM" ]] && continue
    gh issue close "$_OLD_NUM" --comment "Superseded by new merge plan run at $(date -u '+%Y-%m-%d %H:%M UTC')." 2>/dev/null && \
      echo "  Closed old merge plan issue #$_OLD_NUM" || true
  done

  # Step 2: Create fresh merge plan issue with accurate title
  NEW_ISSUE=$(gh issue create \
    --title "$_MP_TITLE" \
    --body "$MERGE_PLAN_BODY" \
    --label "$_MP_LABEL" 2>/dev/null || echo "")
  if [[ -n "$NEW_ISSUE" ]]; then
    echo "  Created merge plan issue: $NEW_ISSUE"
  else
    echo "  ⚠️  Failed to create merge plan issue"
  fi
else
  echo "  ⚠️  Merge plan generation failed — skipping issue update"
fi
