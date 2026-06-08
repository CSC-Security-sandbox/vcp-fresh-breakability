#!/usr/bin/env python3
"""Differential probe (agent-driven behavioral verification).

For each declared-behavioral residual PR (build/tests/api-diff clean, but the
changelog declares a behavioral break that prod code imports), this driver commits
a behavioral grade in {none, low, medium, high} -- never "review it yourself".

Pipeline (closed loop), per residual PR:
  depth-1 usage scan (already done -> declared_break_reachability) supplies the call site
  -> break-class router decides if the break is observable from a minimal call-site probe
     - NOT observable (cardinality/memory/latency/concurrency/load/temporal/state) -> a probe
       would FALSE-GREEN it; commit honest Medium + targeted note. Zero AI calls.
     - AMBIGUOUS                                                                   -> Medium + note.
     - call-observable (default/return/error/format/signature)                     -> run the probe.
  -> probe: agent reads the dep source at from & to, builds+runs a self-contained probe that
     exercises ONLY the named dimension, diffs it -> emits a proof contract.
  -> DRIVER owns conservatism: floors the grade. Incomplete/timeout/can't-build/trigger-not-
     exercised -> Medium. None/Low ONLY with source-cited proof the break doesn't apply to us.

Invariants (two independent rubber-duck reviews):
- Never false-green: only call-observable breaks are probed; absence of an observed diff never
  lowers risk unless the trigger was actually exercised AND our exposure is disproven with a reason.
- Cost-bounded: not-observable/ambiguous residuals cost ZERO AI calls; probe-able ones cost one,
  capped at DP_MAX_PRS, and a per-(pkg,from,to,call-site) cache amortizes repeats.
- Sandboxed: the driver creates an ephemeral workdir, runs the agent there with cwd enforced, a
  scrubbed env (no GH/secrets, GOWORK=off, HOME=scratch), and verifies the repo stayed clean.
- Fail-open: any failure leaves a committed Medium (never a crash, never a false low/none).
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from break_class_router import classify_break, NOT_OBSERVABLE, AMBIGUOUS, CALL_OBSERVABLE

RESULTS = os.environ.get("DP_RESULTS", "/tmp/build-results.json")
PROMPT_FILE = os.environ.get("DP_PROMPT", ".github/differential-probe-prompt.md")
REASON_PROMPT_FILE = os.environ.get("DP_REASON_PROMPT", ".github/differential-reasoning-prompt.md")
REPO_ROOT = os.environ.get("DP_REPO_ROOT", ".")
AGENT_CMD = os.environ.get("DP_AGENT_CMD", "agent -p --force --model claude-4-sonnet")
MAX_PRS = int(os.environ.get("DP_MAX_PRS", "5"))
MAX_REASON = int(os.environ.get("DP_MAX_REASON", "15"))
MAX_BULLETS = int(os.environ.get("DP_MAX_BULLETS", "5"))
MAX_USAGES = int(os.environ.get("DP_MAX_USAGES", "20"))
SNIPPET_RADIUS = int(os.environ.get("DP_SNIPPET_RADIUS", "20"))
PROBE_TIMEOUT = int(os.environ.get("DP_TIMEOUT", "360"))
REASON_TIMEOUT = int(os.environ.get("DP_REASON_TIMEOUT", "180"))
CACHE_DIR = os.environ.get("DP_CACHE_DIR", "/tmp/dp-cache")
PROMPT_VERSION = "dp-v1"
REASON_PROMPT_VERSION = "dr-v1"

GRADES = ("none", "low", "medium", "high")


def log(msg):
    print(f"[differential-probe] {msg}", file=sys.stderr, flush=True)


def is_residual(pr):
    r = pr.get("declared_break_reachability") or {}
    return bool(r.get("reachability_kind") == "import" and r.get("prod_reachable"))


def safe_int(v):
    try:
        return int(str(v).strip())
    except Exception:
        return None


def read_snippet(rel_path, line):
    line = safe_int(line)
    if not rel_path or line is None:
        return ""
    path = os.path.join(REPO_ROOT, rel_path)
    try:
        with open(path, "r", errors="replace") as f:
            lines = f.readlines()
    except Exception:
        return ""
    lo = max(0, line - 1 - SNIPPET_RADIUS)
    hi = min(len(lines), line + SNIPPET_RADIUS)
    return "".join(lines[lo:hi])[:4000]


def clean_bullets(raw_bullets):
    out, seen = [], set()
    for b in raw_bullets:
        if not isinstance(b, str):
            continue
        s = re.sub(r"\s+", " ", b.replace("\r", " ").replace("\n", " ")).strip(" -*\t")
        if not s or s.startswith("#"):
            continue
        s = s[:400]
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    return out


def first_prod_site(pr):
    r = pr.get("declared_break_reachability") or {}
    # Prefer the ACTUAL usage site (surface_evidence: where our code references the
    # changed symbol, named first), not the bare import line. The probe/oracle must
    # reason about the real call, e.g. prometheus.New at metric.go:22, not the import.
    surf = [e for e in (r.get("surface_evidence") or []) if isinstance(e, dict)]
    for e in sorted(surf, key=lambda x: (not x.get("named"),)):
        if not e.get("is_test") and e.get("file"):
            return {
                "import_path": e.get("path") or e.get("import_path") or "",
                "symbol": e.get("symbol") or "",
                "file": e.get("file"),
                "line": safe_int(e.get("line")),
                "snippet": read_snippet(e.get("file"), safe_int(e.get("line"))),
            }
    for e in (r.get("evidence") or []):
        if isinstance(e, dict) and not e.get("is_test") and e.get("file"):
            return {
                "import_path": e.get("import_path") or e.get("path") or "",
                "file": e.get("file"),
                "line": safe_int(e.get("line")),
                "snippet": read_snippet(e.get("file"), e.get("line")),
            }
    return None


def affected_files(pr):
    """Production files that import the affected package(s) (from reachability)."""
    r = pr.get("declared_break_reachability") or {}
    files = []
    for e in (r.get("evidence") or []):
        if isinstance(e, dict) and not e.get("is_test") and e.get("file"):
            files.append(e["file"])
    return set(files)


def our_usages(pr):
    """How OUR code uses the affected package: production usage rows in the files
    that import it. This is the 'grep our call sites + eyeball them' a dev does."""
    det = pr.get("deterministic") or {}
    files = affected_files(pr)
    out = []
    for u in (det.get("usages") or []):
        if not isinstance(u, dict):
            continue
        if str(u.get("context", "")).lower() != "production":
            continue
        if files and u.get("file") not in files:
            continue
        out.append({
            "file": u.get("file"), "line": u.get("line"),
            "symbol": u.get("symbol"), "usageType": u.get("usageType"),
        })
        if len(out) >= MAX_USAGES:
            break
    return out


def targeted_note(pr, bullet, site, router):
    """Honest, committed Medium note for a break a probe can't safely clear."""
    loc = f"{site['file']}:{site['line']}" if site and site.get("line") else (
        site.get("file") if site else "")
    if router["class"] == NOT_OBSERVABLE:
        why = ("this break depends on runtime state/load/timing and is not reproducible from a "
               "minimal probe; assess against your usage")
    else:
        why = "the declared change could not be pinned to a reproducible call-site behavior"
    return {
        "grade": "medium",
        "source": "router_not_observable" if router["class"] == NOT_OBSERVABLE else "router_ambiguous",
        "behavior_changed": "unverified",
        "rationale": f"{bullet[:200]} — {why}.",
        "guidance": (f"Affected package used at {loc}. " if loc else "") +
                    "Check whether your usage relies on the changed behavior.",
        "call_site": loc,
        "router_markers": router.get("markers", []),
        "confidence": "low",
        "generated_at": int(time.time()),
    }


# ── proof-contract parsing + conservative grade derivation ──────────────────
def parse_contract(out_path):
    try:
        raw = open(out_path).read().strip()
    except Exception:
        return None
    if raw.startswith("```"):
        raw = raw.strip("`")
        i = raw.find("{")
        raw = raw[i:] if i >= 0 else raw
        j = raw.rfind("}")
        raw = raw[: j + 1] if j >= 0 else raw
    try:
        obj = json.loads(raw)
    except Exception as e:
        log(f"invalid contract json: {e}")
        return None
    return obj if isinstance(obj, dict) else None


def _as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "yes"):
            return True
        if s in ("false", "no"):
            return False
    return None  # unknown / "unclear"


def derive_grade(c):
    grade, reason = _derive_grade_raw(c)
    # PROOF FLOOR: the AI may only LOWER risk with cited proof. A probe-derived low/none
    # must carry either a release-note/source quote (evidence) or concrete observed
    # from->to values. Plausible prose without a citation is floored back to Medium.
    if grade in ("low", "none"):
        evidence = str(c.get("evidence", "")).strip()
        ofrom = str(c.get("observed_from", "")).strip()
        oto = str(c.get("observed_to", "")).strip()
        if not (len(evidence) >= 20 or (ofrom and oto)):
            return "medium", ("probe lowered risk but lacked cited proof (no source quote and no "
                              "observed from->to values); floored to Medium (no false-green)")
    return grade, reason


def _derive_grade_raw(c):
    """Driver-owned conservative floors. Returns (grade, reason).

    Base for any declared-behavioral residual is MEDIUM. We only move OFF Medium with
    real evidence:
      - HIGH: trigger exercised, behavior changed, and our usage is exposed.
      - LOW : either (a) trigger exercised and behavior did NOT change for the named
              dimension, or (b) our usage is provably NOT exposed (reasoned mapping).
      - NONE: reserved -- requires (b) AND an explicit not-used mapping; otherwise floored to LOW.
    Anything incomplete (no probe / trigger not exercised / unknown) stays MEDIUM.
    """
    built = _as_bool(c.get("probe_built"))
    exercised = _as_bool(c.get("trigger_condition_exercised"))
    changed = _as_bool(c.get("behavior_changed"))
    exposed = _as_bool(c.get("our_usage_exposed"))
    mapping = (c.get("our_usage_mapping") or "").strip()

    # Incomplete proof -> Medium floor.
    if not built or exercised is not True:
        return "medium", "probe did not exercise the trigger; committed at Medium (no false-green)"

    if changed is True:
        if exposed is True:
            return "high", "probe exercised the trigger; behavior changed and our usage is exposed"
        if exposed is False and len(mapping) >= 12:
            return "low", "behavior changed but our usage is provably not exposed: " + mapping[:160]
        return "medium", "behavior changed; our exposure is unclear -> Medium"

    if changed is False:
        # Trigger was actually exercised and the named dimension did not change for us.
        if exposed is False and len(mapping) >= 12:
            return "none", "trigger exercised, no change, and our usage does not rely on it: " + mapping[:140]
        return "low", "trigger exercised; the named behavior did not change in a way that affects this call"

    return "medium", "inconclusive probe result; committed at Medium"


def build_grade_from_contract(c):
    grade, reason = derive_grade(c)
    return {
        "grade": grade,
        "source": "probe",
        "rationale": reason,
        "changed_behavior": str(c.get("changed_behavior_summary", ""))[:200],
        "observed_from": str(c.get("observed_from", ""))[:200],
        "observed_to": str(c.get("observed_to", ""))[:200],
        "trigger_condition": str(c.get("trigger_condition", ""))[:200],
        "trigger_exercised": _as_bool(c.get("trigger_condition_exercised")),
        "behavior_changed": _as_bool(c.get("behavior_changed")),
        "our_usage_exposed": _as_bool(c.get("our_usage_exposed")),
        "our_usage_mapping": str(c.get("our_usage_mapping", ""))[:300],
        "evidence": str(c.get("evidence", ""))[:600],
        "limitations": str(c.get("limitations", ""))[:300],
        "confidence": str(c.get("confidence", "low")).strip().lower()
        if str(c.get("confidence", "")).strip().lower() in ("low", "medium", "high") else "low",
        "probe_commands": [str(x)[:200] for x in (c.get("probe_commands") or [])][:8],
        "model": AGENT_CMD,
        "generated_at": int(time.time()),
        "honest_cap": "reproduced the documented change under a synthetic call configuration; "
                      "not a production guarantee",
    }


# ── not-observable reasoning oracle (release-notes + usage, no execution) ────
def derive_reasoning_grade(c):
    grade, reason = _derive_reasoning_grade_raw(c)
    # PROOF FLOOR: lowering to LOW requires a cited release-note/source quote, not just
    # plausible "structurally avoids" prose. No citation -> honest Medium.
    if grade == "low":
        evidence = str(c.get("evidence", "")).strip()
        if len(evidence) < 20:
            return "medium", ("release-notes reasoning suggested low exposure but cited no source "
                              "quote; committed at Medium (only lower with cited proof)")
    return grade, reason


def _derive_reasoning_grade_raw(c):
    """Conservative floors for the release-notes reasoning oracle.

    This oracle reasons about a break a probe CANNOT reproduce (cardinality, memory,
    latency, retry, concurrency, stateful). It cannot PROVE absence of a runtime break,
    so it NEVER returns None. It mirrors how a senior dev reads the release notes and
    maps them to our call sites:
      - HIGH  : our usage plausibly HITS the trigger condition, with cited reasoning.
      - LOW   : our usage STRUCTURALLY avoids the trigger, with cited reasoning.
      - MEDIUM: uncertain / under-justified (the honest default, never a shrug).
    """
    assess = str(c.get("exposure_assessment", "")).strip().lower()
    reasoning = str(c.get("exposure_reasoning", "")).strip()
    if assess == "hits" and len(reasoning) >= 24:
        return "high", "release-notes reasoning: our usage hits the trigger condition — " + reasoning[:200]
    if assess in ("avoids", "structurally_avoids", "not_exposed") and len(reasoning) >= 24:
        return "low", "release-notes reasoning: our usage structurally avoids the trigger — " + reasoning[:200]
    return "medium", "release-notes reasoning: exposure uncertain; committed at Medium"


def build_reasoning_grade(c, site, router):
    grade, reason = derive_reasoning_grade(c)
    loc = f"{site['file']}:{site['line']}" if site and site.get("line") else (site.get("file") if site else "")
    return {
        "grade": grade,
        "source": "reasoning",
        "rationale": reason,
        "trigger_condition": str(c.get("trigger_condition", ""))[:240],
        "our_relevant_usage": str(c.get("our_relevant_usage", ""))[:300],
        "exposure_assessment": str(c.get("exposure_assessment", ""))[:40],
        "guidance": str(c.get("guidance", ""))[:300] or (f"Affected package used at {loc}." if loc else ""),
        "evidence": str(c.get("evidence", ""))[:600],
        "behavior_changed": "declared",
        "call_site": loc,
        "router_class": router["class"],
        "router_markers": router.get("markers", []),
        "confidence": str(c.get("confidence", "low")).strip().lower()
        if str(c.get("confidence", "")).strip().lower() in ("low", "medium", "high") else "low",
        "limitations": str(c.get("limitations", ""))[:300]
        or "reasoned from release notes + static usage; not a runtime guarantee",
        "model": AGENT_CMD,
        "generated_at": int(time.time()),
    }



def repo_porcelain():
    try:
        cp = subprocess.run(["git", "-C", REPO_ROOT, "status", "--porcelain"],
                            capture_output=True, text=True, timeout=30)
        return cp.stdout
    except Exception:
        return None


# ── sandbox safety helpers ──────────────────────────────────────────────────
def scrub_env_for_agent(env, keep_api_key=True, work_gocache=None):
    """Return a scrubbed environment for sandboxed agent execution.
    
    Removes all credential-like variables and dangerous paths, keeping only
    essentials (USER, TERM, etc). Intentionally aggressive to prevent credential
    leakage through obscure paths.
    """
    out = {}
    # Allowlist: minimum vars needed for a normal shell/Go execution
    safe_prefixes = ("TERM", "LANG", "LC_", "USER", "LOGNAME", "SHELL", "TMPDIR")
    safe_exact = ("PATH", "HOME", "PWD")  # These will be set/verified separately
    
    # Credential/secret patterns to remove (case-insensitive key checks)
    dangerous_patterns = (
        "TOKEN", "SECRET", "PASSWORD", "PASSWD", "KEY", "CREDENTIAL", "PRIVATE",
        "AUTH", "CERTIFICATE", "CERT", "KEYFILE", "PEM", "RSA", "SSH",
        "API_KEY", "APIKEY", "BEARER", "SESSION", "VAULT", "KUBE",
        "SLACK_", "STRIPE_", "DATABASE_", "DB_", "GOOGLE_", "AWS_",
        "AZURE_", "DOCKER_", "ENCRYPTION", "CERTIFICATE", "ENCRYPTION_",
    )
    
    for k, v in env.items():
        ku = k.upper()
        
        # Skip special handling for CURSOR_API_KEY if requested
        if ku == "CURSOR_API_KEY":
            if keep_api_key and v:
                out[k] = v
            continue
        
        # Remove any var with dangerous patterns
        if any(pat in ku for pat in dangerous_patterns):
            continue
        
        # Remove dangerous path/library vars
        if ku in ("LD_LIBRARY_PATH", "LD_PRELOAD", "PYTHONPATH", "PERL5LIB", 
                  "RUBYLIB", "CLASSPATH", "DYLD_LIBRARY_PATH"):
            continue
        
        # Remove package manager paths that might be hijacked
        if ku in ("NPM_CONFIG_PREFIX", "PIP_INDEX_URL", "GEM_HOME"):
            continue
        
        # Keep safe allowlisted vars
        if any(k.startswith(p) for p in safe_prefixes):
            out[k] = v
        elif k in safe_exact:
            out[k] = v
    
    # Force safe Go config
    if work_gocache:
        out["GOCACHE"] = work_gocache
    out["GOWORK"] = "off"
    
    return out


def create_ephemeral_home(workdir):
    """Create an ephemeral HOME directory within workdir for agent isolation.
    
    Returns the path to the ephemeral HOME. Ensures no inherited credentials
    are accessible to the agent.
    """
    home = os.path.join(workdir, ".agent-home")
    os.makedirs(home, exist_ok=True)
    
    # Create empty SSH and git config dirs to prevent agent from
    # discovering or using inherited credentials
    for subdir in (".ssh", ".gnupg"):
        d = os.path.join(home, subdir)
        os.makedirs(d, exist_ok=True)
    
    # Minimal gitconfig to prevent git credential lookups
    gitconfig = os.path.join(home, ".gitconfig")
    try:
        with open(gitconfig, "w") as f:
            f.write("[user]\n")
            f.write("    name = Differential Probe\n")
            f.write("    email = dp@local\n")
    except Exception:
        pass
    
    return home


def validate_workdir(workdir, parent_temp_dir):
    """Validate that workdir is within expected temporary boundaries.
    
    Returns True if workdir is safely contained within parent_temp_dir
    (preventing escape attempts via symlinks or path traversal).
    """
    try:
        # Resolve symlinks and relative paths
        workdir_real = os.path.realpath(workdir)
        parent_real = os.path.realpath(parent_temp_dir)
        
        # workdir must be exactly parent or a direct child
        if workdir_real == parent_real:
            return True
        if workdir_real.startswith(parent_real + os.sep):
            return True
        
        return False
    except Exception:
        return False


def run_agent(ctx, workdir, prompt_file=PROMPT_FILE, timeout=PROBE_TIMEOUT):
    try:
        prompt = open(prompt_file).read()
    except Exception as e:
        log(f"cannot read prompt: {e}")
        return None
    
    # Sandbox setup: ephemeral HOME and workdir validation
    ephemeral_home = create_ephemeral_home(workdir)
    if not validate_workdir(ephemeral_home, workdir):
        log(f"PR {ctx['pr']}: SAFETY -- ephemeral home validation failed")
        return None
    
    in_path = os.path.join(workdir, "dp-in.json")
    out_path = os.path.join(workdir, "dp-out.json")
    with open(in_path, "w") as f:
        json.dump(ctx, f)
    full = (prompt + f"\n\n---\nDP_INPUT={in_path}\nDP_OUTPUT={out_path}\nDP_WORKDIR={workdir}\n"
            + "cd into DP_WORKDIR first. Read DP_INPUT, do the analysis there only, write the "
              "proof-contract JSON to DP_OUTPUT, then stop.")
    
    # Sandbox environment: ephemeral HOME + comprehensive credential scrubbing
    # The agent runs with cwd=workdir (preventing repo writes) and HOME=ephemeral
    # (isolating any dotfiles/caches). All credential-like vars are scrubbed.
    env = scrub_env_for_agent(os.environ, keep_api_key=True,
                              work_gocache=os.path.join(workdir, "gocache"))
    env["HOME"] = ephemeral_home
    
    # Pass the secret on the command line so the real Cursor CLI authenticates from it
    # directly (not from a stored login / keychain). Skip for stub agents used in tests.
    cmd = AGENT_CMD.split()
    api_key = os.environ.get("CURSOR_API_KEY", "").strip()
    if api_key and cmd and os.path.basename(cmd[0]) in ("agent", "cursor-agent") and "--api-key" not in cmd:
        cmd = cmd + ["--api-key", api_key]
    
    before = repo_porcelain()
    try:
        cp = subprocess.run(cmd + [full], env=env, cwd=workdir,
                            timeout=timeout, capture_output=True, text=True)
    except subprocess.TimeoutExpired:
        log(f"PR {ctx['pr']}: agent timed out after {timeout}s (sandbox cleaned on exit)")
        return None
    except Exception as e:
        log(f"PR {ctx['pr']}: agent invocation failed: {e}")
        return None
    
    after = repo_porcelain()
    # Fail CLOSED: if we cannot prove the repo tree is unchanged, discard the result.
    if before is None or after is None or before != after:
        log(f"PR {ctx['pr']}: SAFETY -- repo cleanliness unverified/changed; discarding probe result")
        return None
    if cp.returncode != 0:
        log(f"PR {ctx['pr']}: agent exit {cp.returncode}: {cp.stderr[-300:]}")
        # still try to read output -- the agent may have written before a nonzero exit
    return parse_contract(out_path)




# ── cache ───────────────────────────────────────────────────────────────────
def cache_key(ctx):
    sig = "|".join([
        ctx.get("kind", "probe"),
        ctx.get("package", ""), ctx.get("from", ""), ctx.get("to", ""),
        (ctx.get("call_site") or {}).get("file", ""),
        str((ctx.get("call_site") or {}).get("line", "")),
        ctx.get("bullet", ""),
        ctx.get("prompt_version", PROMPT_VERSION),
    ])
    return hashlib.sha256(sig.encode()).hexdigest()[:24]


def cache_get(key):
    try:
        return json.load(open(os.path.join(CACHE_DIR, key + ".json")))
    except Exception:
        return None


def cache_put(key, contract):
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(os.path.join(CACHE_DIR, key + ".json"), "w") as f:
            json.dump(contract, f)
    except Exception:
        pass


# ── main ────────────────────────────────────────────────────────────────────
def grade_residual(num, pr, budgets):
    """Return a committed behavioral_grade dict for one residual PR (never None).

    budgets = {"probe": int, "reason": int} -- remaining AI-call allowances. The
    returned dict may carry a private "_ai_kind" ("probe"/"reason") + "_ai_attempted"
    so main() can decrement the right budget.
    """
    det = pr.get("deterministic") or {}
    sig = det.get("changelogSignal") or {}
    bullets = clean_bullets(sig.get("bullets") or [])[:MAX_BULLETS]
    site = first_prod_site(pr)
    router = classify_break(bullets)

    if not bullets or not site:
        # No usable scope -> honest Medium (still committed, no punt).
        return {
            "grade": "medium", "source": "insufficient_context",
            "behavior_changed": "unverified",
            "rationale": "declared behavioral break is import-reachable but lacks a precise "
                         "call site/changelog bullet to verify.",
            "confidence": "low", "generated_at": int(time.time()),
            "router_class": router["class"],
        }

    if not router["probe_recommended"]:
        # NOT_OBSERVABLE / AMBIGUOUS: a probe would be structurally blind (false-green
        # risk). Instead, reason over the release notes + our usage like a senior dev
        # would -- a graded, cited verdict, NOT a shrug-Medium. Falls back to the
        # honest targeted note if the reasoning budget is spent (no AI attempt).
        bullet = router["observable_bullet"] or bullets[0]
        if budgets.get("reason", 0) > 0:
            return run_reasoning(num, pr, site, router, bullets)
        note = targeted_note(pr, bullet, site, router)
        note["router_class"] = router["class"]
        log(f"PR {num}: router={router['class']} -> Medium note (reason budget spent; {router['reason']})")
        return note

    if budgets.get("probe", 0) <= 0:
        loc = f"{site['file']}:{site['line']}" if site.get("line") else site.get("file", "")
        return {
            "grade": "medium", "source": "budget_exhausted",
            "behavior_changed": "unverified",
            "rationale": "probe budget exhausted for this run; committed at Medium.",
            "guidance": f"Affected package used at {loc}." if loc else "",
            "call_site": loc, "confidence": "low", "generated_at": int(time.time()),
            "router_class": router["class"],
        }

    # call-observable -> probe (with cache)
    ctx = {
        "pr": str(num), "kind": "probe", "prompt_version": PROMPT_VERSION,
        "package": pr.get("package", ""), "ecosystem": pr.get("ecosystem", ""),
        "from": pr.get("from", ""), "to": pr.get("to", ""),
        "bullet": router["observable_bullet"], "dimension_hint": router.get("markers", []),
        "call_site": site,
    }
    key = cache_key(ctx)
    contract = cache_get(key)
    cached = contract is not None
    ai_attempted = False
    if not cached:
        ai_attempted = True  # a cache miss reaches run_agent -> counts against the budget
        workdir = tempfile.mkdtemp(prefix=f"dp-{num}-")
        try:
            log(f"PR {num}: router=call_observable -> probing {ctx['package']} {ctx['from']}->{ctx['to']}")
            contract = run_agent(ctx, workdir, PROMPT_FILE, PROBE_TIMEOUT)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
        if contract is not None:
            cache_put(key, contract)

    if contract is None:
        # Probe failed -> committed Medium floor (no false-green).
        loc = f"{site['file']}:{site['line']}" if site.get("line") else site.get("file", "")
        return {
            "grade": "medium", "source": "probe_failed",
            "behavior_changed": "unverified",
            "rationale": f"{ctx['bullet'][:200]} — probe could not be built/run; committed at Medium.",
            "guidance": f"Affected package used at {loc}." if loc else "",
            "call_site": loc, "confidence": "low", "generated_at": int(time.time()),
            "router_class": router["class"], "_ai_kind": "probe", "_ai_attempted": ai_attempted,
        }

    try:
        g = build_grade_from_contract(contract)
    except Exception as e:
        # Agent already ran (budget consumed); a malformed contract must NOT escape
        # un-counted or crash the loop. Commit Medium, preserve the budget flag.
        loc = f"{site['file']}:{site['line']}" if site.get("line") else site.get("file", "")
        log(f"PR {num}: contract parse failed ({e}); committing Medium")
        return {
            "grade": "medium", "source": "probe_contract_invalid",
            "behavior_changed": "unverified",
            "rationale": f"{ctx['bullet'][:200]} — probe ran but returned an unusable proof contract; committed at Medium.",
            "guidance": f"Affected package used at {loc}." if loc else "",
            "call_site": loc, "confidence": "low", "generated_at": int(time.time()),
            "router_class": router["class"], "_ai_kind": "probe", "_ai_attempted": ai_attempted,
        }
    g["router_class"] = router["class"]
    g["cached"] = cached
    g["_ai_kind"] = "probe"
    g["_ai_attempted"] = ai_attempted
    g["call_site"] = f"{site['file']}:{site['line']}" if site.get("line") else site.get("file", "")
    log(f"PR {num}: probe grade={g['grade']} ({g['rationale'][:80]})")
    return g


def run_reasoning(num, pr, site, router, bullets):
    """Release-notes + usage reasoning oracle for not-observable breaks. ALWAYS returns
    a committed grade dict (never None). On a cache MISS it consumes one reason-budget
    AI call (marked via _ai_kind/_ai_attempted); on failure it falls back to the honest
    Medium targeted note BUT keeps the budget marker so a flapping oracle can't burn
    unbounded calls."""
    det = pr.get("deterministic") or {}
    bullet = router["observable_bullet"] or bullets[0]
    ctx = {
        "pr": str(num), "kind": "reason", "prompt_version": REASON_PROMPT_VERSION,
        "package": pr.get("package", ""), "ecosystem": pr.get("ecosystem", ""),
        "from": pr.get("from", ""), "to": pr.get("to", ""),
        "bullet": bullet, "all_bullets": bullets,
        "changelog_text": str(det.get("changelogText", ""))[:4000],
        "dimension_hint": router.get("markers", []),
        "call_site": site, "our_usages": our_usages(pr),
    }
    key = cache_key(ctx)
    contract = cache_get(key)
    cached = contract is not None
    ai_attempted = not cached
    if not cached:
        workdir = tempfile.mkdtemp(prefix=f"dr-{num}-")
        try:
            log(f"PR {num}: router={router['class']} -> reasoning over release notes for "
                f"{ctx['package']} {ctx['from']}->{ctx['to']}")
            contract = run_agent(ctx, workdir, REASON_PROMPT_FILE, REASON_TIMEOUT)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
        if contract is not None:
            cache_put(key, contract)

    if contract is None:
        # Oracle failed -> honest Medium note, but the AI attempt still counts.
        note = targeted_note(pr, bullet, site, router)
        note["router_class"] = router["class"]
        note["source"] = "reasoning_failed"
        note["_ai_kind"] = "reason"
        note["_ai_attempted"] = ai_attempted
        log(f"PR {num}: reasoning oracle produced no contract; committed Medium note")
        return note
    try:
        g = build_reasoning_grade(contract, site, router)
    except Exception as e:
        log(f"PR {num}: reasoning contract invalid ({e}); falling back to Medium note")
        g = targeted_note(pr, bullet, site, router)
        g["router_class"] = router["class"]
        g["source"] = "reasoning_invalid"
    g["cached"] = cached
    g["_ai_kind"] = "reason"
    g["_ai_attempted"] = ai_attempted
    log(f"PR {num}: reasoning grade={g['grade']} ({g.get('rationale','')[:80]})")
    return g


def main():
    if not os.path.isfile(RESULTS):
        log(f"no results file at {RESULTS}; nothing to do")
        return 0
    try:
        data = json.load(open(RESULTS))
    except Exception as e:
        log(f"cannot parse {RESULTS}: {e}")
        return 0
    prs = data.get("prs") or {}
    residual = [(n, pr) for n, pr in prs.items() if isinstance(pr, dict) and is_residual(pr)]
    if not residual:
        log("no declared-behavioral residual PRs; nothing to grade")
        return 0
    probe_budget = MAX_PRS
    reason_budget = MAX_REASON
    annotated = 0

    def _persist():
        # Atomic: write to a temp file in the same dir, then os.replace() over RESULTS so a
        # mid-loop kill can never leave a truncated/corrupt artifact for downstream steps.
        try:
            d = os.path.dirname(os.path.abspath(RESULTS)) or "."
            fd, tmp = tempfile.mkstemp(prefix=".dp-results-", dir=d)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(data, f)
                os.replace(tmp, RESULTS)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
            return True
        except Exception as e:
            log(f"failed to write {RESULTS}: {e}")
            return False

    for num, pr in residual:
        try:
            g = grade_residual(num, pr, {"probe": probe_budget, "reason": reason_budget})
            kind = g.pop("_ai_kind", None)
            if g.pop("_ai_attempted", False):
                if kind == "probe":
                    probe_budget -= 1
                elif kind == "reason":
                    reason_budget -= 1
            pr["behavioral_grade"] = g
            annotated += 1
        except Exception as e:
            log(f"PR {num}: grading failed ({e}); committing Medium")
            pr["behavioral_grade"] = {
                "grade": "medium", "source": "error", "behavior_changed": "unverified",
                "rationale": "behavioral grading failed; committed at Medium.",
                "confidence": "low", "generated_at": int(time.time()),
            }
            annotated += 1
        # Persist after EACH PR so a mid-loop timeout/kill (budgets can exceed the step
        # timeout) does not discard grades already committed.
        _persist()

    if annotated:
        if _persist():
            log(f"committed behavioral grades for {annotated} residual PR(s); wrote {RESULTS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
