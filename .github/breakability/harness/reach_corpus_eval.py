#!/usr/bin/env python3
"""reach_corpus_eval — reproducible accuracy validation for the scoped call-graph
reachability prover against the breakability corpus.

Across these Dependabot PRs our APPLICATION code is identical (only go.mod/go.sum
change), so "does our code reach symbol X in the dependency" is measurable on the
current checkout. This harness runs the prover for every Go PR's apidiff-flagged
symbols and asserts the soundness invariants that make the layer trustworthy:

  CONTROLS
    - POSITIVE: PR#11 NewWithInstance must be direct_in_module=True with >=1 real
      call site (proves the prover detects genuine direct use — the dangerous
      'True' direction that name-grep gets wrong on indirect dispatch).
    - NEGATIVE: PR#38 (lib/pq) must be direct=False AND transitive=False in the
      automations/tstctl module (proves 'not imported in module' is sound).
    - NOT-REACHED: PR#19 (pgx) must be direct_in_module=False for ALL flagged
      symbols (no compile break — build agrees).

  INVARIANT (no false-green): the prover must never let us auto-clear a PR by
    reporting direct=False when our code in fact compiles against a truly-changed
    symbol. PR#11 is the witness that a real direct call is caught; PR#18 is a
    build-fail (FIX regardless), so reachability is not consulted for safety there.

Exit 0 = all controls/invariants hold. Exit 1 = a control failed (regression).

Usage: reach_corpus_eval.py [--bin /tmp/reach] [--repo .]
"""
import argparse
import json
import os
import subprocess
import sys

# Per-PR inputs: (module_dir, dep_import_prefix, [flagged symbol bare names]).
# Symbols are the apidiff-flagged HARD/soft names from the corpus artifact.
CASES = {
    "11": (".", "github.com/golang-migrate/migrate/v4",
           ["NewWithDatabaseInstance", "NewWithInstance", "Buffer"]),
    "19": (".", "github.com/jackc/pgx/v5",
           ["SecretKey", "MultiResultReader", "ScanInt64", "ContextWatcher",
            "BackendKeyData", "CancelRequest", "PlanScan"]),
    "38": ("automations/tstctl", "github.com/lib/pq",
           ["Code", "ErrorClass", "ErrorCode"]),
    "10": ("cicd", "github.com/andygrunwald/go-jira",
           ["Response", "SearchV2JQL", "SearchV2JQLWithContext", "SearchOptionsV2"]),
    "32": (".", "github.com/go-openapi/strfmt",
           ["UnmarshalBSONValue", "MarshalBSONValue", "HostnamePattern",
            "MapStructureHookFunc", "ULID", "Compare"]),
    "27": (".", "go.opentelemetry.io/otel/trace",
           ["FlagsRandom", "WithInstrumentationAttributeSet"]),
}


def run_reach(binpath, repo, module, dep, symbols):
    mod_path = repo if module in (".", "") else os.path.join(repo, module)
    out = subprocess.run(
        [binpath, "-module", mod_path, "-dep", dep, "-symbols", ",".join(symbols),
         "-tests=false"],
        capture_output=True, text=True, timeout=600,
    )
    try:
        return json.loads(out.stdout)
    except Exception:
        return {"analyzed": False, "error": (out.stderr or "no JSON").strip()[:200]}


def by_symbol(res):
    return {r["symbol"]: r for r in (res.get("results") or [])}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", default="/tmp/reach")
    ap.add_argument("--repo", default=".")
    args = ap.parse_args()

    if not os.path.exists(args.bin):
        src = os.path.join(os.path.dirname(__file__), "..", "reach")
        print(f"[eval] building prover from {src} -> {args.bin}")
        subprocess.run(["go", "build", "-o", args.bin, "."], cwd=src, check=True)

    results = {}
    for pr, (module, dep, syms) in CASES.items():
        print(f"[eval] PR#{pr} {dep} [module={module}] ...", flush=True)
        results[pr] = run_reach(args.bin, args.repo, module, dep, syms)

    failures = []

    # POSITIVE control: PR#11 NewWithInstance direct=True with sites.
    r11 = by_symbol(results["11"]).get("NewWithInstance", {})
    if not (r11.get("direct_in_module") and r11.get("direct_sites")):
        failures.append(f"POSITIVE control FAILED: PR#11 NewWithInstance "
                        f"direct={r11.get('direct_in_module')} sites={r11.get('direct_sites')}")
    else:
        print(f"  ✓ POSITIVE: PR#11 NewWithInstance direct=True @ {r11['direct_sites']}")

    # NEGATIVE control: PR#38 all symbols direct=False AND transitive=False.
    bad38 = [s for s, r in by_symbol(results["38"]).items()
             if r.get("direct_in_module") or r.get("transitively_reachable")]
    if bad38:
        failures.append(f"NEGATIVE control FAILED: PR#38 unexpectedly reached {bad38}")
    elif not results["38"].get("analyzed"):
        failures.append(f"NEGATIVE control FAILED: PR#38 not analyzed: {results['38'].get('error')}")
    else:
        print("  ✓ NEGATIVE: PR#38 lib/pq not reached in automations/tstctl")

    # NOT-REACHED: PR#19 every flagged symbol direct=False.
    bad19 = [s for s, r in by_symbol(results["19"]).items() if r.get("direct_in_module")]
    if bad19:
        failures.append(f"NOT-REACHED FAILED: PR#19 unexpectedly direct {bad19}")
    elif not results["19"].get("analyzed"):
        failures.append(f"NOT-REACHED FAILED: PR#19 not analyzed: {results['19'].get('error')}")
    else:
        print("  ✓ NOT-REACHED: PR#19 no flagged pgx symbol directly called")

    print("\n=== full reachability table ===")
    for pr, res in results.items():
        if not res.get("analyzed"):
            print(f"PR#{pr}: analyzed=FALSE ({res.get('error')})")
            continue
        print(f"PR#{pr}: any_direct={res['any_direct_in_module']} "
              f"any_transitive={res['any_transitively_reachable']}")
        for r in res["results"]:
            print(f"   {r['symbol']:26} direct={str(r['direct_in_module']):5} "
                  f"transitive={str(r['transitively_reachable']):5} {r.get('direct_sites') or ''}")

    if failures:
        print("\nFAILURES:")
        for f in failures:
            print("  ✗ " + f)
        sys.exit(1)
    print("\nALL CONTROLS PASSED — call-graph reachability validated, no false-green.")


if __name__ == "__main__":
    main()
